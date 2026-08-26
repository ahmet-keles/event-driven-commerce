package com.ahmetkeles.orderservice.domain;

import com.ahmetkeles.orderservice.PostgreSQLIntegrationTest;
import com.ahmetkeles.orderservice.outbox.OutboxEventRepository;
import com.ahmetkeles.orderservice.repository.OrderRepository;
import com.ahmetkeles.orderservice.service.OrderService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that per-item reservation state survives a round trip through
 * PostgreSQL, so an order's confirmation is driven by persisted item state
 * rather than by in-memory bookkeeping.
 */
class MultiItemReservationIntegrationTest extends PostgreSQLIntegrationTest {

    @Autowired
    private OrderService orderService;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @BeforeEach
    void clearDatabase() {
        outboxEventRepository.deleteAll();
        orderRepository.deleteAll();
    }

    @Test
    void singleItemOrderIsConfirmedOnceItsOnlyItemIsReserved() {
        UUID orderId = createOrderWithItems(1);
        List<UUID> itemIds = itemIds(orderId);

        orderService.markItemReserved(orderId, itemIds.getFirst());

        assertEquals(OrderStatus.CONFIRMED, statusOf(orderId));
    }

    @Test
    void twoItemOrderStaysPendingUntilEveryItemIsReserved() {
        UUID orderId = createOrderWithItems(2);
        List<UUID> itemIds = itemIds(orderId);

        orderService.markItemReserved(orderId, itemIds.get(0));

        assertEquals(OrderStatus.PENDING, statusOf(orderId));
        assertTrue(reservedFlags(orderId).contains(true));
        assertTrue(reservedFlags(orderId).contains(false));

        orderService.markItemReserved(orderId, itemIds.get(1));

        assertEquals(OrderStatus.CONFIRMED, statusOf(orderId));
        assertFalse(reservedFlags(orderId).contains(false));
    }

    @Test
    void duplicateReservationEventDoesNotConfirmPartiallyReservedOrder() {
        UUID orderId = createOrderWithItems(2);
        List<UUID> itemIds = itemIds(orderId);

        orderService.markItemReserved(orderId, itemIds.get(0));
        orderService.markItemReserved(orderId, itemIds.get(0));
        orderService.markItemReserved(orderId, itemIds.get(0));

        assertEquals(OrderStatus.PENDING, statusOf(orderId));
    }

    @Test
    void reservationFailureCancelsPartiallyReservedOrder() {
        UUID orderId = createOrderWithItems(2);
        List<UUID> itemIds = itemIds(orderId);

        orderService.markItemReserved(orderId, itemIds.get(0));
        orderService.cancelOrder(orderId);

        assertEquals(OrderStatus.CANCELLED, statusOf(orderId));
    }

    @Test
    void lateReservationDoesNotRevertCancelledOrder() {
        UUID orderId = createOrderWithItems(2);
        List<UUID> itemIds = itemIds(orderId);

        orderService.cancelOrder(orderId);
        orderService.markItemReserved(orderId, itemIds.get(0));
        orderService.markItemReserved(orderId, itemIds.get(1));

        assertEquals(OrderStatus.CANCELLED, statusOf(orderId));
    }

    @Test
    void lateFailureDoesNotRevertConfirmedOrder() {
        UUID orderId = createOrderWithItems(2);
        List<UUID> itemIds = itemIds(orderId);

        orderService.markItemReserved(orderId, itemIds.get(0));
        orderService.markItemReserved(orderId, itemIds.get(1));
        orderService.cancelOrder(orderId);

        assertEquals(OrderStatus.CONFIRMED, statusOf(orderId));
    }

    @Test
    void itemsSharingAProductAreReservedIndependently() {
        UUID sharedProductId = UUID.randomUUID();
        UUID orderId = orderService
                .createOrder(UUID.randomUUID(), "USD")
                .getId();

        orderService.addItem(
                orderId, sharedProductId, 1, new BigDecimal("10.00"));
        orderService.addItem(
                orderId, sharedProductId, 1, new BigDecimal("10.00"));

        List<UUID> itemIds = itemIds(orderId);

        assertEquals(2, itemIds.size());

        orderService.markItemReserved(orderId, itemIds.get(0));

        assertEquals(OrderStatus.PENDING, statusOf(orderId));

        orderService.markItemReserved(orderId, itemIds.get(1));

        assertEquals(OrderStatus.CONFIRMED, statusOf(orderId));
    }

    private UUID createOrderWithItems(int itemCount) {
        UUID orderId = orderService
                .createOrder(UUID.randomUUID(), "USD")
                .getId();

        for (int i = 0; i < itemCount; i++) {
            orderService.addItem(
                    orderId,
                    UUID.randomUUID(),
                    1,
                    new BigDecimal("10.00")
            );
        }

        return orderId;
    }

    private List<UUID> itemIds(UUID orderId) {
        return orderRepository.findWithItemsById(orderId)
                .orElseThrow()
                .getItems()
                .stream()
                .map(OrderItem::getId)
                .toList();
    }

    private List<Boolean> reservedFlags(UUID orderId) {
        return orderRepository.findWithItemsById(orderId)
                .orElseThrow()
                .getItems()
                .stream()
                .map(OrderItem::isReserved)
                .toList();
    }

    private OrderStatus statusOf(UUID orderId) {
        return orderRepository.findWithItemsById(orderId)
                .orElseThrow()
                .getStatus();
    }
}
