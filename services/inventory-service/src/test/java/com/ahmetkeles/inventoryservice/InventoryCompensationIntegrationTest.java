package com.ahmetkeles.inventoryservice;

import com.ahmetkeles.inventoryservice.inventory.InventoryItem;
import com.ahmetkeles.inventoryservice.inventory.InventoryItemRepository;
import com.ahmetkeles.inventoryservice.inventory.InventoryReservation;
import com.ahmetkeles.inventoryservice.inventory.InventoryReservationRepository;
import com.ahmetkeles.inventoryservice.inventory.InventoryReservationService;
import com.ahmetkeles.inventoryservice.inventory.OrderInventoryState;
import com.ahmetkeles.inventoryservice.inventory.OrderInventoryStateRepository;
import com.ahmetkeles.inventoryservice.inventory.OrderInventoryStatus;
import com.ahmetkeles.inventoryservice.inventory.ProcessedEventRepository;
import com.ahmetkeles.inventoryservice.inventory.ReservationStatus;
import com.ahmetkeles.inventoryservice.outbox.OutboxEvent;
import com.ahmetkeles.inventoryservice.outbox.OutboxEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Compensation flow against real PostgreSQL: the reservation ledger written on
 * reserve, the release performed on ORDER_CANCELLED, and the idempotency and
 * ordering guarantees that keep stock consistent under duplicates and late
 * events.
 */
class InventoryCompensationIntegrationTest extends PostgreSQLIntegrationTest {

    private static final String ORDER_ITEM_ADDED = "ORDER_ITEM_ADDED";
    private static final String ORDER_CANCELLED = "ORDER_CANCELLED";

    @Autowired
    private InventoryReservationService reservationService;

    @Autowired
    private InventoryItemRepository inventoryItemRepository;

    @Autowired
    private InventoryReservationRepository reservationRepository;

    @Autowired
    private OrderInventoryStateRepository orderInventoryStateRepository;

    @Autowired
    private ProcessedEventRepository processedEventRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @BeforeEach
    void cleanDatabase() {
        outboxEventRepository.deleteAll();
        processedEventRepository.deleteAll();
        reservationRepository.deleteAll();
        orderInventoryStateRepository.deleteAll();
        inventoryItemRepository.deleteAll();
    }

    @Test
    void successfulReservationWritesDurableLedgerRow() {
        UUID orderId = UUID.randomUUID();
        UUID orderItemId = UUID.randomUUID();
        UUID productId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        seed(productId, 10);

        reservationService.reserve(
                eventId, ORDER_ITEM_ADDED, orderId, orderItemId, productId, 3);

        InventoryReservation reservation =
                reservationRepository.findById(orderItemId).orElseThrow();
        assertEquals(orderId, reservation.getOrderId());
        assertEquals(productId, reservation.getProductId());
        assertEquals(3, reservation.getQuantity());
        assertEquals(ReservationStatus.RESERVED, reservation.getStatus());
        assertEquals(eventId, reservation.getSourceEventId());

        OrderInventoryState state =
                orderInventoryStateRepository.findById(orderId).orElseThrow();
        assertEquals(OrderInventoryStatus.ACTIVE, state.getState());
    }

    @Test
    void cancellationReleasesOnlyWhatWasActuallyReserved() {
        // The primary business scenario: two items, the first reserves, the
        // second fails on insufficient stock, the order cancels.
        UUID orderId = UUID.randomUUID();
        UUID reservedItemId = UUID.randomUUID();
        UUID failedItemId = UUID.randomUUID();
        UUID productA = UUID.randomUUID();
        UUID productB = UUID.randomUUID();
        seed(productA, 10);
        seed(productB, 1);

        reservationService.reserve(
                UUID.randomUUID(), ORDER_ITEM_ADDED,
                orderId, reservedItemId, productA, 3);
        reservationService.reserve(
                UUID.randomUUID(), ORDER_ITEM_ADDED,
                orderId, failedItemId, productB, 5);

        InventoryItem itemA = inventoryItemRepository
                .findById(productA).orElseThrow();
        assertEquals(7, itemA.getAvailableQuantity());
        assertEquals(3, itemA.getReservedQuantity());

        reservationService.releaseForCancelledOrder(
                UUID.randomUUID(), ORDER_CANCELLED, orderId);

        itemA = inventoryItemRepository.findById(productA).orElseThrow();
        assertEquals(10, itemA.getAvailableQuantity());
        assertEquals(0, itemA.getReservedQuantity());

        InventoryItem itemB = inventoryItemRepository
                .findById(productB).orElseThrow();
        assertEquals(1, itemB.getAvailableQuantity());
        assertEquals(0, itemB.getReservedQuantity());

        assertEquals(ReservationStatus.RELEASED,
                reservationRepository.findById(reservedItemId)
                        .orElseThrow().getStatus());
        assertTrue(reservationRepository.findById(failedItemId).isEmpty(),
                "a failed item must never gain a ledger row");
        assertEquals(OrderInventoryStatus.CANCELLED,
                orderInventoryStateRepository.findById(orderId)
                        .orElseThrow().getState());
    }

    @Test
    void duplicateCancellationMovesNoStockAndCausesNoChurn() {
        UUID orderId = UUID.randomUUID();
        UUID orderItemId = UUID.randomUUID();
        UUID productId = UUID.randomUUID();
        seed(productId, 10);

        reservationService.reserve(
                UUID.randomUUID(), ORDER_ITEM_ADDED,
                orderId, orderItemId, productId, 4);

        UUID firstCancelEventId = UUID.randomUUID();
        reservationService.releaseForCancelledOrder(
                firstCancelEventId, ORDER_CANCELLED, orderId);

        Instant stateUpdatedAt = orderInventoryStateRepository
                .findById(orderId).orElseThrow().getUpdatedAt();
        Instant reservationUpdatedAt = reservationRepository
                .findById(orderItemId).orElseThrow().getUpdatedAt();

        // Same eventId replayed (redelivery after restart) and a distinct
        // cancellation eventId (a second ORDER_CANCELLED would be a producer
        // anomaly, but release must still be exactly-once on stock).
        reservationService.releaseForCancelledOrder(
                firstCancelEventId, ORDER_CANCELLED, orderId);
        reservationService.releaseForCancelledOrder(
                UUID.randomUUID(), ORDER_CANCELLED, orderId);

        InventoryItem item = inventoryItemRepository
                .findById(productId).orElseThrow();
        assertEquals(10, item.getAvailableQuantity());
        assertEquals(0, item.getReservedQuantity());
        assertEquals(stateUpdatedAt,
                orderInventoryStateRepository.findById(orderId)
                        .orElseThrow().getUpdatedAt(),
                "duplicate cancellation must not touch the state row");
        assertEquals(reservationUpdatedAt,
                reservationRepository.findById(orderItemId)
                        .orElseThrow().getUpdatedAt(),
                "an already released row must not be rewritten");
    }

    @Test
    void lateReservationAfterCancellationReservesNothing() {
        UUID orderId = UUID.randomUUID();
        UUID orderItemId = UUID.randomUUID();
        UUID productId = UUID.randomUUID();
        UUID lateEventId = UUID.randomUUID();
        seed(productId, 10);

        reservationService.releaseForCancelledOrder(
                UUID.randomUUID(), ORDER_CANCELLED, orderId);

        reservationService.reserve(
                lateEventId, ORDER_ITEM_ADDED,
                orderId, orderItemId, productId, 3);

        InventoryItem item = inventoryItemRepository
                .findById(productId).orElseThrow();
        assertEquals(10, item.getAvailableQuantity());
        assertEquals(0, item.getReservedQuantity());

        assertTrue(reservationRepository.findById(orderItemId).isEmpty(),
                "a late event must not create a reservation for a cancelled order");
        assertEquals(OrderInventoryStatus.CANCELLED,
                orderInventoryStateRepository.findById(orderId)
                        .orElseThrow().getState(),
                "a late event must not resurrect the order");
        assertTrue(processedEventRepository.existsById(lateEventId),
                "the skipped event must still be recorded as processed");

        List<OutboxEvent> outbox = outboxEventRepository.findAll();
        assertTrue(outbox.stream().noneMatch(
                        event -> "INVENTORY_RESERVED".equals(
                                event.getEventType())),
                "no reservation outcome may be announced for a cancelled order");
    }

    @Test
    void distinctEventIdForSameOrderItemDoesNotReserveTwice() {
        UUID orderId = UUID.randomUUID();
        UUID orderItemId = UUID.randomUUID();
        UUID productId = UUID.randomUUID();
        UUID secondEventId = UUID.randomUUID();
        seed(productId, 10);

        reservationService.reserve(
                UUID.randomUUID(), ORDER_ITEM_ADDED,
                orderId, orderItemId, productId, 3);
        reservationService.reserve(
                secondEventId, ORDER_ITEM_ADDED,
                orderId, orderItemId, productId, 3);

        InventoryItem item = inventoryItemRepository
                .findById(productId).orElseThrow();
        assertEquals(7, item.getAvailableQuantity());
        assertEquals(3, item.getReservedQuantity());
        assertEquals(1, reservationRepository.count());
        assertTrue(processedEventRepository.existsById(secondEventId),
                "the duplicate must be recorded so it is not retried forever");
    }

    private void seed(UUID productId, int available) {
        inventoryItemRepository.saveAndFlush(
                new InventoryItem(productId, available));
    }
}
