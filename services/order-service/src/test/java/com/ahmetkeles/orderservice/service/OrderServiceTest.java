package com.ahmetkeles.orderservice.service;

import com.ahmetkeles.orderservice.domain.Order;
import com.ahmetkeles.orderservice.domain.OrderItem;
import com.ahmetkeles.orderservice.outbox.OutboxEventRepository;
import com.ahmetkeles.orderservice.repository.OrderRepository;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;

import com.ahmetkeles.orderservice.outbox.OutboxEvent;
import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OrderServiceTest {

    private final OrderRepository orderRepository = mock(OrderRepository.class);
    private final OutboxEventRepository outboxEventRepository =
            mock(OutboxEventRepository.class);
    private final OrderService orderService =
            new OrderService(
                    orderRepository,
                    outboxEventRepository,
                    new ObjectMapper()
            );

    @Test
    void confirmsOrderOnceEveryItemIsReserved() {
        UUID orderId = UUID.randomUUID();
        Order order = new Order(UUID.randomUUID(), "USD");
        OrderItem first = order.addItem(
                UUID.randomUUID(), 1, new BigDecimal("10.00"));
        OrderItem second = order.addItem(
                UUID.randomUUID(), 1, new BigDecimal("20.00"));

        when(orderRepository.findWithItemsById(orderId))
                .thenReturn(Optional.of(order));

        orderService.markItemReserved(orderId, first.getId());

        assertEquals(com.ahmetkeles.orderservice.domain.OrderStatus.PENDING,
                order.getStatus());

        orderService.markItemReserved(orderId, second.getId());

        assertEquals(com.ahmetkeles.orderservice.domain.OrderStatus.CONFIRMED,
                order.getStatus());
    }

    @Test
    void markingSameItemReservedTwiceDoesNotConfirmOrder() {
        UUID orderId = UUID.randomUUID();
        Order order = new Order(UUID.randomUUID(), "USD");
        OrderItem first = order.addItem(
                UUID.randomUUID(), 1, new BigDecimal("10.00"));
        order.addItem(UUID.randomUUID(), 1, new BigDecimal("20.00"));

        when(orderRepository.findWithItemsById(orderId))
                .thenReturn(Optional.of(order));

        orderService.markItemReserved(orderId, first.getId());
        orderService.markItemReserved(orderId, first.getId());

        assertEquals(com.ahmetkeles.orderservice.domain.OrderStatus.PENDING,
                order.getStatus());
    }

    @Test
    void markingItemReservedOnUnknownOrderThrows() {
        UUID orderId = UUID.randomUUID();

        when(orderRepository.findWithItemsById(orderId))
                .thenReturn(Optional.empty());

        assertThrows(
                OrderNotFoundException.class,
                () -> orderService.markItemReserved(
                        orderId, UUID.randomUUID())
        );
    }

    @Test
    void cancelsPendingOrder() {
        UUID orderId = UUID.randomUUID();
        Order order = new Order(UUID.randomUUID(), "USD");

        when(orderRepository.findWithItemsById(orderId))
                .thenReturn(Optional.of(order));

        orderService.cancelOrder(orderId);

        assertEquals(com.ahmetkeles.orderservice.domain.OrderStatus.CANCELLED,
                order.getStatus());

        ArgumentCaptor<OutboxEvent> captor =
                ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(captor.capture());
        assertEquals("ORDER_CANCELLED", captor.getValue().getEventType());
        assertEquals(order.getId(), captor.getValue().getAggregateId());
        assertTrue(captor.getValue().getPayload()
                .contains("\"orderId\":\"" + order.getId() + "\""));
    }

    @Test
    void cancellingOrderIsIdempotent() {
        UUID orderId = UUID.randomUUID();
        Order order = new Order(UUID.randomUUID(), "USD");

        when(orderRepository.findWithItemsById(orderId))
                .thenReturn(Optional.of(order));

        orderService.cancelOrder(orderId);
        orderService.cancelOrder(orderId);

        assertEquals(com.ahmetkeles.orderservice.domain.OrderStatus.CANCELLED,
                order.getStatus());
        verify(outboxEventRepository, times(1)).save(any(OutboxEvent.class));
    }

    @Test
    void cancellingConfirmedOrderEmitsNoOutboxEvent() {
        UUID orderId = UUID.randomUUID();
        Order order = new Order(UUID.randomUUID(), "USD");
        OrderItem item = order.addItem(
                UUID.randomUUID(), 1, new BigDecimal("10.00"));
        order.markItemReserved(item.getId());

        when(orderRepository.findWithItemsById(orderId))
                .thenReturn(Optional.of(order));

        orderService.cancelOrder(orderId);

        assertEquals(com.ahmetkeles.orderservice.domain.OrderStatus.CONFIRMED,
                order.getStatus());
        verify(outboxEventRepository, never()).save(any(OutboxEvent.class));
    }

    @Test
    void addingItemToCancelledOrderThrowsAndWritesNoOutboxEvent() {
        UUID orderId = UUID.randomUUID();
        Order order = new Order(UUID.randomUUID(), "USD");
        order.cancel();

        when(orderRepository.findWithItemsById(orderId))
                .thenReturn(Optional.of(order));

        assertThrows(
                com.ahmetkeles.orderservice.domain.OrderNotModifiableException.class,
                () -> orderService.addItem(
                        orderId,
                        UUID.randomUUID(),
                        1,
                        new BigDecimal("10.00")
                )
        );

        assertTrue(order.getItems().isEmpty());
        verify(outboxEventRepository, never()).save(any(OutboxEvent.class));
    }

    @Test
    void addingItemToConfirmedOrderThrowsAndWritesNoOutboxEvent() {
        UUID orderId = UUID.randomUUID();
        Order order = new Order(UUID.randomUUID(), "USD");
        OrderItem item = order.addItem(
                UUID.randomUUID(), 1, new BigDecimal("10.00"));
        order.markItemReserved(item.getId());

        when(orderRepository.findWithItemsById(orderId))
                .thenReturn(Optional.of(order));

        assertThrows(
                com.ahmetkeles.orderservice.domain.OrderNotModifiableException.class,
                () -> orderService.addItem(
                        orderId,
                        UUID.randomUUID(),
                        1,
                        new BigDecimal("10.00")
                )
        );

        assertEquals(1, order.getItems().size());
        verify(outboxEventRepository, never()).save(any(OutboxEvent.class));
    }

    @Test
    void cancellingUnknownOrderThrows() {
        UUID orderId = UUID.randomUUID();

        when(orderRepository.findWithItemsById(orderId))
                .thenReturn(Optional.empty());

        assertThrows(
                OrderNotFoundException.class,
                () -> orderService.cancelOrder(orderId)
        );
    }
}
