package com.ahmetkeles.orderservice.service;

import com.ahmetkeles.orderservice.domain.Order;
import com.ahmetkeles.orderservice.outbox.OutboxEventRepository;
import com.ahmetkeles.orderservice.repository.OrderRepository;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
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
    void confirmsPendingOrder() {
        UUID orderId = UUID.randomUUID();
        Order order = new Order(UUID.randomUUID(), "USD");

        when(orderRepository.findWithItemsById(orderId))
                .thenReturn(Optional.of(order));

        orderService.confirmOrder(orderId);

        assertEquals(com.ahmetkeles.orderservice.domain.OrderStatus.CONFIRMED,
                order.getStatus());
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
