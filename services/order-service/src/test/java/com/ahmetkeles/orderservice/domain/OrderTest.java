package com.ahmetkeles.orderservice.domain;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OrderTest {

    @Test
    void orderRequiresCustomerId() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new Order(null, "USD")
        );
    }

    @Test
    void orderRequiresCurrency() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new Order(UUID.randomUUID(), null)
        );
    }

    @Test
    void orderRequiresNonBlankCurrency() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new Order(UUID.randomUUID(), "  ")
        );
    }

    @Test
    void newOrderStartsWithZeroTotal() {
        Order order = new Order(UUID.randomUUID(), "USD");

        assertEquals(BigDecimal.ZERO, order.getTotalAmount());
    }

    @Test
    void addItemAddsItem() {
        Order order = new Order(UUID.randomUUID(), "USD");

        order.addItem(UUID.randomUUID(), 2, new BigDecimal("15.00"));

        assertEquals(1, order.getItems().size());
    }

    @Test
    void addItemUpdatesTotal() {
        Order order = new Order(UUID.randomUUID(), "USD");

        order.addItem(UUID.randomUUID(), 2, new BigDecimal("15.00"));

        assertEquals(new BigDecimal("30.00"), order.getTotalAmount());
    }

    @Test
    void multipleItemsAccumulateTotal() {
        Order order = new Order(UUID.randomUUID(), "USD");

        order.addItem(UUID.randomUUID(), 2, new BigDecimal("15.00"));
        order.addItem(UUID.randomUUID(), 3, new BigDecimal("4.50"));

        assertEquals(new BigDecimal("43.50"), order.getTotalAmount());
    }

    @Test
    void pendingOrderCanBeConfirmed() {
        Order order = new Order(UUID.randomUUID(), "USD");

        order.confirm();

        assertEquals(OrderStatus.CONFIRMED, order.getStatus());
    }

    @Test
    void confirmingAlreadyConfirmedOrderIsIdempotent() {
        Order order = new Order(UUID.randomUUID(), "USD");

        order.confirm();
        order.confirm();

        assertEquals(OrderStatus.CONFIRMED, order.getStatus());
    }

    @Test
    void pendingOrderCanBeCancelled() {
        Order order = new Order(UUID.randomUUID(), "USD");

        order.cancel();

        assertEquals(OrderStatus.CANCELLED, order.getStatus());
    }

    @Test
    void cancellingAlreadyCancelledOrderIsIdempotent() {
        Order order = new Order(UUID.randomUUID(), "USD");

        order.cancel();
        order.cancel();

        assertEquals(OrderStatus.CANCELLED, order.getStatus());
    }

    @Test
    void confirmedOrderIsNotCancelled() {
        Order order = new Order(UUID.randomUUID(), "USD");

        order.confirm();
        order.cancel();

        assertEquals(OrderStatus.CONFIRMED, order.getStatus());
    }

    @Test
    void cancelledOrderIsNotConfirmed() {
        Order order = new Order(UUID.randomUUID(), "USD");

        order.cancel();
        order.confirm();

        assertEquals(OrderStatus.CANCELLED, order.getStatus());
    }

    @Test
    void itemsCannotBeModifiedByCallers() {
        Order order = new Order(UUID.randomUUID(), "USD");

        assertThrows(
                UnsupportedOperationException.class,
                () -> order.getItems().add(new OrderItem(
                        UUID.randomUUID(), 1, BigDecimal.ONE, order))
        );
    }
}
