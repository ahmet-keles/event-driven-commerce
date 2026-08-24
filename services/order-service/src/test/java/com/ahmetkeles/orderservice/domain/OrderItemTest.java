package com.ahmetkeles.orderservice.domain;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OrderItemTest {

    @Test
    void orderItemRequiresProductId() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new OrderItem(null, 1, BigDecimal.ONE, newOrder())
        );
    }

    @Test
    void orderItemRequiresPositiveQuantity() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new OrderItem(UUID.randomUUID(), 0, BigDecimal.ONE, newOrder())
        );
    }

    @Test
    void orderItemRequiresUnitPrice() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new OrderItem(UUID.randomUUID(), 1, null, newOrder())
        );
    }

    @Test
    void orderItemDoesNotAllowNegativeUnitPrice() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new OrderItem(
                        UUID.randomUUID(), 1, new BigDecimal("-0.01"), newOrder())
        );
    }

    @Test
    void orderItemRequiresOrder() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new OrderItem(UUID.randomUUID(), 1, BigDecimal.ONE, null)
        );
    }

    @Test
    void subtotalIsQuantityMultipliedByUnitPrice() {
        OrderItem item = new OrderItem(
                UUID.randomUUID(), 3, new BigDecimal("4.50"), newOrder());

        assertEquals(new BigDecimal("13.50"), item.subtotal());
    }

    private Order newOrder() {
        return new Order(UUID.randomUUID(), "USD");
    }
}
