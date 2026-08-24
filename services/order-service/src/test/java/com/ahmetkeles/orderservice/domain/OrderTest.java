package com.ahmetkeles.orderservice.domain;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class OrderTest {

    @Test
    void addItemAddsItemAndUpdatesTotal() {
        Order order = new Order(UUID.randomUUID(), "USD");

        order.addItem(
                UUID.randomUUID(),
                2,
                new BigDecimal("15.00")
        );

        assertEquals(1, order.getItems().size());
        assertEquals(new BigDecimal("30.00"), order.getTotalAmount());
    }
}
