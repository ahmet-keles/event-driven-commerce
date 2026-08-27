package com.ahmetkeles.orderservice.domain;

import java.util.UUID;

public class OrderNotModifiableException extends RuntimeException {

    public OrderNotModifiableException(UUID orderId, OrderStatus status) {
        super("Order " + orderId + " cannot be modified in status " + status);
    }

    public OrderNotModifiableException(UUID orderId) {
        super("Order " + orderId + " cannot be modified after submission");
    }
}
