package com.ahmetkeles.orderservice.outbox.event;

import com.ahmetkeles.orderservice.domain.OrderStatus;

import java.util.UUID;

public record OrderCreatedEvent(
        UUID orderId,
        UUID customerId,
        String currency,
        OrderStatus status
) {
}
