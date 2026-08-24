package com.ahmetkeles.orderservice.outbox.event;

import java.math.BigDecimal;
import java.util.UUID;

public record OrderItemAddedEvent(
        UUID orderId,
        UUID productId,
        int quantity,
        BigDecimal unitPrice,
        BigDecimal totalAmount
) {
}
