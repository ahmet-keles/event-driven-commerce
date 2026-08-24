package com.ahmetkeles.inventoryservice.messaging;

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
