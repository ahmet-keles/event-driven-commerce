package com.ahmetkeles.orderservice.messaging;

import java.util.UUID;

public record InventoryReservedEvent(
        UUID orderId,
        UUID productId,
        int quantity
) {
}
