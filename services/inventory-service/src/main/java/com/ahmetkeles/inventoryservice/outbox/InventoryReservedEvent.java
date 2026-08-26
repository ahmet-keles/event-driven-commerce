package com.ahmetkeles.inventoryservice.outbox;

import java.util.UUID;

public record InventoryReservedEvent(
        UUID orderId,
        UUID orderItemId,
        UUID productId,
        int quantity
) {
}
