package com.ahmetkeles.inventoryservice.outbox;

import java.util.UUID;

public record InventoryReservedEvent(
        UUID orderId,
        UUID productId,
        int quantity
) {
}
