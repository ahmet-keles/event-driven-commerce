package com.ahmetkeles.inventoryservice.outbox;

import java.util.UUID;

public record InventoryReservationFailedEvent(
        UUID orderId,
        UUID orderItemId,
        UUID productId,
        int requestedQuantity,
        String reason
) {
}
