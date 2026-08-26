package com.ahmetkeles.orderservice.messaging;

import java.util.UUID;

public record InventoryReservationFailedEvent(
        UUID orderId,
        UUID productId,
        int requestedQuantity,
        String reason
) {
}
