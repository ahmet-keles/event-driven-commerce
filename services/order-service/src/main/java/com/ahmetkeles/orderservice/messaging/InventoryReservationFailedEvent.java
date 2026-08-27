package com.ahmetkeles.orderservice.messaging;

import java.util.UUID;

public record InventoryReservationFailedEvent(
        UUID orderId,
        UUID orderItemId,
        UUID productId,
        int requestedQuantity,
        String reason
) {
}
