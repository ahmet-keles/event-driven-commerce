package com.ahmetkeles.orderservice.messaging;

import java.time.Instant;
import java.util.UUID;

public record InventoryEventEnvelope(
        UUID eventId,
        String aggregateType,
        UUID aggregateId,
        String eventType,
        String payload,
        Instant occurredAt
) {
}
