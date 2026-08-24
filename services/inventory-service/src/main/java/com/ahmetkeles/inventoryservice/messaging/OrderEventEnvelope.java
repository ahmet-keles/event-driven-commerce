package com.ahmetkeles.inventoryservice.messaging;

import java.time.Instant;
import java.util.UUID;

public record OrderEventEnvelope(
        UUID eventId,
        String aggregateType,
        UUID aggregateId,
        String eventType,
        String payload,
        Instant occurredAt
) {
}
