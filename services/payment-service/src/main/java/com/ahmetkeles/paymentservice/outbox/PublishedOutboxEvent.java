package com.ahmetkeles.paymentservice.outbox;

import java.time.Instant;
import java.util.UUID;

public record PublishedOutboxEvent(
        UUID eventId,
        String aggregateType,
        UUID aggregateId,
        String eventType,
        String payload,
        Instant occurredAt
) {
    public static PublishedOutboxEvent from(OutboxEvent event) {
        return new PublishedOutboxEvent(
                event.getId(),
                event.getAggregateType(),
                event.getAggregateId(),
                event.getEventType(),
                event.getPayload(),
                event.getOccurredAt()
        );
    }
}
