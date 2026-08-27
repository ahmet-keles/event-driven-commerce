package com.ahmetkeles.orderservice.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.util.UUID;

@Component
public class InventoryEventsConsumer {

    private static final Logger log =
            LoggerFactory.getLogger(InventoryEventsConsumer.class);

    private static final String INVENTORY_RESERVED = "INVENTORY_RESERVED";
    private static final String INVENTORY_RESERVATION_FAILED =
            "INVENTORY_RESERVATION_FAILED";

    private final ObjectMapper objectMapper;
    private final InventoryEventProcessor eventProcessor;

    public InventoryEventsConsumer(
            ObjectMapper objectMapper,
            InventoryEventProcessor eventProcessor
    ) {
        this.objectMapper = objectMapper;
        this.eventProcessor = eventProcessor;
    }

    /**
     * Failures propagate to the container's error handler with their original
     * type so it can classify them: contract violations — unparseable JSON,
     * a payload that does not match the announced event type, or an envelope
     * whose identity is missing or inconsistent — are wrapped in
     * {@link InvalidEventException} (non-retryable), while exceptions from
     * event processing — including transient database errors — are not
     * wrapped at all.
     *
     * <p>Supported events are durably deduplicated by envelope eventId via
     * {@link InventoryEventProcessor}; a redelivered event whose effects are
     * already committed is skipped without touching the order.
     */
    @KafkaListener(topics = "${app.kafka.inventory-events-topic}")
    public void consume(String message) {
        InventoryEventEnvelope envelope =
                parse(
                        message,
                        InventoryEventEnvelope.class,
                        "inventory event envelope"
                );

        if (INVENTORY_RESERVED.equals(envelope.eventType())) {
            handleInventoryReserved(envelope);
            return;
        }

        if (INVENTORY_RESERVATION_FAILED.equals(envelope.eventType())) {
            handleInventoryReservationFailed(envelope);
            return;
        }

        // Unsupported types are ignored WITHOUT a processed_events claim: a
        // claim row would permanently suppress replay of the same event once
        // a handler for that type ships.
        log.warn(
                "Ignoring unsupported inventory event type {} (eventId {})",
                envelope.eventType(),
                envelope.eventId()
        );
    }

    private void handleInventoryReserved(InventoryEventEnvelope envelope) {
        InventoryReservedEvent event =
                parse(
                        envelope.payload(),
                        InventoryReservedEvent.class,
                        INVENTORY_RESERVED + " payload"
                );

        validateIdentity(envelope, event.orderId());

        if (event.orderItemId() == null) {
            throw new InvalidEventException(
                    INVENTORY_RESERVED + " payload is missing orderItemId"
            );
        }

        boolean processed = eventProcessor.processReserved(envelope, event);

        if (!processed) {
            logDuplicateSkip(envelope, event.orderId());
            return;
        }

        log.info(
                "Recorded inventory reservation for order {}, item {}, product {}, quantity {}",
                event.orderId(),
                event.orderItemId(),
                event.productId(),
                event.quantity()
        );
    }

    private void handleInventoryReservationFailed(
            InventoryEventEnvelope envelope
    ) {
        InventoryReservationFailedEvent event =
                parse(
                        envelope.payload(),
                        InventoryReservationFailedEvent.class,
                        INVENTORY_RESERVATION_FAILED + " payload"
                );

        validateIdentity(envelope, event.orderId());

        boolean processed =
                eventProcessor.processReservationFailed(envelope, event);

        if (!processed) {
            logDuplicateSkip(envelope, event.orderId());
            return;
        }

        log.info(
                "Cancelled order {} after failed inventory reservation for product {}, quantity {}: {}",
                event.orderId(),
                event.productId(),
                event.requestedQuantity(),
                event.reason()
        );
    }

    private void logDuplicateSkip(
            InventoryEventEnvelope envelope,
            UUID orderId
    ) {
        log.info(
                "Skipped duplicate inventory event {} for order {}",
                envelope.eventId(),
                orderId
        );
    }

    /**
     * Identity checks run after parsing and before the idempotency claim, so
     * an event without a trustworthy identity never creates a
     * processed_events row.
     */
    private void validateIdentity(
            InventoryEventEnvelope envelope,
            UUID payloadOrderId
    ) {
        if (envelope.eventId() == null) {
            throw new InvalidEventException(
                    "inventory event envelope is missing eventId"
            );
        }

        if (payloadOrderId == null) {
            throw new InvalidEventException(
                    envelope.eventType() + " payload is missing orderId"
            );
        }

        if (!payloadOrderId.equals(envelope.aggregateId())) {
            throw new InvalidEventException(
                    "envelope aggregateId " + envelope.aggregateId()
                            + " does not match payload orderId "
                            + payloadOrderId
            );
        }
    }

    private <T> T parse(String json, Class<T> type, String description) {
        try {
            return objectMapper.readValue(json, type);
        } catch (RuntimeException exception) {
            throw new InvalidEventException(
                    "Malformed " + description,
                    exception
            );
        }
    }
}
