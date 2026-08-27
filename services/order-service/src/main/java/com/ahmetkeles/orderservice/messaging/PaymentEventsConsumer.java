package com.ahmetkeles.orderservice.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.util.UUID;

@Component
public class PaymentEventsConsumer {

    private static final Logger log =
            LoggerFactory.getLogger(PaymentEventsConsumer.class);

    private static final String PAYMENT_COMPLETED = "PAYMENT_COMPLETED";
    private static final String PAYMENT_FAILED = "PAYMENT_FAILED";

    private final ObjectMapper objectMapper;
    private final PaymentEventProcessor eventProcessor;

    public PaymentEventsConsumer(
            ObjectMapper objectMapper,
            PaymentEventProcessor eventProcessor
    ) {
        this.objectMapper = objectMapper;
        this.eventProcessor = eventProcessor;
    }

    /**
     * Same failure contract as the inventory consumer: contract violations —
     * unparseable JSON, a payload that does not match the announced type, or
     * an envelope with missing or inconsistent identity — are wrapped in
     * {@link InvalidEventException} (non-retryable), while exceptions from
     * event processing, transient database errors included, propagate
     * unwrapped for the error handler to classify.
     *
     * <p>Supported events are durably deduplicated by envelope eventId via
     * {@link PaymentEventProcessor}; a redelivered event whose effects are
     * already committed is skipped without touching the order.
     */
    @KafkaListener(topics = "${app.kafka.payment-events-topic}")
    public void consume(String message) {
        PaymentEventEnvelope envelope =
                parse(
                        message,
                        PaymentEventEnvelope.class,
                        "payment event envelope"
                );

        if (PAYMENT_COMPLETED.equals(envelope.eventType())) {
            handlePaymentCompleted(envelope);
            return;
        }

        if (PAYMENT_FAILED.equals(envelope.eventType())) {
            handlePaymentFailed(envelope);
            return;
        }

        // Unsupported types are ignored WITHOUT a processed_events claim: a
        // claim row would permanently suppress replay of the same event once
        // a handler for that type ships.
        log.warn(
                "Ignoring unsupported payment event type {} (eventId {})",
                envelope.eventType(),
                envelope.eventId()
        );
    }

    private void handlePaymentCompleted(PaymentEventEnvelope envelope) {
        PaymentCompletedEvent event =
                parse(
                        envelope.payload(),
                        PaymentCompletedEvent.class,
                        PAYMENT_COMPLETED + " payload"
                );

        validateIdentity(envelope, event.orderId());

        boolean processed = eventProcessor.processCompleted(envelope, event);

        if (!processed) {
            logDuplicateSkip(envelope, event.orderId());
            return;
        }

        log.info(
                "Recorded completed payment {} for order {} ({} {})",
                event.paymentId(),
                event.orderId(),
                event.amount(),
                event.currency()
        );
    }

    private void handlePaymentFailed(PaymentEventEnvelope envelope) {
        PaymentFailedEvent event =
                parse(
                        envelope.payload(),
                        PaymentFailedEvent.class,
                        PAYMENT_FAILED + " payload"
                );

        validateIdentity(envelope, event.orderId());

        boolean processed = eventProcessor.processFailed(envelope, event);

        if (!processed) {
            logDuplicateSkip(envelope, event.orderId());
            return;
        }

        log.info(
                "Recorded failed payment {} for order {} ({} {}): {}",
                event.paymentId(),
                event.orderId(),
                event.amount(),
                event.currency(),
                event.reason()
        );
    }

    private void logDuplicateSkip(
            PaymentEventEnvelope envelope,
            UUID orderId
    ) {
        log.info(
                "Skipped duplicate payment event {} for order {}",
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
            PaymentEventEnvelope envelope,
            UUID payloadOrderId
    ) {
        if (envelope.eventId() == null) {
            throw new InvalidEventException(
                    "payment event envelope is missing eventId"
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
