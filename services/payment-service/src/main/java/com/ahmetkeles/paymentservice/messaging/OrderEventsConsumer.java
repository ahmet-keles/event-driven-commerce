package com.ahmetkeles.paymentservice.messaging;

import com.ahmetkeles.paymentservice.payment.PaymentService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

@Component
public class OrderEventsConsumer {

    private static final Logger log =
            LoggerFactory.getLogger(OrderEventsConsumer.class);

    private static final String ORDER_CONFIRMED = "ORDER_CONFIRMED";

    private final ObjectMapper objectMapper;
    private final PaymentService paymentService;

    public OrderEventsConsumer(
            ObjectMapper objectMapper,
            PaymentService paymentService
    ) {
        this.objectMapper = objectMapper;
        this.paymentService = paymentService;
    }

    /**
     * Failures propagate to the container's error handler with their original
     * type so it can classify them: contract violations are wrapped in
     * {@link InvalidEventException} (non-retryable), while exceptions from the
     * payment service — including transient database errors — are not wrapped
     * at all.
     */
    @KafkaListener(topics = "${app.kafka.order-events-topic}")
    public void consume(String message) {
        OrderEventEnvelope envelope =
                parse(message, OrderEventEnvelope.class, "order event envelope");

        if (!ORDER_CONFIRMED.equals(envelope.eventType())) {
            return;
        }

        OrderConfirmedEvent event =
                parse(
                        envelope.payload(),
                        OrderConfirmedEvent.class,
                        ORDER_CONFIRMED + " payload"
                );

        if (envelope.eventId() == null) {
            throw new InvalidEventException(
                    ORDER_CONFIRMED + " envelope is missing eventId"
            );
        }

        if (event.orderId() == null) {
            throw new InvalidEventException(
                    ORDER_CONFIRMED + " payload is missing orderId"
            );
        }

        if (event.customerId() == null) {
            throw new InvalidEventException(
                    ORDER_CONFIRMED + " payload is missing customerId"
            );
        }

        if (event.totalAmount() == null || event.totalAmount().signum() <= 0) {
            throw new InvalidEventException(
                    ORDER_CONFIRMED + " payload totalAmount must be greater than zero"
            );
        }

        if (event.currency() == null || event.currency().isBlank()) {
            throw new InvalidEventException(
                    ORDER_CONFIRMED + " payload is missing currency"
            );
        }

        if (!event.orderId().equals(envelope.aggregateId())) {
            throw new InvalidEventException(
                    "envelope aggregateId " + envelope.aggregateId()
                            + " does not match payload orderId "
                            + event.orderId()
            );
        }

        paymentService.processOrderConfirmed(
                envelope.eventId(),
                envelope.eventType(),
                event.orderId(),
                event.customerId(),
                event.totalAmount(),
                event.currency()
        );

        log.info(
                "Processed payment for order {}, customer {}, amount {} {}",
                event.orderId(),
                event.customerId(),
                event.totalAmount(),
                event.currency()
        );
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
