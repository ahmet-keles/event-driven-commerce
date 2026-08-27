package com.ahmetkeles.orderservice.messaging;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PaymentEventsConsumerTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final PaymentEventProcessor eventProcessor =
            mock(PaymentEventProcessor.class);
    private final PaymentEventsConsumer consumer =
            new PaymentEventsConsumer(objectMapper, eventProcessor);

    @Test
    void processesPaymentCompletedEvent() throws Exception {
        UUID orderId = UUID.randomUUID();
        String message = envelope(
                UUID.randomUUID(),
                orderId,
                "PAYMENT_COMPLETED",
                objectMapper.writeValueAsString(new PaymentCompletedEvent(
                        orderId, UUID.randomUUID(),
                        new BigDecimal("37.50"), "USD")));

        when(eventProcessor.processCompleted(any(), any())).thenReturn(true);

        assertDoesNotThrow(() -> consumer.consume(message));

        verify(eventProcessor).processCompleted(any(), any());
        verify(eventProcessor, never()).processFailed(any(), any());
    }

    @Test
    void processesPaymentFailedEvent() throws Exception {
        UUID orderId = UUID.randomUUID();
        String message = envelope(
                UUID.randomUUID(),
                orderId,
                "PAYMENT_FAILED",
                objectMapper.writeValueAsString(new PaymentFailedEvent(
                        orderId, UUID.randomUUID(),
                        new BigDecimal("37.50"), "USD",
                        "CARD_DECLINED")));

        when(eventProcessor.processFailed(any(), any())).thenReturn(true);

        assertDoesNotThrow(() -> consumer.consume(message));

        verify(eventProcessor).processFailed(any(), any());
        verify(eventProcessor, never()).processCompleted(any(), any());
    }

    @Test
    void ignoresOtherPaymentEventTypes() throws Exception {
        String message = envelope(
                UUID.randomUUID(),
                UUID.randomUUID(),
                "PAYMENT_AUTHORIZED",
                "{}");

        assertDoesNotThrow(() -> consumer.consume(message));

        verify(eventProcessor, never()).processCompleted(any(), any());
        verify(eventProcessor, never()).processFailed(any(), any());
    }

    @Test
    void rejectsMalformedMessage() {
        assertThrows(
                InvalidEventException.class,
                () -> consumer.consume("not json")
        );
    }

    @Test
    void rejectsPaymentEventWithMissingOrderId() throws Exception {
        String message = envelope(
                UUID.randomUUID(),
                UUID.randomUUID(),
                "PAYMENT_COMPLETED",
                "{}");

        assertThrows(
                InvalidEventException.class,
                () -> consumer.consume(message)
        );
        verify(eventProcessor, never()).processCompleted(any(), any());
    }

    @Test
    void rejectsPaymentEventWhoseAggregateIdDoesNotMatchPayload()
            throws Exception {
        String message = envelope(
                UUID.randomUUID(),
                UUID.randomUUID(),
                "PAYMENT_FAILED",
                objectMapper.writeValueAsString(new PaymentFailedEvent(
                        UUID.randomUUID(), UUID.randomUUID(),
                        new BigDecimal("1.00"), "USD", "CARD_DECLINED")));

        assertThrows(
                InvalidEventException.class,
                () -> consumer.consume(message)
        );
        verify(eventProcessor, never()).processFailed(any(), any());
    }

    @Test
    void rejectsPaymentEventWithMissingEventId() throws Exception {
        UUID orderId = UUID.randomUUID();
        String message = envelope(
                null,
                orderId,
                "PAYMENT_COMPLETED",
                objectMapper.writeValueAsString(new PaymentCompletedEvent(
                        orderId, UUID.randomUUID(),
                        new BigDecimal("1.00"), "USD")));

        assertThrows(
                InvalidEventException.class,
                () -> consumer.consume(message)
        );
        verify(eventProcessor, never()).processCompleted(any(), any());
    }

    private String envelope(
            UUID eventId,
            UUID orderId,
            String eventType,
            String payload
    ) throws Exception {
        return objectMapper.writeValueAsString(new PaymentEventEnvelope(
                eventId,
                "Order",
                orderId,
                eventType,
                payload,
                Instant.now()
        ));
    }
}
