package com.ahmetkeles.paymentservice;

import com.ahmetkeles.paymentservice.messaging.InvalidEventException;
import com.ahmetkeles.paymentservice.messaging.OrderConfirmedEvent;
import com.ahmetkeles.paymentservice.messaging.OrderEventEnvelope;
import com.ahmetkeles.paymentservice.messaging.OrderEventsConsumer;
import com.ahmetkeles.paymentservice.payment.PaymentService;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class OrderEventsConsumerTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final PaymentService paymentService = mock(PaymentService.class);
    private final OrderEventsConsumer consumer =
            new OrderEventsConsumer(objectMapper, paymentService);

    private String envelope(
            UUID eventId,
            UUID aggregateId,
            String eventType,
            String payload
    ) {
        return objectMapper.writeValueAsString(
                new OrderEventEnvelope(
                        eventId,
                        "Order",
                        aggregateId,
                        eventType,
                        payload,
                        Instant.now()
                )
        );
    }

    private String confirmedPayload(UUID orderId, UUID customerId, String amount) {
        return objectMapper.writeValueAsString(
                new OrderConfirmedEvent(
                        orderId,
                        customerId,
                        amount == null ? null : new BigDecimal(amount),
                        "USD"
                )
        );
    }

    @Test
    void processesOrderConfirmedEvent() {
        UUID eventId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        UUID customerId = UUID.randomUUID();

        String message = envelope(
                eventId,
                orderId,
                "ORDER_CONFIRMED",
                confirmedPayload(orderId, customerId, "42.50")
        );

        assertDoesNotThrow(() -> consumer.consume(message));

        verify(paymentService).processOrderConfirmed(
                eventId,
                "ORDER_CONFIRMED",
                orderId,
                customerId,
                new BigDecimal("42.50"),
                "USD"
        );
    }

    @Test
    void ignoresOtherEventTypes() {
        String message = envelope(
                UUID.randomUUID(),
                UUID.randomUUID(),
                "ORDER_ITEM_ADDED",
                "{}"
        );

        assertDoesNotThrow(() -> consumer.consume(message));

        verifyNoInteractions(paymentService);
    }

    @Test
    void malformedMessageIsInvalid() {
        assertThrows(
                InvalidEventException.class,
                () -> consumer.consume("not-json")
        );

        verifyNoInteractions(paymentService);
    }

    @Test
    void missingOrderIdIsInvalid() {
        UUID customerId = UUID.randomUUID();

        String message = envelope(
                UUID.randomUUID(),
                UUID.randomUUID(),
                "ORDER_CONFIRMED",
                confirmedPayload(null, customerId, "10.00")
        );

        assertThrows(
                InvalidEventException.class,
                () -> consumer.consume(message)
        );

        verifyNoInteractions(paymentService);
    }

    @Test
    void nonPositiveTotalAmountIsInvalid() {
        UUID orderId = UUID.randomUUID();

        String message = envelope(
                UUID.randomUUID(),
                orderId,
                "ORDER_CONFIRMED",
                confirmedPayload(orderId, UUID.randomUUID(), "0.00")
        );

        assertThrows(
                InvalidEventException.class,
                () -> consumer.consume(message)
        );

        verifyNoInteractions(paymentService);
    }

    @Test
    void aggregateIdMismatchIsInvalid() {
        UUID orderId = UUID.randomUUID();

        String message = envelope(
                UUID.randomUUID(),
                UUID.randomUUID(),
                "ORDER_CONFIRMED",
                confirmedPayload(orderId, UUID.randomUUID(), "10.00")
        );

        assertThrows(
                InvalidEventException.class,
                () -> consumer.consume(message)
        );

        verifyNoInteractions(paymentService);
    }
}
