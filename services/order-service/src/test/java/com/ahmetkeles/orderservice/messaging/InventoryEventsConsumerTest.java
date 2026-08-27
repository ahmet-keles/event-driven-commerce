package com.ahmetkeles.orderservice.messaging;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class InventoryEventsConsumerTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final InventoryEventProcessor eventProcessor =
            mock(InventoryEventProcessor.class);
    private final InventoryEventsConsumer consumer =
            new InventoryEventsConsumer(objectMapper, eventProcessor);

    @Test
    void processesInventoryReservedEvent() throws Exception {
        UUID eventId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        UUID orderItemId = UUID.randomUUID();
        UUID productId = UUID.randomUUID();
        InventoryReservedEvent event =
                new InventoryReservedEvent(orderId, orderItemId, productId, 3);
        when(eventProcessor.processReserved(any(), any())).thenReturn(true);

        assertDoesNotThrow(() -> consumer.consume(
                reservedMessage(eventId, orderId, event)
        ));

        verify(eventProcessor).processReserved(any(), eq(event));
        verify(eventProcessor, never())
                .processReservationFailed(any(), any());
    }

    @Test
    void processesInventoryReservationFailedEvent() throws Exception {
        UUID eventId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        InventoryReservationFailedEvent event =
                new InventoryReservationFailedEvent(
                        orderId,
                        UUID.randomUUID(),
                        UUID.randomUUID(),
                        3,
                        "INSUFFICIENT_INVENTORY"
                );
        when(eventProcessor.processReservationFailed(any(), any()))
                .thenReturn(true);

        assertDoesNotThrow(() -> consumer.consume(
                failedMessage(eventId, orderId, event)
        ));

        verify(eventProcessor).processReservationFailed(any(), eq(event));
        verify(eventProcessor, never()).processReserved(any(), any());
    }

    @Test
    void processesUnknownInventoryItemFailureEvent() throws Exception {
        UUID orderId = UUID.randomUUID();
        InventoryReservationFailedEvent event =
                new InventoryReservationFailedEvent(
                        orderId,
                        UUID.randomUUID(),
                        UUID.randomUUID(),
                        3,
                        "INVENTORY_ITEM_NOT_FOUND"
                );

        assertDoesNotThrow(() -> consumer.consume(
                failedMessage(UUID.randomUUID(), orderId, event)
        ));

        verify(eventProcessor).processReservationFailed(any(), eq(event));
    }

    @Test
    void duplicateDeliveryIsSkippedWithoutError() throws Exception {
        UUID orderId = UUID.randomUUID();
        InventoryReservedEvent event = new InventoryReservedEvent(
                orderId, UUID.randomUUID(), UUID.randomUUID(), 1);
        when(eventProcessor.processReserved(any(), any())).thenReturn(false);

        assertDoesNotThrow(() -> consumer.consume(
                reservedMessage(UUID.randomUUID(), orderId, event)
        ));
    }

    @Test
    void ignoresOtherInventoryEventTypesWithoutProcessing() throws Exception {
        String message = objectMapper.writeValueAsString(
                new InventoryEventEnvelope(
                        UUID.randomUUID(),
                        "Order",
                        UUID.randomUUID(),
                        "OTHER_EVENT",
                        "{}",
                        Instant.now()
                )
        );

        assertDoesNotThrow(() -> consumer.consume(message));

        verifyNoInteractions(eventProcessor);
    }

    @Test
    void rejectsMalformedMessage() {
        assertThrows(
                InvalidEventException.class,
                () -> consumer.consume("not-json")
        );

        verifyNoInteractions(eventProcessor);
    }

    @Test
    void rejectsMissingEventId() throws Exception {
        UUID orderId = UUID.randomUUID();
        InventoryReservedEvent event = new InventoryReservedEvent(
                orderId, UUID.randomUUID(), UUID.randomUUID(), 1);

        assertThrows(
                InvalidEventException.class,
                () -> consumer.consume(reservedMessage(null, orderId, event))
        );

        verifyNoInteractions(eventProcessor);
    }

    @Test
    void rejectsAggregateIdMismatchedWithPayloadOrderId() throws Exception {
        InventoryReservedEvent event = new InventoryReservedEvent(
                UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), 1);

        assertThrows(
                InvalidEventException.class,
                () -> consumer.consume(reservedMessage(
                        UUID.randomUUID(), UUID.randomUUID(), event
                ))
        );

        verifyNoInteractions(eventProcessor);
    }

    @Test
    void rejectsMissingPayloadOrderId() throws Exception {
        InventoryReservationFailedEvent event =
                new InventoryReservationFailedEvent(
                        null,
                        UUID.randomUUID(),
                        UUID.randomUUID(),
                        1,
                        "INSUFFICIENT_INVENTORY"
                );

        assertThrows(
                InvalidEventException.class,
                () -> consumer.consume(failedMessage(
                        UUID.randomUUID(), UUID.randomUUID(), event
                ))
        );

        verifyNoInteractions(eventProcessor);
    }

    @Test
    void rejectsReservedEventMissingOrderItemId() throws Exception {
        UUID orderId = UUID.randomUUID();
        InventoryReservedEvent event = new InventoryReservedEvent(
                orderId, null, UUID.randomUUID(), 1);

        assertThrows(
                InvalidEventException.class,
                () -> consumer.consume(reservedMessage(
                        UUID.randomUUID(), orderId, event
                ))
        );

        verifyNoInteractions(eventProcessor);
    }

    private String reservedMessage(
            UUID eventId,
            UUID aggregateId,
            InventoryReservedEvent event
    ) throws Exception {
        return envelope(
                eventId,
                aggregateId,
                "INVENTORY_RESERVED",
                objectMapper.writeValueAsString(event)
        );
    }

    private String failedMessage(
            UUID eventId,
            UUID aggregateId,
            InventoryReservationFailedEvent event
    ) throws Exception {
        return envelope(
                eventId,
                aggregateId,
                "INVENTORY_RESERVATION_FAILED",
                objectMapper.writeValueAsString(event)
        );
    }

    private String envelope(
            UUID eventId,
            UUID aggregateId,
            String eventType,
            String payload
    ) throws Exception {
        return objectMapper.writeValueAsString(
                new InventoryEventEnvelope(
                        eventId,
                        "Order",
                        aggregateId,
                        eventType,
                        payload,
                        Instant.now()
                )
        );
    }
}
