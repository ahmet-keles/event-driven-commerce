package com.ahmetkeles.orderservice.messaging;

import com.ahmetkeles.orderservice.service.OrderService;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class InventoryEventsConsumerTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final OrderService orderService = mock(OrderService.class);
    private final InventoryEventsConsumer consumer =
            new InventoryEventsConsumer(objectMapper, orderService);

    @Test
    void processesInventoryReservedEvent() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID orderItemId = UUID.randomUUID();
        UUID productId = UUID.randomUUID();
        String payload = objectMapper.writeValueAsString(
                new InventoryReservedEvent(
                        orderId, orderItemId, productId, 3)
        );
        String message = objectMapper.writeValueAsString(
                new InventoryEventEnvelope(
                        UUID.randomUUID(),
                        "Order",
                        orderId,
                        "INVENTORY_RESERVED",
                        payload,
                        Instant.now()
                )
        );

        assertDoesNotThrow(() -> consumer.consume(message));

        verify(orderService).markItemReserved(orderId, orderItemId);
        verify(orderService, never()).cancelOrder(any());
    }

    @Test
    void processesInventoryReservationFailedEvent() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID orderItemId = UUID.randomUUID();
        UUID productId = UUID.randomUUID();
        String payload = objectMapper.writeValueAsString(
                new InventoryReservationFailedEvent(
                        orderId,
                        orderItemId,
                        productId,
                        3,
                        "INSUFFICIENT_INVENTORY"
                )
        );
        String message = objectMapper.writeValueAsString(
                new InventoryEventEnvelope(
                        UUID.randomUUID(),
                        "Order",
                        orderId,
                        "INVENTORY_RESERVATION_FAILED",
                        payload,
                        Instant.now()
                )
        );

        assertDoesNotThrow(() -> consumer.consume(message));

        verify(orderService).cancelOrder(orderId);
        verify(orderService, never()).markItemReserved(any(), any());
    }

    @Test
    void processesUnknownInventoryItemFailureEvent() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID orderItemId = UUID.randomUUID();
        UUID productId = UUID.randomUUID();
        String payload = objectMapper.writeValueAsString(
                new InventoryReservationFailedEvent(
                        orderId,
                        orderItemId,
                        productId,
                        3,
                        "INVENTORY_ITEM_NOT_FOUND"
                )
        );
        String message = objectMapper.writeValueAsString(
                new InventoryEventEnvelope(
                        UUID.randomUUID(),
                        "Order",
                        orderId,
                        "INVENTORY_RESERVATION_FAILED",
                        payload,
                        Instant.now()
                )
        );

        assertDoesNotThrow(() -> consumer.consume(message));

        verify(orderService).cancelOrder(orderId);
    }

    @Test
    void ignoresOtherInventoryEventTypes() throws Exception {
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

        verifyNoInteractions(orderService);
    }

    @Test
    void rejectsMalformedMessage() {
        assertThrows(
                InvalidEventException.class,
                () -> consumer.consume("not-json")
        );

        verifyNoInteractions(orderService);
    }
}
