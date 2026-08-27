package com.ahmetkeles.inventoryservice;

import com.ahmetkeles.inventoryservice.inventory.InventoryReservationService;
import com.ahmetkeles.inventoryservice.messaging.InvalidEventException;
import com.ahmetkeles.inventoryservice.messaging.OrderEventEnvelope;
import com.ahmetkeles.inventoryservice.messaging.OrderEventsConsumer;
import com.ahmetkeles.inventoryservice.messaging.OrderCancelledEvent;
import com.ahmetkeles.inventoryservice.messaging.OrderItemAddedEvent;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

class OrderEventsConsumerTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final InventoryReservationService reservationService =
            mock(InventoryReservationService.class);
    private final OrderEventsConsumer consumer =
            new OrderEventsConsumer(
                    objectMapper,
                    reservationService
            );

    @Test
    void processesOrderItemAddedEvent() throws Exception {
        UUID eventId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        UUID orderItemId = UUID.randomUUID();
        UUID productId = UUID.randomUUID();

        String payload = objectMapper.writeValueAsString(
                new OrderItemAddedEvent(
                        orderId,
                        orderItemId,
                        productId,
                        3,
                        new BigDecimal("12.50"),
                        new BigDecimal("37.50")
                )
        );

        String message = objectMapper.writeValueAsString(
                new OrderEventEnvelope(
                        eventId,
                        "Order",
                        orderId,
                        "ORDER_ITEM_ADDED",
                        payload,
                        Instant.now()
                )
        );

        assertDoesNotThrow(() -> consumer.consume(message));

        verify(reservationService).reserve(
                eventId,
                "ORDER_ITEM_ADDED",
                orderId,
                orderItemId,
                productId,
                3
        );
    }

    @Test
    void ignoresOtherOrderEventTypes() throws Exception {
        String message = objectMapper.writeValueAsString(
                new OrderEventEnvelope(
                        UUID.randomUUID(),
                        "Order",
                        UUID.randomUUID(),
                        "ORDER_CREATED",
                        "{}",
                        Instant.now()
                )
        );

        assertDoesNotThrow(() -> consumer.consume(message));

        verifyNoInteractions(reservationService);
    }

    @Test
    void rejectsMalformedMessage() {
        assertThrows(
                InvalidEventException.class,
                () -> consumer.consume("not-json")
        );

        verifyNoInteractions(reservationService);
    }

    @Test
    void processesOrderCancelledEvent() throws Exception {
        UUID eventId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();

        String message = objectMapper.writeValueAsString(
                new OrderEventEnvelope(
                        eventId,
                        "Order",
                        orderId,
                        "ORDER_CANCELLED",
                        objectMapper.writeValueAsString(
                                new OrderCancelledEvent(orderId)),
                        Instant.now()
                )
        );

        assertDoesNotThrow(() -> consumer.consume(message));

        verify(reservationService).releaseForCancelledOrder(
                eventId,
                "ORDER_CANCELLED",
                orderId
        );
    }

    @Test
    void rejectsOrderCancelledWithMissingOrderId() throws Exception {
        String message = objectMapper.writeValueAsString(
                new OrderEventEnvelope(
                        UUID.randomUUID(),
                        "Order",
                        UUID.randomUUID(),
                        "ORDER_CANCELLED",
                        "{}",
                        Instant.now()
                )
        );

        assertThrows(
                InvalidEventException.class,
                () -> consumer.consume(message)
        );
        verify(reservationService, never())
                .releaseForCancelledOrder(any(), any(), any());
    }

    @Test
    void rejectsOrderCancelledWhoseAggregateIdDoesNotMatchPayload()
            throws Exception {
        String message = objectMapper.writeValueAsString(
                new OrderEventEnvelope(
                        UUID.randomUUID(),
                        "Order",
                        UUID.randomUUID(),
                        "ORDER_CANCELLED",
                        objectMapper.writeValueAsString(
                                new OrderCancelledEvent(UUID.randomUUID())),
                        Instant.now()
                )
        );

        assertThrows(
                InvalidEventException.class,
                () -> consumer.consume(message)
        );
        verify(reservationService, never())
                .releaseForCancelledOrder(any(), any(), any());
    }

    @Test
    void rejectsOrderCancelledWithMissingEventId() throws Exception {
        String message = objectMapper.writeValueAsString(
                new OrderEventEnvelope(
                        null,
                        "Order",
                        UUID.randomUUID(),
                        "ORDER_CANCELLED",
                        objectMapper.writeValueAsString(
                                new OrderCancelledEvent(UUID.randomUUID())),
                        Instant.now()
                )
        );

        assertThrows(
                InvalidEventException.class,
                () -> consumer.consume(message)
        );
        verify(reservationService, never())
                .releaseForCancelledOrder(any(), any(), any());
    }

    @Test
    void rejectsOrderItemAddedWithMissingOrderItemId() throws Exception {
        UUID orderId = UUID.randomUUID();

        String message = objectMapper.writeValueAsString(
                new OrderEventEnvelope(
                        UUID.randomUUID(),
                        "Order",
                        orderId,
                        "ORDER_ITEM_ADDED",
                        objectMapper.writeValueAsString(
                                new OrderItemAddedEvent(
                                        orderId,
                                        null,
                                        UUID.randomUUID(),
                                        3,
                                        new BigDecimal("12.50"),
                                        new BigDecimal("37.50")
                                )),
                        Instant.now()
                )
        );

        assertThrows(
                InvalidEventException.class,
                () -> consumer.consume(message)
        );
        verify(reservationService, never())
                .reserve(any(), any(), any(), any(), any(), anyInt());
    }
}
