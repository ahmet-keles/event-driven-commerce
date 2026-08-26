package com.ahmetkeles.inventoryservice;

import com.ahmetkeles.inventoryservice.inventory.InventoryItem;
import com.ahmetkeles.inventoryservice.inventory.InventoryItemRepository;
import com.ahmetkeles.inventoryservice.inventory.InventoryReservationService;
import com.ahmetkeles.inventoryservice.inventory.ProcessedEvent;
import com.ahmetkeles.inventoryservice.inventory.ProcessedEventRepository;
import com.ahmetkeles.inventoryservice.outbox.OutboxEvent;
import com.ahmetkeles.inventoryservice.outbox.OutboxEventRepository;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import tools.jackson.databind.ObjectMapper;

import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class InventoryReservationServiceTest {

    private final InventoryItemRepository inventoryItemRepository =
            mock(InventoryItemRepository.class);

    private final ProcessedEventRepository processedEventRepository =
            mock(ProcessedEventRepository.class);

    private final OutboxEventRepository outboxEventRepository =
            mock(OutboxEventRepository.class);

    private final InventoryReservationService reservationService =
            new InventoryReservationService(
                    inventoryItemRepository,
                    processedEventRepository,
                    outboxEventRepository,
                    new ObjectMapper()
            );

    private final UUID eventId = UUID.randomUUID();
    private final UUID orderId = UUID.randomUUID();
    private final UUID orderItemId = UUID.randomUUID();
    private final UUID productId = UUID.randomUUID();

    @Test
    void insufficientInventoryWritesSingleReservationFailedEvent() {
        InventoryItem item = new InventoryItem(productId, 2);

        when(processedEventRepository.existsById(eventId))
                .thenReturn(false);
        when(inventoryItemRepository.findById(productId))
                .thenReturn(Optional.of(item));

        assertDoesNotThrow(() -> reservationService.reserve(
                eventId,
                "ORDER_ITEM_ADDED",
                orderId,
                orderItemId,
                productId,
                3
        ));

        assertEquals(2, item.getAvailableQuantity());
        assertEquals(0, item.getReservedQuantity());

        verify(processedEventRepository).save(any(ProcessedEvent.class));

        OutboxEvent outboxEvent = capturedOutboxEvent();

        assertEquals("Order", outboxEvent.getAggregateType());
        assertEquals(orderId, outboxEvent.getAggregateId());
        assertEquals(
                "INVENTORY_RESERVATION_FAILED",
                outboxEvent.getEventType()
        );
        assertTrue(outboxEvent.getPayload().contains(
                "\"reason\":\"INSUFFICIENT_INVENTORY\""
        ));
        assertTrue(outboxEvent.getPayload().contains(
                "\"requestedQuantity\":3"
        ));
    }

    @Test
    void unknownInventoryItemWritesSingleReservationFailedEvent() {
        when(processedEventRepository.existsById(eventId))
                .thenReturn(false);
        when(inventoryItemRepository.findById(productId))
                .thenReturn(Optional.empty());

        assertDoesNotThrow(() -> reservationService.reserve(
                eventId,
                "ORDER_ITEM_ADDED",
                orderId,
                orderItemId,
                productId,
                3
        ));

        verify(processedEventRepository).save(any(ProcessedEvent.class));

        OutboxEvent outboxEvent = capturedOutboxEvent();

        assertEquals(orderId, outboxEvent.getAggregateId());
        assertEquals(
                "INVENTORY_RESERVATION_FAILED",
                outboxEvent.getEventType()
        );
        assertTrue(outboxEvent.getPayload().contains(
                "\"reason\":\"INVENTORY_ITEM_NOT_FOUND\""
        ));
    }

    @Test
    void duplicateEventIsIgnoredBeforeAnyReservationAttempt() {
        when(processedEventRepository.existsById(eventId))
                .thenReturn(true);

        reservationService.reserve(
                eventId,
                "ORDER_ITEM_ADDED",
                orderId,
                orderItemId,
                productId,
                3
        );

        verify(inventoryItemRepository, never()).findById(any());
        verify(processedEventRepository, never())
                .save(any(ProcessedEvent.class));
        verify(outboxEventRepository, never())
                .save(any(OutboxEvent.class));
    }

    @Test
    void successfulReservationStillWritesReservedEvent() {
        InventoryItem item = new InventoryItem(productId, 10);

        when(processedEventRepository.existsById(eventId))
                .thenReturn(false);
        when(inventoryItemRepository.findById(productId))
                .thenReturn(Optional.of(item));

        reservationService.reserve(
                eventId,
                "ORDER_ITEM_ADDED",
                orderId,
                orderItemId,
                productId,
                3
        );

        assertEquals(7, item.getAvailableQuantity());
        assertEquals(3, item.getReservedQuantity());

        verify(processedEventRepository).save(any(ProcessedEvent.class));

        OutboxEvent outboxEvent = capturedOutboxEvent();

        assertEquals("INVENTORY_RESERVED", outboxEvent.getEventType());
        assertTrue(outboxEvent.getPayload().contains(
                "\"quantity\":3"
        ));
    }

    private OutboxEvent capturedOutboxEvent() {
        ArgumentCaptor<OutboxEvent> captor =
                ArgumentCaptor.forClass(OutboxEvent.class);

        verify(outboxEventRepository).save(captor.capture());

        return captor.getValue();
    }
}
