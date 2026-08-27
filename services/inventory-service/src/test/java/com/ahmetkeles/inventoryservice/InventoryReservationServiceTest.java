package com.ahmetkeles.inventoryservice;

import com.ahmetkeles.inventoryservice.inventory.InventoryItem;
import com.ahmetkeles.inventoryservice.inventory.InventoryItemRepository;
import com.ahmetkeles.inventoryservice.inventory.InventoryReservation;
import com.ahmetkeles.inventoryservice.inventory.InventoryReservationRepository;
import com.ahmetkeles.inventoryservice.inventory.InventoryReservationService;
import com.ahmetkeles.inventoryservice.inventory.OrderInventoryState;
import com.ahmetkeles.inventoryservice.inventory.OrderInventoryStateRepository;
import com.ahmetkeles.inventoryservice.inventory.ProcessedEvent;
import com.ahmetkeles.inventoryservice.inventory.ProcessedEventRepository;
import com.ahmetkeles.inventoryservice.inventory.ReservationStatus;
import com.ahmetkeles.inventoryservice.outbox.OutboxEvent;
import com.ahmetkeles.inventoryservice.outbox.OutboxEventRepository;
import org.junit.jupiter.api.BeforeEach;
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

    private final InventoryReservationRepository inventoryReservationRepository =
            mock(InventoryReservationRepository.class);

    private final OrderInventoryStateRepository orderInventoryStateRepository =
            mock(OrderInventoryStateRepository.class);

    private final OrderInventoryState orderState =
            mock(OrderInventoryState.class);

    private final InventoryReservationService reservationService =
            new InventoryReservationService(
                    inventoryItemRepository,
                    processedEventRepository,
                    outboxEventRepository,
                    inventoryReservationRepository,
                    orderInventoryStateRepository,
                    new ObjectMapper()
            );

    private final UUID eventId = UUID.randomUUID();
    private final UUID orderId = UUID.randomUUID();
    private final UUID orderItemId = UUID.randomUUID();
    private final UUID productId = UUID.randomUUID();

    @BeforeEach
    void stubOrderStateLock() {
        when(orderInventoryStateRepository.lockByOrderId(orderId))
                .thenReturn(Optional.of(orderState));
    }

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

    @Test
    void cancelledOrderSkipsReservationButRecordsTheEvent() {
        when(processedEventRepository.existsById(eventId)).thenReturn(false);
        when(orderState.isCancelled()).thenReturn(true);

        reservationService.reserve(
                eventId,
                "ORDER_ITEM_ADDED",
                orderId,
                orderItemId,
                productId,
                3
        );

        verify(processedEventRepository).save(any(ProcessedEvent.class));
        verify(inventoryItemRepository, never()).findById(any());
        verify(inventoryReservationRepository, never())
                .save(any(InventoryReservation.class));
        verify(outboxEventRepository, never()).save(any(OutboxEvent.class));
    }

    @Test
    void alreadyReservedOrderItemIsNotReservedTwice() {
        when(processedEventRepository.existsById(eventId)).thenReturn(false);
        when(inventoryReservationRepository.existsById(orderItemId))
                .thenReturn(true);

        reservationService.reserve(
                eventId,
                "ORDER_ITEM_ADDED",
                orderId,
                orderItemId,
                productId,
                3
        );

        verify(processedEventRepository).save(any(ProcessedEvent.class));
        verify(inventoryItemRepository, never()).findById(any());
        verify(outboxEventRepository, never()).save(any(OutboxEvent.class));
    }

    @Test
    void successfulReservationWritesLedgerRow() {
        InventoryItem item = new InventoryItem(productId, 10);

        when(processedEventRepository.existsById(eventId)).thenReturn(false);
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

        ArgumentCaptor<InventoryReservation> captor =
                ArgumentCaptor.forClass(InventoryReservation.class);
        verify(inventoryReservationRepository).save(captor.capture());

        InventoryReservation reservation = captor.getValue();
        assertEquals(orderItemId, reservation.getOrderItemId());
        assertEquals(orderId, reservation.getOrderId());
        assertEquals(productId, reservation.getProductId());
        assertEquals(3, reservation.getQuantity());
        assertEquals(ReservationStatus.RESERVED, reservation.getStatus());
        assertEquals(eventId, reservation.getSourceEventId());
    }

    @Test
    void cancellationReleasesEveryReservedRowAndMarksThemReleased() {
        UUID cancelEventId = UUID.randomUUID();
        UUID otherProductId = UUID.randomUUID();
        InventoryItem first = new InventoryItem(productId, 10);
        first.reserve(3);
        InventoryItem second = new InventoryItem(otherProductId, 5);
        second.reserve(2);

        InventoryReservation firstReservation = new InventoryReservation(
                UUID.randomUUID(), orderId, productId, 3, UUID.randomUUID());
        InventoryReservation secondReservation = new InventoryReservation(
                UUID.randomUUID(), orderId, otherProductId, 2,
                UUID.randomUUID());

        when(processedEventRepository.existsById(cancelEventId))
                .thenReturn(false);
        when(inventoryReservationRepository.findByOrderIdAndStatus(
                orderId, ReservationStatus.RESERVED))
                .thenReturn(java.util.List.of(
                        firstReservation, secondReservation));
        when(inventoryItemRepository.findById(productId))
                .thenReturn(Optional.of(first));
        when(inventoryItemRepository.findById(otherProductId))
                .thenReturn(Optional.of(second));

        reservationService.releaseForCancelledOrder(
                cancelEventId,
                "ORDER_CANCELLED",
                orderId
        );

        verify(orderState).markCancelled();
        assertEquals(10, first.getAvailableQuantity());
        assertEquals(0, first.getReservedQuantity());
        assertEquals(5, second.getAvailableQuantity());
        assertEquals(0, second.getReservedQuantity());
        assertEquals(ReservationStatus.RELEASED, firstReservation.getStatus());
        assertEquals(ReservationStatus.RELEASED, secondReservation.getStatus());
        verify(processedEventRepository).save(any(ProcessedEvent.class));
    }

    @Test
    void duplicateCancellationEventIsIgnoredByProcessedCheck() {
        UUID cancelEventId = UUID.randomUUID();
        when(processedEventRepository.existsById(cancelEventId))
                .thenReturn(true);

        reservationService.releaseForCancelledOrder(
                cancelEventId,
                "ORDER_CANCELLED",
                orderId
        );

        verify(orderInventoryStateRepository, never()).lockByOrderId(any());
        verify(inventoryItemRepository, never()).findById(any());
        verify(processedEventRepository, never())
                .save(any(ProcessedEvent.class));
    }

    private OutboxEvent capturedOutboxEvent() {
        ArgumentCaptor<OutboxEvent> captor =
                ArgumentCaptor.forClass(OutboxEvent.class);

        verify(outboxEventRepository).save(captor.capture());

        return captor.getValue();
    }
}
