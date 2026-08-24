package com.ahmetkeles.inventoryservice;

import com.ahmetkeles.inventoryservice.inventory.InsufficientInventoryException;
import com.ahmetkeles.inventoryservice.inventory.InventoryItem;
import com.ahmetkeles.inventoryservice.inventory.InventoryItemRepository;
import com.ahmetkeles.inventoryservice.inventory.InventoryReservationService;
import com.ahmetkeles.inventoryservice.inventory.ProcessedEventRepository;
import com.ahmetkeles.inventoryservice.outbox.OutboxEvent;
import com.ahmetkeles.inventoryservice.outbox.OutboxEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class InventoryReservationIntegrationTest
        extends PostgreSQLIntegrationTest {

    @Autowired
    private InventoryReservationService reservationService;

    @Autowired
    private InventoryItemRepository inventoryItemRepository;

    @Autowired
    private ProcessedEventRepository processedEventRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @BeforeEach
    void cleanDatabase() {
        outboxEventRepository.deleteAll();
        processedEventRepository.deleteAll();
        inventoryItemRepository.deleteAll();
    }

    @Test
    void reservesInventoryRecordsProcessedEventAndWritesOutboxEvent() {
        UUID eventId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        UUID productId = UUID.randomUUID();

        inventoryItemRepository.saveAndFlush(
                new InventoryItem(productId, 10)
        );

        reservationService.reserve(
                eventId,
                "ORDER_ITEM_ADDED",
                orderId,
                productId,
                3
        );

        InventoryItem item = inventoryItemRepository
                .findById(productId)
                .orElseThrow();

        assertEquals(7, item.getAvailableQuantity());
        assertEquals(3, item.getReservedQuantity());
        assertTrue(processedEventRepository.existsById(eventId));

        List<OutboxEvent> outboxEvents = outboxEventRepository.findAll();

        assertEquals(1, outboxEvents.size());

        OutboxEvent outboxEvent = outboxEvents.getFirst();

        assertEquals("Order", outboxEvent.getAggregateType());
        assertEquals(orderId, outboxEvent.getAggregateId());
        assertEquals("INVENTORY_RESERVED", outboxEvent.getEventType());
        assertNull(outboxEvent.getPublishedAt());

        assertTrue(outboxEvent.getPayload().contains(
                "\"orderId\":\"" + orderId + "\""
        ));
        assertTrue(outboxEvent.getPayload().contains(
                "\"productId\":\"" + productId + "\""
        ));
        assertTrue(outboxEvent.getPayload().contains(
                "\"quantity\":3"
        ));
    }

    @Test
    void duplicateEventDoesNotReserveOrWriteOutboxTwice() {
        UUID eventId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        UUID productId = UUID.randomUUID();

        inventoryItemRepository.saveAndFlush(
                new InventoryItem(productId, 10)
        );

        reservationService.reserve(
                eventId,
                "ORDER_ITEM_ADDED",
                orderId,
                productId,
                3
        );

        reservationService.reserve(
                eventId,
                "ORDER_ITEM_ADDED",
                orderId,
                productId,
                3
        );

        InventoryItem item = inventoryItemRepository
                .findById(productId)
                .orElseThrow();

        assertEquals(7, item.getAvailableQuantity());
        assertEquals(3, item.getReservedQuantity());
        assertEquals(1, processedEventRepository.count());
        assertEquals(1, outboxEventRepository.count());
    }

    @Test
    void insufficientInventoryDoesNotRecordEventOrWriteOutbox() {
        UUID eventId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        UUID productId = UUID.randomUUID();

        inventoryItemRepository.saveAndFlush(
                new InventoryItem(productId, 2)
        );

        assertThrows(
                InsufficientInventoryException.class,
                () -> reservationService.reserve(
                        eventId,
                        "ORDER_ITEM_ADDED",
                        orderId,
                        productId,
                        3
                )
        );

        InventoryItem item = inventoryItemRepository
                .findById(productId)
                .orElseThrow();

        assertEquals(2, item.getAvailableQuantity());
        assertEquals(0, item.getReservedQuantity());
        assertFalse(processedEventRepository.existsById(eventId));
        assertEquals(0, outboxEventRepository.count());
    }
}
