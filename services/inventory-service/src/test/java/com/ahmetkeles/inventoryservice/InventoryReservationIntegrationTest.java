package com.ahmetkeles.inventoryservice;

import com.ahmetkeles.inventoryservice.inventory.InsufficientInventoryException;
import com.ahmetkeles.inventoryservice.inventory.InventoryItem;
import com.ahmetkeles.inventoryservice.inventory.InventoryItemRepository;
import com.ahmetkeles.inventoryservice.inventory.InventoryReservationService;
import com.ahmetkeles.inventoryservice.inventory.ProcessedEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

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

    @BeforeEach
    void cleanDatabase() {
        processedEventRepository.deleteAll();
        inventoryItemRepository.deleteAll();
    }

    @Test
    void reservesInventoryAndRecordsProcessedEvent() {
        UUID productId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();

        inventoryItemRepository.saveAndFlush(
                new InventoryItem(productId, 10)
        );

        reservationService.reserve(
                eventId,
                "ORDER_ITEM_ADDED",
                productId,
                3
        );

        InventoryItem item = inventoryItemRepository
                .findById(productId)
                .orElseThrow();

        assertEquals(7, item.getAvailableQuantity());
        assertEquals(3, item.getReservedQuantity());

        assertTrue(processedEventRepository.existsById(eventId));
        assertEquals(1, processedEventRepository.count());
    }

    @Test
    void duplicateEventDoesNotReserveInventoryTwice() {
        UUID productId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();

        inventoryItemRepository.saveAndFlush(
                new InventoryItem(productId, 10)
        );

        reservationService.reserve(
                eventId,
                "ORDER_ITEM_ADDED",
                productId,
                3
        );

        reservationService.reserve(
                eventId,
                "ORDER_ITEM_ADDED",
                productId,
                3
        );

        InventoryItem item = inventoryItemRepository
                .findById(productId)
                .orElseThrow();

        assertEquals(7, item.getAvailableQuantity());
        assertEquals(3, item.getReservedQuantity());
        assertEquals(1, processedEventRepository.count());
    }

    @Test
    void insufficientInventoryDoesNotRecordEvent() {
        UUID productId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();

        inventoryItemRepository.saveAndFlush(
                new InventoryItem(productId, 2)
        );

        assertThrows(
                InsufficientInventoryException.class,
                () -> reservationService.reserve(
                        eventId,
                        "ORDER_ITEM_ADDED",
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
    }
}
