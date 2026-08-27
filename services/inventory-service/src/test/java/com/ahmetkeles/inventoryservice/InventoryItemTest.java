package com.ahmetkeles.inventoryservice;

import com.ahmetkeles.inventoryservice.inventory.InsufficientInventoryException;
import com.ahmetkeles.inventoryservice.inventory.InventoryItem;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class InventoryItemTest {

    @Test
    void reservesAvailableInventory() {
        InventoryItem item = new InventoryItem(
                UUID.randomUUID(),
                10
        );

        item.reserve(3);

        assertEquals(7, item.getAvailableQuantity());
        assertEquals(3, item.getReservedQuantity());
    }

    @Test
    void rejectsReservationLargerThanAvailableInventory() {
        InventoryItem item = new InventoryItem(
                UUID.randomUUID(),
                2
        );

        assertThrows(
                InsufficientInventoryException.class,
                () -> item.reserve(3)
        );

        assertEquals(2, item.getAvailableQuantity());
        assertEquals(0, item.getReservedQuantity());
    }

    @Test
    void rejectsZeroReservationQuantity() {
        InventoryItem item = new InventoryItem(
                UUID.randomUUID(),
                10
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> item.reserve(0)
        );
    }

    @Test
    void rejectsNegativeReservationQuantity() {
        InventoryItem item = new InventoryItem(
                UUID.randomUUID(),
                10
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> item.reserve(-1)
        );
    }

    @Test
    void rejectsNegativeInitialInventory() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new InventoryItem(
                        UUID.randomUUID(),
                        -1
                )
        );
    }

    @Test
    void rejectsNullProductId() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new InventoryItem(
                        null,
                        10
                )
        );
    }

    @Test
    void releaseReturnsReservedStockToAvailable() {
        InventoryItem item = new InventoryItem(UUID.randomUUID(), 10);
        item.reserve(4);

        item.release(4);

        assertEquals(10, item.getAvailableQuantity());
        assertEquals(0, item.getReservedQuantity());
    }

    @Test
    void partialReleaseKeepsRemainingReservation() {
        InventoryItem item = new InventoryItem(UUID.randomUUID(), 10);
        item.reserve(4);

        item.release(3);

        assertEquals(9, item.getAvailableQuantity());
        assertEquals(1, item.getReservedQuantity());
    }

    @Test
    void rejectsReleasingMoreThanReserved() {
        InventoryItem item = new InventoryItem(UUID.randomUUID(), 10);
        item.reserve(2);

        assertThrows(IllegalStateException.class, () -> item.release(3));
        assertEquals(8, item.getAvailableQuantity());
        assertEquals(2, item.getReservedQuantity());
    }

    @Test
    void rejectsZeroOrNegativeRelease() {
        InventoryItem item = new InventoryItem(UUID.randomUUID(), 10);
        item.reserve(2);

        assertThrows(IllegalArgumentException.class, () -> item.release(0));
        assertThrows(IllegalArgumentException.class, () -> item.release(-1));
    }
}
