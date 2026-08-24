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
}
