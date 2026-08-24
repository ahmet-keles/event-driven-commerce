package com.ahmetkeles.inventoryservice.inventory;

import java.util.UUID;

public class InventoryItemNotFoundException extends RuntimeException {

    public InventoryItemNotFoundException(UUID productId) {
        super("Inventory item not found for product " + productId);
    }
}
