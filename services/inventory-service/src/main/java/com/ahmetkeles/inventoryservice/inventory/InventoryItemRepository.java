package com.ahmetkeles.inventoryservice.inventory;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface InventoryItemRepository
        extends JpaRepository<InventoryItem, UUID> {
}
