package com.ahmetkeles.inventoryservice.inventory;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
public class InventoryReservationService {

    private final InventoryItemRepository inventoryItemRepository;
    private final ProcessedEventRepository processedEventRepository;

    public InventoryReservationService(
            InventoryItemRepository inventoryItemRepository,
            ProcessedEventRepository processedEventRepository
    ) {
        this.inventoryItemRepository = inventoryItemRepository;
        this.processedEventRepository = processedEventRepository;
    }

    @Transactional
    public void reserve(
            UUID eventId,
            String eventType,
            UUID productId,
            int quantity
    ) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }

        InventoryItem item = inventoryItemRepository.findById(productId)
                .orElseThrow(
                        () -> new InventoryItemNotFoundException(productId)
                );

        item.reserve(quantity);

        processedEventRepository.save(
                new ProcessedEvent(eventId, eventType)
        );
    }
}
