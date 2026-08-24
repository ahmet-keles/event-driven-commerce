package com.ahmetkeles.inventoryservice.inventory;

import com.ahmetkeles.inventoryservice.outbox.InventoryReservedEvent;
import com.ahmetkeles.inventoryservice.outbox.OutboxEvent;
import com.ahmetkeles.inventoryservice.outbox.OutboxEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

import java.util.UUID;

@Service
public class InventoryReservationService {

    private static final String INVENTORY_RESERVED = "INVENTORY_RESERVED";
    private static final String AGGREGATE_TYPE = "Order";

    private final InventoryItemRepository inventoryItemRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    public InventoryReservationService(
            InventoryItemRepository inventoryItemRepository,
            ProcessedEventRepository processedEventRepository,
            OutboxEventRepository outboxEventRepository,
            ObjectMapper objectMapper
    ) {
        this.inventoryItemRepository = inventoryItemRepository;
        this.processedEventRepository = processedEventRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public void reserve(
            UUID eventId,
            String eventType,
            UUID orderId,
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

        InventoryReservedEvent event =
                new InventoryReservedEvent(
                        orderId,
                        productId,
                        quantity
                );

        outboxEventRepository.save(
                new OutboxEvent(
                        AGGREGATE_TYPE,
                        orderId,
                        INVENTORY_RESERVED,
                        serialize(event)
                )
        );
    }

    private String serialize(InventoryReservedEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (Exception exception) {
            throw new IllegalStateException(
                    "Failed to serialize inventory event",
                    exception
            );
        }
    }
}
