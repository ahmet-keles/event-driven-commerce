package com.ahmetkeles.inventoryservice.inventory;

import com.ahmetkeles.inventoryservice.outbox.InventoryReservationFailedEvent;
import com.ahmetkeles.inventoryservice.outbox.InventoryReservedEvent;
import com.ahmetkeles.inventoryservice.outbox.OutboxEvent;
import com.ahmetkeles.inventoryservice.outbox.OutboxEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

import java.util.UUID;

@Service
public class InventoryReservationService {

    private static final Logger log =
            LoggerFactory.getLogger(InventoryReservationService.class);

    private static final String INVENTORY_RESERVED = "INVENTORY_RESERVED";
    private static final String INVENTORY_RESERVATION_FAILED =
            "INVENTORY_RESERVATION_FAILED";
    private static final String AGGREGATE_TYPE = "Order";

    private static final String INSUFFICIENT_INVENTORY =
            "INSUFFICIENT_INVENTORY";
    private static final String INVENTORY_ITEM_NOT_FOUND =
            "INVENTORY_ITEM_NOT_FOUND";

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
            UUID orderItemId,
            UUID productId,
            int quantity
    ) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }

        try {
            InventoryItem item = inventoryItemRepository.findById(productId)
                    .orElseThrow(
                            () -> new InventoryItemNotFoundException(productId)
                    );

            item.reserve(quantity);
        } catch (InventoryItemNotFoundException exception) {
            recordReservationFailed(
                    eventId,
                    eventType,
                    orderId,
                    orderItemId,
                    productId,
                    quantity,
                    INVENTORY_ITEM_NOT_FOUND
            );

            return;
        } catch (InsufficientInventoryException exception) {
            recordReservationFailed(
                    eventId,
                    eventType,
                    orderId,
                    orderItemId,
                    productId,
                    quantity,
                    INSUFFICIENT_INVENTORY
            );

            return;
        }

        processedEventRepository.save(
                new ProcessedEvent(eventId, eventType)
        );

        InventoryReservedEvent event =
                new InventoryReservedEvent(
                        orderId,
                        orderItemId,
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

    private void recordReservationFailed(
            UUID eventId,
            String eventType,
            UUID orderId,
            UUID orderItemId,
            UUID productId,
            int quantity,
            String reason
    ) {
        log.warn(
                "Inventory reservation failed for order {}, product {}, quantity {}: {}",
                orderId,
                productId,
                quantity,
                reason
        );

        processedEventRepository.save(
                new ProcessedEvent(eventId, eventType)
        );

        InventoryReservationFailedEvent event =
                new InventoryReservationFailedEvent(
                        orderId,
                        orderItemId,
                        productId,
                        quantity,
                        reason
                );

        outboxEventRepository.save(
                new OutboxEvent(
                        AGGREGATE_TYPE,
                        orderId,
                        INVENTORY_RESERVATION_FAILED,
                        serialize(event)
                )
        );
    }

    private String serialize(Object event) {
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
