package com.ahmetkeles.inventoryservice.messaging;

import com.ahmetkeles.inventoryservice.inventory.InventoryReservationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

@Component
public class OrderEventsConsumer {

    private static final Logger log =
            LoggerFactory.getLogger(OrderEventsConsumer.class);

    private static final String ORDER_ITEM_ADDED = "ORDER_ITEM_ADDED";

    private final ObjectMapper objectMapper;
    private final InventoryReservationService inventoryReservationService;

    public OrderEventsConsumer(
            ObjectMapper objectMapper,
            InventoryReservationService inventoryReservationService
    ) {
        this.objectMapper = objectMapper;
        this.inventoryReservationService = inventoryReservationService;
    }

    @KafkaListener(topics = "${app.kafka.order-events-topic}")
    public void consume(String message) {
        try {
            OrderEventEnvelope envelope =
                    objectMapper.readValue(
                            message,
                            OrderEventEnvelope.class
                    );

            if (!ORDER_ITEM_ADDED.equals(envelope.eventType())) {
                return;
            }

            OrderItemAddedEvent event =
                    objectMapper.readValue(
                            envelope.payload(),
                            OrderItemAddedEvent.class
                    );

            inventoryReservationService.reserve(
                    envelope.eventId(),
                    envelope.eventType(),
                    event.productId(),
                    event.quantity()
            );

            log.info(
                    "Reserved inventory for order {}, product {}, quantity {}",
                    event.orderId(),
                    event.productId(),
                    event.quantity()
            );
        } catch (Exception exception) {
            throw new IllegalStateException(
                    "Failed to process order event",
                    exception
            );
        }
    }
}
