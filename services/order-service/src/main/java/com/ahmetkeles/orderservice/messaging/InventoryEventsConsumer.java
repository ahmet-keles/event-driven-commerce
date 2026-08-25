package com.ahmetkeles.orderservice.messaging;

import com.ahmetkeles.orderservice.service.OrderService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

@Component
public class InventoryEventsConsumer {

    private static final Logger log =
            LoggerFactory.getLogger(InventoryEventsConsumer.class);

    private static final String INVENTORY_RESERVED = "INVENTORY_RESERVED";

    private final ObjectMapper objectMapper;
    private final OrderService orderService;

    public InventoryEventsConsumer(
            ObjectMapper objectMapper,
            OrderService orderService
    ) {
        this.objectMapper = objectMapper;
        this.orderService = orderService;
    }

    @KafkaListener(topics = "${app.kafka.inventory-events-topic}")
    public void consume(String message) {
        try {
            InventoryEventEnvelope envelope =
                    objectMapper.readValue(
                            message,
                            InventoryEventEnvelope.class
                    );

            if (!INVENTORY_RESERVED.equals(envelope.eventType())) {
                return;
            }

            InventoryReservedEvent event =
                    objectMapper.readValue(
                            envelope.payload(),
                            InventoryReservedEvent.class
                    );

            orderService.confirmOrder(event.orderId());

            log.info(
                    "Confirmed order {} after inventory reservation for product {}, quantity {}",
                    event.orderId(),
                    event.productId(),
                    event.quantity()
            );
        } catch (Exception exception) {
            throw new IllegalStateException(
                    "Failed to process inventory event",
                    exception
            );
        }
    }
}
