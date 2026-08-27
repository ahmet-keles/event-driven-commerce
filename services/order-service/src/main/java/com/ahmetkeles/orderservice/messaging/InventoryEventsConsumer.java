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
    private static final String INVENTORY_RESERVATION_FAILED =
            "INVENTORY_RESERVATION_FAILED";

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

            if (INVENTORY_RESERVED.equals(envelope.eventType())) {
                handleInventoryReserved(envelope);
                return;
            }

            if (INVENTORY_RESERVATION_FAILED.equals(envelope.eventType())) {
                handleInventoryReservationFailed(envelope);
            }
        } catch (Exception exception) {
            throw new IllegalStateException(
                    "Failed to process inventory event",
                    exception
            );
        }
    }

    private void handleInventoryReserved(InventoryEventEnvelope envelope) {
        InventoryReservedEvent event =
                objectMapper.readValue(
                        envelope.payload(),
                        InventoryReservedEvent.class
                );

        orderService.markItemReserved(event.orderId(), event.orderItemId());

        log.info(
                "Recorded inventory reservation for order {}, item {}, product {}, quantity {}",
                event.orderId(),
                event.orderItemId(),
                event.productId(),
                event.quantity()
        );
    }

    private void handleInventoryReservationFailed(
            InventoryEventEnvelope envelope
    ) {
        InventoryReservationFailedEvent event =
                objectMapper.readValue(
                        envelope.payload(),
                        InventoryReservationFailedEvent.class
                );

        orderService.cancelOrder(event.orderId());

        log.info(
                "Cancelled order {} after failed inventory reservation for product {}, quantity {}: {}",
                event.orderId(),
                event.productId(),
                event.requestedQuantity(),
                event.reason()
        );
    }
}
