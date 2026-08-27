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

    /**
     * Failures propagate to the container's error handler with their original
     * type so it can classify them: contract violations are wrapped in
     * {@link InvalidEventException} (non-retryable), while exceptions from the
     * reservation service — including transient database errors — are not
     * wrapped at all.
     */
    @KafkaListener(topics = "${app.kafka.order-events-topic}")
    public void consume(String message) {
        OrderEventEnvelope envelope =
                parse(message, OrderEventEnvelope.class, "order event envelope");

        if (!ORDER_ITEM_ADDED.equals(envelope.eventType())) {
            return;
        }

        OrderItemAddedEvent event =
                parse(
                        envelope.payload(),
                        OrderItemAddedEvent.class,
                        ORDER_ITEM_ADDED + " payload"
                );

        inventoryReservationService.reserve(
                envelope.eventId(),
                envelope.eventType(),
                event.orderId(),
                event.orderItemId(),
                event.productId(),
                event.quantity()
        );

        log.info(
                "Processed inventory reservation for order {}, item {}, product {}, quantity {}",
                event.orderId(),
                event.orderItemId(),
                event.productId(),
                event.quantity()
        );
    }

    private <T> T parse(String json, Class<T> type, String description) {
        try {
            return objectMapper.readValue(json, type);
        } catch (RuntimeException exception) {
            throw new InvalidEventException(
                    "Malformed " + description,
                    exception
            );
        }
    }
}
