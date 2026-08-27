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

    /**
     * Failures propagate to the container's error handler with their original
     * type so it can classify them: contract violations are wrapped in
     * {@link InvalidEventException} (non-retryable), while exceptions from the
     * order service — including transient database errors — are not wrapped
     * at all.
     */
    @KafkaListener(topics = "${app.kafka.inventory-events-topic}")
    public void consume(String message) {
        InventoryEventEnvelope envelope =
                parse(
                        message,
                        InventoryEventEnvelope.class,
                        "inventory event envelope"
                );

        if (INVENTORY_RESERVED.equals(envelope.eventType())) {
            handleInventoryReserved(envelope);
            return;
        }

        if (INVENTORY_RESERVATION_FAILED.equals(envelope.eventType())) {
            handleInventoryReservationFailed(envelope);
        }
    }

    private void handleInventoryReserved(InventoryEventEnvelope envelope) {
        InventoryReservedEvent event =
                parse(
                        envelope.payload(),
                        InventoryReservedEvent.class,
                        INVENTORY_RESERVED + " payload"
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
                parse(
                        envelope.payload(),
                        InventoryReservationFailedEvent.class,
                        INVENTORY_RESERVATION_FAILED + " payload"
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
