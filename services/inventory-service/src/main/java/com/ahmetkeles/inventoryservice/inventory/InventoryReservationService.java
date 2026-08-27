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

import java.time.Instant;
import java.util.List;
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
    private final InventoryReservationRepository inventoryReservationRepository;
    private final OrderInventoryStateRepository orderInventoryStateRepository;
    private final ObjectMapper objectMapper;

    public InventoryReservationService(
            InventoryItemRepository inventoryItemRepository,
            ProcessedEventRepository processedEventRepository,
            OutboxEventRepository outboxEventRepository,
            InventoryReservationRepository inventoryReservationRepository,
            OrderInventoryStateRepository orderInventoryStateRepository,
            ObjectMapper objectMapper
    ) {
        this.inventoryItemRepository = inventoryItemRepository;
        this.processedEventRepository = processedEventRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.inventoryReservationRepository = inventoryReservationRepository;
        this.orderInventoryStateRepository = orderInventoryStateRepository;
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

        OrderInventoryState state = lockOrderState(orderId);

        if (state.isCancelled()) {
            log.info(
                    "Skipping reservation for cancelled order {}, item {}, product {}: no stock mutated",
                    orderId,
                    orderItemId,
                    productId
            );

            processedEventRepository.save(
                    new ProcessedEvent(eventId, eventType)
            );

            return;
        }

        if (inventoryReservationRepository.existsById(orderItemId)) {
            log.warn(
                    "Order item {} already holds a reservation; skipping event {} without reserving again",
                    orderItemId,
                    eventId
            );

            processedEventRepository.save(
                    new ProcessedEvent(eventId, eventType)
            );

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

        inventoryReservationRepository.save(
                new InventoryReservation(
                        orderItemId,
                        orderId,
                        productId,
                        quantity,
                        eventId
                )
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

    /**
     * Applies an ORDER_CANCELLED event: marks the order cancelled in this
     * service's state table and returns every stock quantity the order still
     * holds, all in one transaction. Release is driven entirely by the
     * durable reservation ledger — the event carries no item details — so a
     * duplicate cancellation, which finds no RESERVED rows left, moves
     * nothing.
     */
    @Transactional
    public void releaseForCancelledOrder(
            UUID eventId,
            String eventType,
            UUID orderId
    ) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }

        OrderInventoryState state = lockOrderState(orderId);
        state.markCancelled();

        List<InventoryReservation> reservations =
                inventoryReservationRepository.findByOrderIdAndStatus(
                        orderId,
                        ReservationStatus.RESERVED
                );

        for (InventoryReservation reservation : reservations) {
            InventoryItem item = inventoryItemRepository
                    .findById(reservation.getProductId())
                    .orElseThrow(() -> new IllegalStateException(
                            "Reservation for order item "
                                    + reservation.getOrderItemId()
                                    + " references missing inventory item "
                                    + reservation.getProductId()
                    ));

            item.release(reservation.getQuantity());
            reservation.release();
        }

        processedEventRepository.save(
                new ProcessedEvent(eventId, eventType)
        );

        log.info(
                "Cancelled inventory state for order {}; released {} reservation(s)",
                orderId,
                reservations.size()
        );
    }

    /**
     * Upserts the order's state row and locks it. Every reserve/cancel
     * decision for one order runs under this lock, so a late reservation can
     * never slip past a concurrent release: whichever transaction wins the
     * lock, the other sees its committed outcome.
     */
    private OrderInventoryState lockOrderState(UUID orderId) {
        orderInventoryStateRepository.insertIfAbsent(orderId, Instant.now());

        return orderInventoryStateRepository.lockByOrderId(orderId)
                .orElseThrow(() -> new IllegalStateException(
                        "order_inventory_state row missing after upsert for order "
                                + orderId
                ));
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
