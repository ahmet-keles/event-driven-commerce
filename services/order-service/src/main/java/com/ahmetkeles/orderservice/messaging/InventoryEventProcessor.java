package com.ahmetkeles.orderservice.messaging;

import com.ahmetkeles.orderservice.service.OrderService;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

/**
 * Applies a supported inventory event exactly once, keyed by the envelope's
 * eventId.
 *
 * <p>The idempotency claim and the order mutation share one transaction: the
 * claim is inserted first, and any failure afterwards — domain exception,
 * flush error, a future optimistic-locking conflict — rolls the claim back
 * with everything else, so the redelivery that the error handler schedules can
 * claim the same event again. The claim must therefore never move to its own
 * transaction ({@code REQUIRES_NEW} would commit a marker for work that was
 * rolled back, losing the event permanently).
 *
 * <p>Each method returns {@code true} when this delivery performed the
 * mutation and {@code false} when the event was already processed by an
 * earlier committed delivery.
 */
@Service
public class InventoryEventProcessor {

    private final ProcessedEventRepository processedEventRepository;
    private final OrderService orderService;

    public InventoryEventProcessor(
            ProcessedEventRepository processedEventRepository,
            OrderService orderService
    ) {
        this.processedEventRepository = processedEventRepository;
        this.orderService = orderService;
    }

    @Transactional
    public boolean processReserved(
            InventoryEventEnvelope envelope,
            InventoryReservedEvent event
    ) {
        if (!claim(envelope)) {
            return false;
        }

        orderService.markItemReserved(event.orderId(), event.orderItemId());

        return true;
    }

    @Transactional
    public boolean processReservationFailed(
            InventoryEventEnvelope envelope,
            InventoryReservationFailedEvent event
    ) {
        if (!claim(envelope)) {
            return false;
        }

        orderService.cancelOrder(event.orderId());

        return true;
    }

    private boolean claim(InventoryEventEnvelope envelope) {
        return processedEventRepository.claim(
                envelope.eventId(),
                envelope.eventType(),
                envelope.aggregateId(),
                Instant.now()
        ) == 1;
    }
}
