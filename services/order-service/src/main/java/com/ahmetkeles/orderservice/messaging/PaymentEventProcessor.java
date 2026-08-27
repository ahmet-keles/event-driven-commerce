package com.ahmetkeles.orderservice.messaging;

import com.ahmetkeles.orderservice.service.OrderService;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

/**
 * Applies a supported payment event exactly once, keyed by the envelope's
 * eventId — the same claim-then-mutate contract as
 * {@link InventoryEventProcessor}: the idempotency claim and the order
 * mutation share one transaction, so any failure afterwards (domain
 * exception, flush error, an optimistic-locking conflict) rolls the claim
 * back with everything else and the redelivery can claim the same event
 * again. The claim must never move to its own transaction
 * ({@code REQUIRES_NEW} would commit a marker for rolled-back work, losing
 * the event permanently).
 *
 * <p>Each method returns {@code true} when this delivery won the claim and
 * {@code false} when the event was already processed by an earlier committed
 * delivery. A won claim can still be a domain no-op: the first terminal
 * payment outcome wins, so a late opposite outcome claims its event and
 * changes nothing.
 */
@Service
public class PaymentEventProcessor {

    private final ProcessedEventRepository processedEventRepository;
    private final OrderService orderService;

    public PaymentEventProcessor(
            ProcessedEventRepository processedEventRepository,
            OrderService orderService
    ) {
        this.processedEventRepository = processedEventRepository;
        this.orderService = orderService;
    }

    @Transactional
    public boolean processCompleted(
            PaymentEventEnvelope envelope,
            PaymentCompletedEvent event
    ) {
        return claimAndRun(
                envelope,
                () -> orderService.completePayment(event.orderId())
        );
    }

    @Transactional
    public boolean processFailed(
            PaymentEventEnvelope envelope,
            PaymentFailedEvent event
    ) {
        return claimAndRun(
                envelope,
                () -> orderService.failPayment(event.orderId())
        );
    }

    private boolean claimAndRun(
            PaymentEventEnvelope envelope,
            Runnable mutation
    ) {
        int claimed = processedEventRepository.claim(
                envelope.eventId(),
                envelope.eventType(),
                envelope.aggregateId(),
                Instant.now()
        );

        if (claimed == 0) {
            return false;
        }

        mutation.run();

        return true;
    }
}
