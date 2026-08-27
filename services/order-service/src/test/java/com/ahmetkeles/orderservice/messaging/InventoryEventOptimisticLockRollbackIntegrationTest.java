package com.ahmetkeles.orderservice.messaging;

import com.ahmetkeles.orderservice.PostgreSQLIntegrationTest;
import com.ahmetkeles.orderservice.domain.Order;
import com.ahmetkeles.orderservice.domain.OrderItem;
import com.ahmetkeles.orderservice.domain.OrderStatus;
import com.ahmetkeles.orderservice.outbox.OutboxEventRepository;
import com.ahmetkeles.orderservice.repository.OrderRepository;
import com.ahmetkeles.orderservice.service.OrderService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

/**
 * Proves that the A4 idempotency claim and the A5 optimistic lock compose:
 * when two distinct inventory events race on the same order and one loses
 * the version check, the loser's {@code processed_events} claim rolls back
 * with the order mutation — so redelivering that event can claim the same
 * eventId again and succeed. This is what makes an optimistic-lock failure
 * safe for the Kafka error handler to retry: a rolled-back attempt leaves no
 * committed marker behind.
 *
 * <p>The conflict is forced deterministically: a spy on
 * {@link OrderService#markItemReserved} parks each transaction on a barrier
 * after it has loaded and mutated the aggregate but before its transaction
 * commits, so both provably flush against the same loaded version.
 */
class InventoryEventOptimisticLockRollbackIntegrationTest
        extends PostgreSQLIntegrationTest {

    private static final int BARRIER_TIMEOUT_SECONDS = 30;
    private static final String INVENTORY_RESERVED = "INVENTORY_RESERVED";

    @Autowired
    private InventoryEventProcessor eventProcessor;

    @Autowired
    private ProcessedEventRepository processedEventRepository;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @MockitoSpyBean
    private OrderService orderService;

    @BeforeEach
    void clearDatabase() {
        processedEventRepository.deleteAll();
        outboxEventRepository.deleteAll();
        orderRepository.deleteAll();
    }

    @Test
    void losingEventsClaimRollsBackAndRedeliveryReclaimsAndConverges()
            throws Exception {
        Order order = orderService.createOrder(UUID.randomUUID(), "USD");
        UUID orderId = order.getId();
        orderService.addItem(orderId, UUID.randomUUID(), 1,
                new BigDecimal("10.00"));
        orderService.addItem(orderId, UUID.randomUUID(), 2,
                new BigDecimal("5.00"));

        List<OrderItem> items = orderService.getOrder(orderId).getItems();
        List<InventoryEventEnvelope> envelopes = List.of(
                reservedEnvelope(orderId),
                reservedEnvelope(orderId)
        );

        // Park each event's transaction after it has loaded and mutated the
        // order but before commit, so both flush against the same version.
        CyclicBarrier bothMutated = new CyclicBarrier(2);
        AtomicBoolean holdAtBarrier = new AtomicBoolean(true);

        doAnswer(invocation -> {
            Object result = invocation.callRealMethod();
            if (holdAtBarrier.get()) {
                bothMutated.await(BARRIER_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            return result;
        }).when(orderService).markItemReserved(any(), any());

        List<Throwable> failures = processConcurrently(envelopes, items);
        holdAtBarrier.set(false);

        int loser = indexOfSingleOptimisticLockFailure(failures);
        int winner = 1 - loser;
        UUID loserEventId = envelopes.get(loser).eventId();
        UUID winnerEventId = envelopes.get(winner).eventId();

        // The losing event's claim must have rolled back with its mutation:
        // a surviving marker would make the redelivery a silent no-op and
        // lose the event permanently.
        assertTrue(processedEventRepository.existsById(winnerEventId),
                "the committed event must keep its claim");
        assertFalse(processedEventRepository.existsById(loserEventId),
                "the rolled-back event must not keep a processed_events row");
        assertEquals(1, processedEventRepository.count());
        assertEquals(1, countReservedItems(orderId));

        // Redelivery of the losing event — what the Kafka error handler does
        // after backoff — must be able to claim the same eventId again.
        boolean redelivered = eventProcessor.processReserved(
                envelopes.get(loser),
                reservedEventFor(orderId, items.get(loser))
        );

        assertTrue(redelivered,
                "the redelivered event must win a fresh claim, not be "
                        + "treated as a duplicate");
        assertTrue(processedEventRepository.existsById(loserEventId));
        assertEquals(2, processedEventRepository.count());
        assertEquals(2, countReservedItems(orderId));
        assertEquals(OrderStatus.CONFIRMED,
                orderService.getOrder(orderId).getStatus());
    }

    private List<Throwable> processConcurrently(
            List<InventoryEventEnvelope> envelopes,
            List<OrderItem> items
    ) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);

        try {
            List<Future<Throwable>> results = new ArrayList<>();

            for (int i = 0; i < 2; i++) {
                InventoryEventEnvelope envelope = envelopes.get(i);
                InventoryReservedEvent event = reservedEventFor(
                        envelope.aggregateId(),
                        items.get(i)
                );

                results.add(executor.submit(() -> {
                    try {
                        eventProcessor.processReserved(envelope, event);
                        return null;
                    } catch (Throwable throwable) {
                        return throwable;
                    }
                }));
            }

            List<Throwable> failures = new ArrayList<>();
            for (Future<Throwable> result : results) {
                failures.add(result.get(BARRIER_TIMEOUT_SECONDS,
                        TimeUnit.SECONDS));
            }
            return failures;
        } finally {
            executor.shutdownNow();
        }
    }

    private static int indexOfSingleOptimisticLockFailure(
            List<Throwable> failures
    ) {
        List<Throwable> conflicts = failures.stream()
                .filter(failure -> failure != null)
                .toList();

        assertEquals(1, conflicts.size(),
                "expected exactly one failing event delivery: " + failures);
        assertInstanceOf(OptimisticLockingFailureException.class,
                conflicts.getFirst(),
                "the conflict must surface as a Spring optimistic-lock "
                        + "failure so the error handler classifies it as "
                        + "retryable");

        int loser = failures.indexOf(conflicts.getFirst());
        assertNull(failures.get(1 - loser));
        return loser;
    }

    private static InventoryEventEnvelope reservedEnvelope(UUID orderId) {
        return new InventoryEventEnvelope(
                UUID.randomUUID(),
                "Order",
                orderId,
                INVENTORY_RESERVED,
                "{}",
                Instant.now()
        );
    }

    private static InventoryReservedEvent reservedEventFor(
            UUID orderId,
            OrderItem item
    ) {
        return new InventoryReservedEvent(
                orderId,
                item.getId(),
                item.getProductId(),
                item.getQuantity()
        );
    }

    private int countReservedItems(UUID orderId) {
        return (int) orderRepository.findWithItemsById(orderId)
                .orElseThrow()
                .getItems().stream()
                .filter(OrderItem::isReserved)
                .count();
    }
}
