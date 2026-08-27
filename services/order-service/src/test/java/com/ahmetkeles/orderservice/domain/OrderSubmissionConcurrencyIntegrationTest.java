package com.ahmetkeles.orderservice.domain;

import com.ahmetkeles.orderservice.PostgreSQLIntegrationTest;
import com.ahmetkeles.orderservice.outbox.OutboxEventRepository;
import com.ahmetkeles.orderservice.repository.OrderRepository;
import com.ahmetkeles.orderservice.service.OrderService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Proves the submission semantics under genuine concurrency: two separate
 * transactions on two threads, each with its own persistence context, both
 * loading the same aggregate version before either commits (coordinated by a
 * {@link CyclicBarrier}), against a real PostgreSQL.
 *
 * <ul>
 * <li><b>addItem vs submit</b>: optimistic locking serializes them into
 *     exactly one committed ordering. If submit wins, the retried addItem is
 *     rejected with {@link OrderNotModifiableException} and the submitted
 *     order contains exactly the pre-race items; if addItem wins, the retried
 *     submit succeeds and the item is part of the submitted order. Either
 *     way the final item set is exactly the winning ordering's set.</li>
 * <li><b>reservation vs submit</b>: whichever single transaction commits,
 *     the order is still PENDING — confirmation requires BOTH submitted and
 *     fully reserved, so no interleaving confirms prematurely. Retrying the
 *     loser completes the pair and confirms.</li>
 * </ul>
 */
class OrderSubmissionConcurrencyIntegrationTest
        extends PostgreSQLIntegrationTest {

    private static final int TIMEOUT_SECONDS = 30;

    @Autowired
    private OrderService orderService;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void clearDatabase() {
        outboxEventRepository.deleteAll();
        orderRepository.deleteAll();
    }

    @Test
    void addItemVersusSubmitCommitsExactlyOneOrdering() throws Exception {
        UUID orderId = orderService.createOrder(UUID.randomUUID(), "USD")
                .getId();
        UUID originalProductId = UUID.randomUUID();
        orderService.addItem(
                orderId, originalProductId, 1, new BigDecimal("10.00"));

        UUID racingProductId = UUID.randomUUID();
        long versionBefore = dbVersionOf(orderId);

        List<Throwable> failures = runConcurrently(
                orderId,
                order -> order.addItem(
                        racingProductId, 2, new BigDecimal("5.00")),
                Order::submit
        );

        int loser = failures.indexOf(exactlyOneOptimisticLockFailure(failures));

        assertEquals(versionBefore + 1, dbVersionOf(orderId),
                "exactly one of the two transactions may commit");

        Order afterRace = loadOrder(orderId);

        if (loser == 0) {
            // Submit won. The lost addItem was rolled back, and its retry —
            // the client re-issuing the request — is rejected with the
            // order_not_modifiable semantics.
            assertTrue(afterRace.isSubmitted());
            assertEquals(1, afterRace.getItems().size(),
                    "the losing addItem must leave no partial row behind");

            assertThrows(
                    OrderNotModifiableException.class,
                    () -> orderService.addItem(
                            orderId, racingProductId, 2,
                            new BigDecimal("5.00"))
            );

            Order finalOrder = loadOrder(orderId);
            assertEquals(1, finalOrder.getItems().size(),
                    "the submitted order contains exactly the pre-race item");
            assertEquals(originalProductId,
                    finalOrder.getItems().getFirst().getProductId());
            assertEquals(versionBefore + 1, dbVersionOf(orderId),
                    "a rejected addItem must not bump the version");
        } else {
            // addItem won. The new item is part of the order; the retried
            // submit sees and includes it.
            assertFalse(afterRace.isSubmitted());
            assertEquals(2, afterRace.getItems().size());

            orderService.submitOrder(orderId);

            Order finalOrder = loadOrder(orderId);
            assertTrue(finalOrder.isSubmitted());
            assertEquals(2, finalOrder.getItems().size(),
                    "the item that won the race is part of the "
                            + "submitted order");
        }

        assertEquals(OrderStatus.PENDING, loadOrder(orderId).getStatus(),
                "no interleaving of addItem and submit may confirm an "
                        + "order whose items are unreserved");
    }

    @Test
    void reservationVersusSubmitNeverConfirmsPrematurely() throws Exception {
        UUID orderId = orderService.createOrder(UUID.randomUUID(), "USD")
                .getId();
        orderService.addItem(
                orderId, UUID.randomUUID(), 1, new BigDecimal("10.00"));
        UUID onlyItemId = loadOrder(orderId)
                .getItems().getFirst().getId();

        long versionBefore = dbVersionOf(orderId);

        List<Throwable> failures = runConcurrently(
                orderId,
                order -> order.markItemReserved(onlyItemId),
                Order::submit
        );

        int loser = failures.indexOf(exactlyOneOptimisticLockFailure(failures));

        assertEquals(versionBefore + 1, dbVersionOf(orderId));

        // The heart of the confirmation rule: after exactly one of
        // {reserve, submit} has committed, the order must still be PENDING —
        // whichever one it was.
        Order afterRace = loadOrder(orderId);
        assertEquals(OrderStatus.PENDING, afterRace.getStatus(),
                "one half of {submitted, fully reserved} must never "
                        + "confirm the order");

        // Retry the loser from a fresh transaction: with both halves
        // committed the order confirms, and contains exactly its one item.
        if (loser == 0) {
            assertTrue(afterRace.isSubmitted());
            orderService.markItemReserved(orderId, onlyItemId);
        } else {
            assertTrue(afterRace.getItems().getFirst().isReserved());
            orderService.submitOrder(orderId);
        }

        Order finalOrder = loadOrder(orderId);
        assertEquals(OrderStatus.CONFIRMED, finalOrder.getStatus());
        assertTrue(finalOrder.isSubmitted());
        assertEquals(1, finalOrder.getItems().size());
        assertTrue(finalOrder.getItems().getFirst().isReserved());
        assertEquals(versionBefore + 2, dbVersionOf(orderId));
    }

    // -- harness (same shape as OrderOptimisticLockingIntegrationTest) ------

    private List<Throwable> runConcurrently(
            UUID orderId,
            Consumer<Order> firstMutation,
            Consumer<Order> secondMutation
    ) throws Exception {
        TransactionTemplate transactionTemplate =
                new TransactionTemplate(transactionManager);
        CyclicBarrier bothLoaded = new CyclicBarrier(2);
        ExecutorService executor = Executors.newFixedThreadPool(2);

        try {
            List<Future<Throwable>> results = new ArrayList<>();

            for (Consumer<Order> mutation :
                    List.of(firstMutation, secondMutation)) {
                results.add(executor.submit(() -> {
                    try {
                        transactionTemplate.executeWithoutResult(status -> {
                            Order order = orderRepository
                                    .findWithItemsById(orderId)
                                    .orElseThrow();

                            awaitBarrier(bothLoaded);

                            mutation.accept(order);
                        });
                        return null;
                    } catch (Throwable throwable) {
                        return throwable;
                    }
                }));
            }

            List<Throwable> failures = new ArrayList<>();
            for (Future<Throwable> result : results) {
                failures.add(result.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            }
            return failures;
        } finally {
            executor.shutdownNow();
        }
    }

    private static void awaitBarrier(CyclicBarrier barrier) {
        try {
            barrier.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (Exception exception) {
            throw new IllegalStateException(
                    "Concurrency barrier failed", exception);
        }
    }

    private static Throwable exactlyOneOptimisticLockFailure(
            List<Throwable> failures
    ) {
        List<Throwable> conflicts = failures.stream()
                .filter(OptimisticLockingFailureException.class::isInstance)
                .toList();

        assertEquals(1, conflicts.size(),
                "expected exactly one optimistic-lock conflict but got: "
                        + failures);
        assertTrue(failures.stream()
                        .allMatch(failure -> failure == null
                                || failure == conflicts.getFirst()),
                "the winning transaction must not fail: " + failures);

        Throwable conflict = conflicts.getFirst();
        assertNotNull(conflict);
        return conflict;
    }

    private Order loadOrder(UUID orderId) {
        return orderRepository.findWithItemsById(orderId).orElseThrow();
    }

    private long dbVersionOf(UUID orderId) {
        Long version = jdbcTemplate.queryForObject(
                "SELECT version FROM orders WHERE id = ?",
                Long.class,
                orderId);
        assertNotNull(version);
        return version;
    }
}
