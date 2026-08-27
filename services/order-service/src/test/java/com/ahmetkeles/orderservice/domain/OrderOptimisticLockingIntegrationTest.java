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
import java.time.OffsetDateTime;
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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Proves the optimistic-locking contract of the Order aggregate against a
 * real PostgreSQL, without any shared persistence context between competing
 * transactions:
 *
 * <ul>
 * <li>a real child reservation transition increments the root version,
 *     because {@link Order#markItemReserved} touches parent state;</li>
 * <li>duplicate and unknown reservations are true no-ops — no version
 *     increment, no {@code updated_at} write;</li>
 * <li>two transactions that both load version N and write conflicting state
 *     produce exactly one commit and one
 *     {@link OptimisticLockingFailureException}, never a silent lost
 *     update;</li>
 * <li>retrying the loser from a fresh transaction converges the aggregate,
 *     and terminal state stays latched under a reserve-versus-cancel
 *     race.</li>
 * </ul>
 *
 * <p>Concurrency is coordinated with a {@link CyclicBarrier} placed after
 * both transactions have loaded the aggregate, so both provably work from
 * the same version; which transaction commits first is left to the database
 * and asserted structurally (exactly one winner) rather than by identity.
 */
class OrderOptimisticLockingIntegrationTest extends PostgreSQLIntegrationTest {

    private static final int BARRIER_TIMEOUT_SECONDS = 30;

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

    // -- A: version lifecycle -----------------------------------------------

    @Test
    void newOrderStartsUnversionedAndIsPersistedAtVersionZero() {
        Order unsaved = new Order(UUID.randomUUID(), "USD");
        assertNull(unsaved.getVersion(),
                "unsaved order must carry a null version so Spring Data "
                        + "treats it as new despite the assigned UUID id");

        Order saved = orderService.createOrder(UUID.randomUUID(), "USD");

        assertEquals(0L, saved.getVersion());
        assertEquals(0L, dbVersionOf(saved.getId()));
    }

    // -- B/C/D: what moves the root version ---------------------------------

    @Test
    void reservingAPreviouslyUnreservedItemIncrementsRootVersion() {
        UUID orderId = createOrderWithItems(2);
        List<UUID> itemIds = itemIds(orderId);
        long versionBefore = dbVersionOf(orderId);

        orderService.markItemReserved(orderId, itemIds.get(0));

        assertEquals(versionBefore + 1, dbVersionOf(orderId),
                "a real reservation transition mutates parent state "
                        + "(updatedAt), so the root version must increment");
    }

    @Test
    void duplicateReservationDoesNotIncrementRootVersionOrTouchUpdatedAt() {
        UUID orderId = createOrderWithItems(2);
        List<UUID> itemIds = itemIds(orderId);

        orderService.markItemReserved(orderId, itemIds.get(0));

        long versionAfterFirst = dbVersionOf(orderId);
        OffsetDateTime updatedAtAfterFirst = dbUpdatedAtOf(orderId);

        orderService.markItemReserved(orderId, itemIds.get(0));

        assertEquals(versionAfterFirst, dbVersionOf(orderId),
                "an already-reserved item must be a DB-visible no-op");
        assertEquals(updatedAtAfterFirst, dbUpdatedAtOf(orderId));
    }

    @Test
    void unknownItemReservationDoesNotIncrementRootVersionOrTouchUpdatedAt() {
        UUID orderId = createOrderWithItems(2);
        long versionBefore = dbVersionOf(orderId);
        OffsetDateTime updatedAtBefore = dbUpdatedAtOf(orderId);

        orderService.markItemReserved(orderId, UUID.randomUUID());

        assertEquals(versionBefore, dbVersionOf(orderId),
                "an unknown item id must be a DB-visible no-op");
        assertEquals(updatedAtBefore, dbUpdatedAtOf(orderId));
    }

    // -- E/F: lost-update prevention and retry convergence ------------------

    @Test
    void concurrentDifferentItemReservationsConflictOnceAndRetryConverges()
            throws Exception {
        UUID orderId = createOrderWithItems(2);
        List<UUID> itemIds = itemIds(orderId);
        long versionBefore = dbVersionOf(orderId);

        List<Throwable> failures = runConcurrently(
                orderId,
                order -> order.markItemReserved(itemIds.get(0)),
                order -> order.markItemReserved(itemIds.get(1))
        );

        Throwable conflict = exactlyOneOptimisticLockFailure(failures);
        int loser = failures.indexOf(conflict);

        assertEquals(versionBefore + 1, dbVersionOf(orderId),
                "exactly one of the two transactions may commit");
        assertEquals(1, countReservedItems(orderId),
                "the loser's reservation must not be silently merged in");
        assertEquals(OrderStatus.PENDING, statusOf(orderId));

        // Retry the loser from a fresh transaction: it reloads current state
        // and applies the reservation that was rolled back.
        orderService.markItemReserved(orderId, itemIds.get(loser));

        assertEquals(versionBefore + 2, dbVersionOf(orderId));
        assertEquals(2, countReservedItems(orderId));
        assertEquals(OrderStatus.CONFIRMED, statusOf(orderId));
    }

    // -- I: reserve versus cancel -------------------------------------------

    @Test
    void concurrentReserveAndCancelLatchExactlyOneTerminalState()
            throws Exception {
        UUID orderId = createOrderWithItems(1);
        UUID onlyItemId = itemIds(orderId).getFirst();
        long versionBefore = dbVersionOf(orderId);

        List<Throwable> failures = runConcurrently(
                orderId,
                order -> order.markItemReserved(onlyItemId),
                Order::cancel
        );

        Throwable conflict = exactlyOneOptimisticLockFailure(failures);
        int loser = failures.indexOf(conflict);

        // Exactly one transaction committed; which terminal state won is
        // legitimately determined by commit order (reserve => CONFIRMED,
        // cancel => CANCELLED) — the guarantee is that the decision was made
        // exactly once, not which way it went.
        assertEquals(versionBefore + 1, dbVersionOf(orderId));
        OrderStatus terminalStatus = statusOf(orderId);
        assertEquals(loser == 0 ? OrderStatus.CANCELLED : OrderStatus.CONFIRMED,
                terminalStatus);

        // Retrying the loser against fresh state must be a no-op: the
        // terminal state is latched and the stale transition is rejected by
        // the status guard, without another version increment.
        if (loser == 0) {
            orderService.markItemReserved(orderId, onlyItemId);
        } else {
            orderService.cancelOrder(orderId);
        }

        assertEquals(terminalStatus, statusOf(orderId),
                "terminal state must remain latched after the loser retries");
        assertEquals(versionBefore + 1, dbVersionOf(orderId),
                "a rejected stale transition must not bump the version");
    }

    // -- harness -------------------------------------------------------------

    /**
     * Runs two mutations against the same order in two genuinely separate
     * transactions on two threads. Each transaction loads the aggregate
     * through its own persistence context, then waits on a barrier, so both
     * are guaranteed to hold the same pre-mutation version before either
     * commits. Returns each thread's failure (or null), index-aligned with
     * the mutations.
     */
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
                failures.add(result.get(BARRIER_TIMEOUT_SECONDS,
                        TimeUnit.SECONDS));
            }
            return failures;
        } finally {
            executor.shutdownNow();
        }
    }

    private static void awaitBarrier(CyclicBarrier barrier) {
        try {
            barrier.await(BARRIER_TIMEOUT_SECONDS, TimeUnit.SECONDS);
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

    private UUID createOrderWithItems(int itemCount) {
        Order order = orderService.createOrder(UUID.randomUUID(), "USD");

        for (int i = 0; i < itemCount; i++) {
            orderService.addItem(
                    order.getId(),
                    UUID.randomUUID(),
                    1 + i,
                    new BigDecimal("10.00")
            );
        }

        orderService.submitOrder(order.getId());

        return order.getId();
    }

    private List<UUID> itemIds(UUID orderId) {
        return orderService.getOrder(orderId).getItems().stream()
                .map(OrderItem::getId)
                .toList();
    }

    private OrderStatus statusOf(UUID orderId) {
        return orderService.getOrder(orderId).getStatus();
    }

    private long dbVersionOf(UUID orderId) {
        Long version = jdbcTemplate.queryForObject(
                "SELECT version FROM orders WHERE id = ?",
                Long.class,
                orderId
        );
        assertNotNull(version);
        return version;
    }

    private OffsetDateTime dbUpdatedAtOf(UUID orderId) {
        return jdbcTemplate.queryForObject(
                "SELECT updated_at FROM orders WHERE id = ?",
                OffsetDateTime.class,
                orderId
        );
    }

    private int countReservedItems(UUID orderId) {
        Integer reserved = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM order_items"
                        + " WHERE order_id = ? AND reserved",
                Integer.class,
                orderId
        );
        assertNotNull(reserved);
        return reserved;
    }
}
