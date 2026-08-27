package com.ahmetkeles.orderservice.outbox;

import com.ahmetkeles.orderservice.PostgreSQLIntegrationTest;
import com.ahmetkeles.orderservice.domain.Order;
import com.ahmetkeles.orderservice.domain.OrderStatus;
import com.ahmetkeles.orderservice.repository.OrderRepository;
import com.ahmetkeles.orderservice.service.OrderService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

/**
 * Proves that the ORDER_CANCELLED outbox write composes with the A5
 * optimistic lock: when a cancellation transaction loses the version race to
 * a concurrent reservation, its staged outbox row rolls back with the stale
 * order update. A surviving row would announce a cancellation that never
 * happened — inventory would release stock for an order that ends CONFIRMED.
 *
 * <p>The race is forced deterministically with latches, no sleeps: a spy on
 * {@link OrderService#cancelOrder} parks the cancellation transaction after
 * it has loaded the order and staged both the status change and the outbox
 * row, but before its transaction commits. While it is parked, a reservation
 * transaction — which loaded the same original order version, because the
 * cancellation never flushed — commits and confirms the order. Only then is
 * the cancellation released to commit, where Hibernate's version check must
 * reject it.
 *
 * <p>This is a different property from
 * {@code cancellationRollsBackWithItsOutboxEventWhenOutboxWriteFails}, which
 * proves the reverse direction: a failed outbox write rolls back the status
 * change.
 */
class OrderCancellationOptimisticLockRollbackIntegrationTest
        extends PostgreSQLIntegrationTest {

    private static final int TIMEOUT_SECONDS = 30;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @MockitoSpyBean
    private OrderService orderService;

    @BeforeEach
    void clearDatabase() {
        outboxEventRepository.deleteAll();
        orderRepository.deleteAll();
    }

    @Test
    void losingCancellationRollsBackItsOutboxEventWithTheStaleUpdate()
            throws Exception {
        Order order = orderService.createOrder(UUID.randomUUID(), "USD");
        UUID orderId = order.getId();
        UUID itemId = orderService
                .addItem(orderId, UUID.randomUUID(), 1, new BigDecimal("10.00"))
                .getItems().getFirst().getId();

        // Only the cancellation's outbox row is under test.
        outboxEventRepository.deleteAll();

        CountDownLatch cancellationStaged = new CountDownLatch(1);
        CountDownLatch reservationCommitted = new CountDownLatch(1);

        doAnswer(invocation -> {
            Object result = invocation.callRealMethod();
            // The cancellation has loaded the order and staged CANCELLED plus
            // its ORDER_CANCELLED outbox row; nothing is flushed yet, so the
            // reservation below reads the same original version.
            cancellationStaged.countDown();
            assertTrue(reservationCommitted.await(
                            TIMEOUT_SECONDS, TimeUnit.SECONDS),
                    "reservation transaction never committed");
            return result;
        }).when(orderService).cancelOrder(any());

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Throwable> cancellationOutcome = executor.submit(() -> {
                try {
                    orderService.cancelOrder(orderId);
                    return null;
                } catch (Throwable throwable) {
                    return throwable;
                }
            });

            assertTrue(cancellationStaged.await(
                            TIMEOUT_SECONDS, TimeUnit.SECONDS),
                    "cancellation transaction never staged its mutation");

            // Same original order version, separate persistence context;
            // commits first and confirms the single-item order.
            orderService.markItemReserved(orderId, itemId);
            reservationCommitted.countDown();

            Throwable failure = cancellationOutcome.get(
                    TIMEOUT_SECONDS, TimeUnit.SECONDS);

            assertNotNull(failure,
                    "the cancellation must fail its version check, not "
                            + "silently overwrite the confirmed order");
            assertInstanceOf(OptimisticLockingFailureException.class, failure,
                    "the conflict must surface as a Spring optimistic-lock "
                            + "failure so the caller's error handling treats "
                            + "it as retryable, got: " + failure);
        } finally {
            executor.shutdownNow();
        }

        assertEquals(OrderStatus.CONFIRMED,
                orderRepository.findWithItemsById(orderId)
                        .orElseThrow().getStatus(),
                "the committed reservation must win");
        assertEquals(0, outboxEventRepository.count(),
                "the losing cancellation's ORDER_CANCELLED outbox row must "
                        + "roll back with its stale update");
    }
}
