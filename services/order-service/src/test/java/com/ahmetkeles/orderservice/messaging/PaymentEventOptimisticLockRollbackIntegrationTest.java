package com.ahmetkeles.orderservice.messaging;

import com.ahmetkeles.orderservice.PostgreSQLIntegrationTest;
import com.ahmetkeles.orderservice.domain.Order;
import com.ahmetkeles.orderservice.domain.OrderStatus;
import com.ahmetkeles.orderservice.domain.PaymentStatus;
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
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

/**
 * Proves the payment saga composes with the A5 optimistic lock: a
 * PAYMENT_FAILED delivery that loses the version race to a concurrent
 * PAYMENT_COMPLETED rolls back everything it staged — its processed_events
 * claim, the FAILED payment status, the CONFIRMED -> CANCELLED transition,
 * and the ORDER_CANCELLED outbox row. A surviving claim would swallow the
 * event forever; a surviving outbox row would cancel (and release inventory
 * for) an order whose payment actually completed.
 *
 * <p>The race is latch-forced, sleep-free: a spy parks the failure
 * transaction between staging and commit, the completion commits first, and
 * the released failure must then fail its version check. The redelivery —
 * what the Kafka error handler schedules for a retryable failure — wins a
 * fresh claim and is a domain no-op, proving the first terminal outcome wins
 * even through a retry.
 */
class PaymentEventOptimisticLockRollbackIntegrationTest
        extends PostgreSQLIntegrationTest {

    private static final int TIMEOUT_SECONDS = 30;
    private static final String PAYMENT_FAILED = "PAYMENT_FAILED";

    @Autowired
    private PaymentEventProcessor eventProcessor;

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
    void losingPaymentFailureRollsBackClaimAndOutboxAndStaysNoOpOnRedelivery()
            throws Exception {
        Order order = orderService.createOrder(UUID.randomUUID(), "USD");
        UUID orderId = order.getId();
        UUID itemId = orderService
                .addItem(orderId, UUID.randomUUID(), 1, new BigDecimal("30.00"))
                .getItems().getFirst().getId();
        orderService.submitOrder(orderId);
        orderService.markItemReserved(orderId, itemId);
        outboxEventRepository.deleteAll();

        PaymentEventEnvelope failedEnvelope = new PaymentEventEnvelope(
                UUID.randomUUID(), "Order", orderId, PAYMENT_FAILED, "{}",
                Instant.now());
        PaymentFailedEvent failedEvent = new PaymentFailedEvent(
                orderId, UUID.randomUUID(), new BigDecimal("30.00"), "USD",
                "CARD_DECLINED");

        CountDownLatch failureStaged = new CountDownLatch(1);
        CountDownLatch completionCommitted = new CountDownLatch(1);

        doAnswer(invocation -> {
            Object result = invocation.callRealMethod();
            // The failure transaction now holds its processed_events claim,
            // the FAILED status, the CANCELLED transition, and the
            // ORDER_CANCELLED outbox row — all uncommitted and unflushed, so
            // the completion below reads the same original order version.
            failureStaged.countDown();
            assertTrue(completionCommitted.await(
                            TIMEOUT_SECONDS, TimeUnit.SECONDS),
                    "completion transaction never committed");
            return result;
        }).when(orderService).failPayment(any());

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Throwable> failureOutcome = executor.submit(() -> {
                try {
                    eventProcessor.processFailed(failedEnvelope, failedEvent);
                    return null;
                } catch (Throwable throwable) {
                    return throwable;
                }
            });

            assertTrue(failureStaged.await(TIMEOUT_SECONDS, TimeUnit.SECONDS),
                    "failure transaction never staged its mutation");

            assertTrue(eventProcessor.processCompleted(
                    new PaymentEventEnvelope(
                            UUID.randomUUID(), "Order", orderId,
                            "PAYMENT_COMPLETED", "{}", Instant.now()),
                    new PaymentCompletedEvent(
                            orderId, UUID.randomUUID(),
                            new BigDecimal("30.00"), "USD")));
            completionCommitted.countDown();

            Throwable failure = failureOutcome.get(
                    TIMEOUT_SECONDS, TimeUnit.SECONDS);

            assertNotNull(failure,
                    "the parked failure must lose the version race");
            assertInstanceOf(OptimisticLockingFailureException.class, failure,
                    "the conflict must surface as retryable, got: " + failure);
        } finally {
            executor.shutdownNow();
        }

        // Everything the loser staged must be gone.
        assertFalse(processedEventRepository
                        .existsById(failedEnvelope.eventId()),
                "the rolled-back claim must not survive — it would swallow "
                        + "the event permanently");
        assertEquals(0, outboxEventRepository.count(),
                "the rolled-back ORDER_CANCELLED row must not survive — it "
                        + "would cancel a paid order downstream");

        Order afterRace = orderRepository.findWithItemsById(orderId)
                .orElseThrow();
        assertEquals(PaymentStatus.COMPLETED, afterRace.getPaymentStatus());
        assertEquals(OrderStatus.CONFIRMED, afterRace.getStatus());

        // Redelivery: wins a fresh claim, and the first terminal outcome
        // holds — no cancellation, no outbox row, order still paid.
        assertTrue(eventProcessor.processFailed(failedEnvelope, failedEvent),
                "the redelivered event must claim again, not be treated as "
                        + "a duplicate");
        assertTrue(processedEventRepository
                .existsById(failedEnvelope.eventId()));

        Order afterRedelivery = orderRepository.findWithItemsById(orderId)
                .orElseThrow();
        assertEquals(PaymentStatus.COMPLETED,
                afterRedelivery.getPaymentStatus(),
                "COMPLETED must survive the replayed failure");
        assertEquals(OrderStatus.CONFIRMED, afterRedelivery.getStatus());
        assertEquals(0, outboxEventRepository.count());
    }
}
