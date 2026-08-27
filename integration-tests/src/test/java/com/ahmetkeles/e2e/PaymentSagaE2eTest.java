package com.ahmetkeles.e2e;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The payment saga against the REAL payment service: order confirmation emits
 * ORDER_CONFIRMED, payment-service charges through its simulated gateway, and
 * the outcome flows back to settle or cancel the order — with a failed
 * payment feeding the existing inventory compensation.
 *
 * <p>Both outcomes are driven deterministically through the gateway's
 * documented contract: amounts strictly below the decline threshold
 * (1000.00 by default) are approved, amounts at or above it are declined.
 * No outcomes are injected; every payment event asserted here was published
 * by the real service.
 *
 * <p>Duplicates reuse the suite's established patterns: exact redelivered
 * envelopes exercise the eventId ledgers, and a fresh-eventId copy of the
 * same event doubles as a durable, side-effect-free partition fence (the
 * consumers claim its eventId while the guarded mutation no-ops).
 */
@Timeout(180)
class PaymentSagaE2eTest {

    private static final Duration WAIT = Duration.ofSeconds(30);

    private static final String ORDER_CONFIRMED = "ORDER_CONFIRMED";
    private static final String ORDER_CANCELLED = "ORDER_CANCELLED";
    private static final String PAYMENT_COMPLETED = "PAYMENT_COMPLETED";
    private static final String PAYMENT_FAILED = "PAYMENT_FAILED";

    /** The gateway's documented decline contract (see SimulatedPaymentGateway). */
    private static final String DECLINE_REASON =
            "simulated decline: amount at or above threshold";

    private static E2eStack stack;
    private static OrderApi api;
    private static Db orderDb;
    private static Db inventoryDb;
    private static Db paymentDb;
    private static Topics topics;

    @BeforeAll
    static void startStack() {
        stack = E2eStack.get();
        api = new OrderApi(stack.orderApiBaseUrl());
        orderDb = new Db(stack.orderDb);
        inventoryDb = new Db(stack.inventoryDb);
        paymentDb = new Db(stack.paymentDb);
        topics = new Topics(stack.bootstrapServers());
    }

    @Test
    void confirmedOrderIsChargedAndSettled() {
        UUID productId = UUID.randomUUID();
        // 3 x 12.50 = 37.50, safely below the decline threshold: approved.
        UUID orderId = submittedOrder(productId, 10, 3, "12.50");

        await().atMost(WAIT).untilAsserted(() ->
                assertEquals("CONFIRMED", api.orderStatus(orderId)));
        await().atMost(WAIT).untilAsserted(() -> assertEquals("COMPLETED",
                orderDb.orderPaymentStatus(orderId).orElse(null)));

        assertEquals("CONFIRMED", api.orderStatus(orderId),
                "a paid order stays confirmed");

        // The real charge, recorded immutably in the payment service.
        Db.PaymentRow payment =
                paymentDb.paymentForOrder(orderId).orElseThrow();
        assertEquals("COMPLETED", payment.status());
        assertEquals(0,
                new BigDecimal("37.50").compareTo(payment.amount()));
        assertNull(payment.failureReason(),
                "a completed payment carries no failure reason");

        // Ledger chain across all three services: the ORDER_CONFIRMED outbox
        // id reached payment's ledger, and the PAYMENT_COMPLETED outbox id
        // reached order's ledger.
        Db.OutboxRow confirmed = orderDb
                .outboxRow(orderId, ORDER_CONFIRMED)
                .orElseThrow();
        assertTrue(paymentDb.processedEventExists(confirmed.id()),
                "payment ledger must record the consumed ORDER_CONFIRMED");

        Db.OutboxRow completed = paymentDb
                .outboxRow(orderId, PAYMENT_COMPLETED)
                .orElseThrow();
        assertTrue(orderDb.processedEventExists(completed.id()),
                "order ledger must record the consumed PAYMENT_COMPLETED");
    }

    @Test
    void declinedPaymentCancelsTheOrderAndReleasesItsInventory() {
        UUID productId = UUID.randomUUID();
        // 4 x 250.00 = 1000.00, at the threshold: declined.
        UUID orderId = submittedOrder(productId, 10, 4, "250.00");

        await().atMost(WAIT).untilAsserted(() ->
                assertEquals("CANCELLED", api.orderStatus(orderId),
                        "a declined payment must cancel the confirmed order"));
        assertEquals("FAILED",
                orderDb.orderPaymentStatus(orderId).orElseThrow());

        Db.PaymentRow payment =
                paymentDb.paymentForOrder(orderId).orElseThrow();
        assertEquals("FAILED", payment.status());
        assertEquals(DECLINE_REASON, payment.failureReason());

        // The payment-failure cancellation feeds the existing compensation:
        // ORDER_CANCELLED flows to inventory, which releases the held stock.
        await().atMost(WAIT).untilAsserted(() -> {
            Db.InventoryRow restored =
                    inventoryDb.inventoryRow(productId).orElseThrow();
            assertEquals(10, restored.available(),
                    "inventory must be released after the declined payment");
            assertEquals(0, restored.reserved());
        });
        assertEquals("CANCELLED",
                inventoryDb.orderInventoryState(orderId).orElseThrow());
    }

    @Test
    void duplicateOrderConfirmedChargesExactlyOnce() {
        UUID productId = UUID.randomUUID();
        UUID orderId = submittedOrder(productId, 10, 3, "12.50");

        await().atMost(WAIT).untilAsserted(() -> assertEquals("COMPLETED",
                orderDb.orderPaymentStatus(orderId).orElse(null)));

        // The real ORDER_CONFIRMED, exactly as order-service published it.
        JsonNode confirmed = topics.awaitEnvelope(
                E2eStack.ORDER_TOPIC,
                ORDER_CONFIRMED + " for " + orderId,
                Topics.envelopeFor(orderId, ORDER_CONFIRMED),
                WAIT);
        UUID confirmedEventId =
                UUID.fromString(confirmed.get("eventId").asText());

        // Layer 1: the exact envelope redelivered — the eventId ledger
        // absorbs it.
        String redelivery = Fixtures.write(confirmed);
        topics.produce(E2eStack.ORDER_TOPIC, orderId.toString(), redelivery);
        topics.produce(E2eStack.ORDER_TOPIC, orderId.toString(), redelivery);

        // Layer 2 and the fence in one: the same confirmation re-emitted
        // under a FRESH eventId. The service records the new eventId in its
        // ledger (durable fence) but must not charge the already-paid order.
        UUID freshEventId = UUID.randomUUID();
        ObjectNode reEmitted = (ObjectNode) confirmed.deepCopy();
        reEmitted.put("eventId", freshEventId.toString());
        reEmitted.put("occurredAt", Instant.now().toString());
        topics.produce(E2eStack.ORDER_TOPIC, orderId.toString(),
                Fixtures.write(reEmitted));

        await().atMost(WAIT).untilAsserted(() ->
                assertTrue(paymentDb.processedEventExists(freshEventId)));

        assertEquals(1, paymentDb.processedEventCount(confirmedEventId),
                "exactly one ledger row for the duplicated eventId");
        assertEquals(1, paymentDb.paymentCountForOrder(orderId),
                "duplicates must never create a second payment");

        long completedOutcomes = paymentDb.outboxRows(orderId).stream()
                .filter(row -> row.eventType().equals(PAYMENT_COMPLETED))
                .count();
        assertEquals(1, completedOutcomes,
                "exactly one PAYMENT_COMPLETED outcome for the order");

        assertEquals("COMPLETED",
                orderDb.orderPaymentStatus(orderId).orElseThrow());
    }

    @Test
    void duplicatePaymentFailedReleasesInventoryOnce() {
        UUID productId = UUID.randomUUID();
        UUID orderId = submittedOrder(productId, 10, 4, "250.00");

        await().atMost(WAIT).untilAsserted(() ->
                assertEquals("CANCELLED", api.orderStatus(orderId)));
        await().atMost(WAIT).untilAsserted(() -> assertEquals(10,
                inventoryDb.inventoryRow(productId).orElseThrow().available(),
                "sync point: the release completed once"));

        // The real PAYMENT_FAILED, exactly as payment-service published it.
        JsonNode failed = topics.awaitEnvelope(
                E2eStack.PAYMENT_TOPIC,
                PAYMENT_FAILED + " for " + orderId,
                Topics.envelopeFor(orderId, PAYMENT_FAILED),
                WAIT);
        UUID failedEventId =
                UUID.fromString(failed.get("eventId").asText());

        long cancellationsBefore = orderDb.outboxRows(orderId).stream()
                .filter(row -> row.eventType().equals(ORDER_CANCELLED))
                .count();
        assertEquals(1, cancellationsBefore,
                "the decline produced exactly one cancellation");
        Instant updatedAtSettled =
                orderDb.orderUpdatedAt(orderId).orElseThrow();

        // Redeliver the exact envelope twice, then fence with a fresh-eventId
        // copy: failPayment() is already terminal, so the fence claims its
        // ledger row and mutates nothing.
        String redelivery = Fixtures.write(failed);
        topics.produce(E2eStack.PAYMENT_TOPIC, orderId.toString(), redelivery);
        topics.produce(E2eStack.PAYMENT_TOPIC, orderId.toString(), redelivery);

        UUID fenceEventId = UUID.randomUUID();
        ObjectNode fence = (ObjectNode) failed.deepCopy();
        fence.put("eventId", fenceEventId.toString());
        fence.put("occurredAt", Instant.now().toString());
        topics.produce(E2eStack.PAYMENT_TOPIC, orderId.toString(),
                Fixtures.write(fence));

        await().atMost(WAIT).untilAsserted(() ->
                assertTrue(orderDb.processedEventExists(fenceEventId)));

        assertEquals(1, orderDb.processedEventCount(failedEventId),
                "exactly one ledger row for the duplicated PAYMENT_FAILED");

        long cancellationsAfter = orderDb.outboxRows(orderId).stream()
                .filter(row -> row.eventType().equals(ORDER_CANCELLED))
                .count();
        assertEquals(1, cancellationsAfter,
                "duplicates must not emit a second release trigger");

        Db.InventoryRow stock =
                inventoryDb.inventoryRow(productId).orElseThrow();
        assertEquals(10, stock.available(),
                "stock must not be released twice");
        assertEquals(0, stock.reserved());

        assertEquals("CANCELLED", api.orderStatus(orderId));
        assertEquals(updatedAtSettled,
                orderDb.orderUpdatedAt(orderId).orElseThrow(),
                "duplicates must not mutate the terminal order");
    }

    /** Creates, fills, and submits an order; reservation runs asynchronously. */
    private UUID submittedOrder(
            UUID productId, int stock, int quantity, String unitPrice) {
        inventoryDb.seedInventory(productId, stock);

        UUID orderId = UUID.fromString(
                api.createOrder(UUID.randomUUID()).get("id").asText());
        api.addItem(orderId, productId, quantity, unitPrice);
        api.submit(orderId);

        return orderId;
    }
}
