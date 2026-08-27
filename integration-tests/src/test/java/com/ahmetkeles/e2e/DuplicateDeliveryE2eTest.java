package com.ahmetkeles.e2e;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * At-least-once delivery is the system's declared contract; these tests prove
 * the consumers' processed-event ledgers turn it into exactly-once effects.
 * Every duplicate is delivered with the exact same envelope and eventId, and
 * every assertion checks both business state and ledger state, correlated by
 * that eventId — never by global counts.
 *
 * <h2>Sequencing without sleeps</h2>
 * All events for one order share a partition (the record key is the orderId),
 * so a later event on the same key is consumed only after every earlier one.
 * Two barrier shapes exploit that:
 * <ul>
 *   <li>inventory-side: a second real reservation for a different product —
 *       its ledger row proves the duplicate before it was consumed;</li>
 *   <li>order-side: a valid {@code INVENTORY_RESERVED} for a random, unknown
 *       orderItemId — the claim-then-mutate transaction commits the ledger
 *       claim while the mutation no-ops on the unknown item, leaving a
 *       durable, side-effect-free marker.</li>
 * </ul>
 *
 * <p>Deliberately version-agnostic: no assertions on any optimistic-lock
 * version column, only on status, quantities, {@code updated_at} stability,
 * and ledger rows — valid before and after the locking workstream merges.
 */
@Timeout(120)
class DuplicateDeliveryE2eTest {

    private static final Duration WAIT = Duration.ofSeconds(30);

    private static final String INVENTORY_RESERVED = "INVENTORY_RESERVED";

    private static E2eStack stack;
    private static OrderApi api;
    private static Db orderDb;
    private static Db inventoryDb;
    private static Topics topics;

    @BeforeAll
    static void startStack() {
        stack = E2eStack.get();
        api = new OrderApi(stack.orderApiBaseUrl());
        orderDb = new Db(stack.orderDb);
        inventoryDb = new Db(stack.inventoryDb);
        topics = new Topics(stack.bootstrapServers());
    }

    @Test
    void duplicateOrderItemAddedIsProcessedOnce() {
        UUID eventId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        UUID orderItemId = UUID.randomUUID();
        UUID productId = UUID.randomUUID();
        inventoryDb.seedInventory(productId, 10);

        String envelope = Fixtures.write(Fixtures.envelope(
                Fixtures.ORDER_ITEM_ADDED, eventId, orderId, payload -> {
                    payload.put("orderItemId", orderItemId.toString());
                    payload.put("productId", productId.toString());
                }));

        topics.produce(E2eStack.ORDER_TOPIC, orderId.toString(), envelope);

        await().atMost(WAIT).untilAsserted(() ->
                assertTrue(inventoryDb.processedEventExists(eventId)));

        Db.InventoryRow afterFirst =
                inventoryDb.inventoryRow(productId).orElseThrow();
        assertEquals(7, afterFirst.available());
        assertEquals(3, afterFirst.reserved());

        // Redeliver the exact same envelope, then a barrier reservation for a
        // different product on the same key: once the barrier is in the
        // ledger, the duplicate has been consumed, not merely queued.
        topics.produce(E2eStack.ORDER_TOPIC, orderId.toString(), envelope);

        UUID barrierEventId = UUID.randomUUID();
        UUID barrierProductId = UUID.randomUUID();
        inventoryDb.seedInventory(barrierProductId, 5);
        topics.produce(
                E2eStack.ORDER_TOPIC,
                orderId.toString(),
                Fixtures.write(Fixtures.envelope(
                        Fixtures.ORDER_ITEM_ADDED,
                        barrierEventId,
                        orderId,
                        payload -> {
                            payload.put("orderItemId",
                                    UUID.randomUUID().toString());
                            payload.put("productId",
                                    barrierProductId.toString());
                        })));

        await().atMost(WAIT).untilAsserted(() ->
                assertTrue(inventoryDb.processedEventExists(barrierEventId)));

        Db.InventoryRow afterDuplicate =
                inventoryDb.inventoryRow(productId).orElseThrow();
        assertEquals(7, afterDuplicate.available(),
                "duplicate must not decrement stock again");
        assertEquals(3, afterDuplicate.reserved());

        assertEquals(1, inventoryDb.processedEventCount(eventId),
                "exactly one ledger row for the duplicated eventId");

        long reservedOutcomes = inventoryDb.outboxRows(orderId).stream()
                .filter(row -> row.eventType().equals(INVENTORY_RESERVED))
                .filter(row -> orderItemId.toString().equals(
                        Fixtures.parseJson(row.payload())
                                .get("orderItemId").asText()))
                .count();
        assertEquals(1, reservedOutcomes,
                "exactly one durable INVENTORY_RESERVED outcome for the item");
    }

    @Test
    void duplicateInventoryReservedIsProcessedOnce() {
        UUID productId = UUID.randomUUID();
        inventoryDb.seedInventory(productId, 10);

        UUID orderId = UUID.fromString(
                api.createOrder(UUID.randomUUID()).get("id").asText());
        JsonNode withItem = api.addItem(orderId, productId, 3, "12.50");
        UUID orderItemId = UUID.fromString(
                withItem.get("items").get(0).get("id").asText());

        api.submit(orderId);

        await().atMost(WAIT).untilAsserted(() ->
                assertEquals("CONFIRMED", api.orderStatus(orderId)));

        // The real reserved event, exactly as inventory published it.
        JsonNode reserved = topics.awaitEnvelope(
                E2eStack.INVENTORY_TOPIC,
                INVENTORY_RESERVED + " for " + orderId,
                Topics.envelopeFor(orderId, INVENTORY_RESERVED),
                WAIT);
        UUID reservedEventId =
                UUID.fromString(reserved.get("eventId").asText());

        assertEquals(1, orderDb.processedEventCount(reservedEventId),
                "original delivery claims exactly one ledger row");

        // With the real payment service in the stack, confirmation opens an
        // async payment leg whose completion bumps updated_at. Snapshot only
        // after that leg settles, so the frozen-updated_at assertion below
        // stays deterministic.
        await().atMost(WAIT).untilAsserted(() -> assertEquals("COMPLETED",
                orderDb.orderPaymentStatus(orderId).orElse(null)));

        Instant updatedAtAfterConfirm =
                orderDb.orderUpdatedAt(orderId).orElseThrow();

        // Redeliver the same envelope twice, then fence with a no-op barrier.
        String redelivery = Fixtures.write(reserved);
        topics.produce(E2eStack.INVENTORY_TOPIC, orderId.toString(), redelivery);
        topics.produce(E2eStack.INVENTORY_TOPIC, orderId.toString(), redelivery);
        UUID barrierEventId = produceNoOpReservedBarrier(orderId);

        await().atMost(WAIT).untilAsserted(() ->
                assertTrue(orderDb.processedEventExists(barrierEventId)));

        assertEquals("CONFIRMED", api.orderStatus(orderId));
        assertTrue(orderDb.orderItemReserved(orderItemId).orElseThrow(),
                "item stays reserved exactly as after the first delivery");
        assertEquals(1, orderDb.processedEventCount(reservedEventId),
                "duplicates must not add ledger rows");
        assertEquals(updatedAtAfterConfirm,
                orderDb.orderUpdatedAt(orderId).orElseThrow(),
                "duplicate must not advance updated_at");
    }

    @Test
    void duplicateInventoryReservationFailedIsProcessedOnce() {
        UUID eventId = UUID.randomUUID();

        // A real PENDING order with no items: the live inventory flow never
        // touches it, so the injected failure is the only writer.
        UUID orderId = UUID.fromString(
                api.createOrder(UUID.randomUUID()).get("id").asText());

        String envelope = Fixtures.write(Fixtures.envelope(
                Fixtures.INVENTORY_RESERVATION_FAILED, eventId, orderId,
                payload -> {
                    payload.put("orderItemId", UUID.randomUUID().toString());
                    payload.put("productId", UUID.randomUUID().toString());
                }));

        topics.produce(E2eStack.INVENTORY_TOPIC, orderId.toString(), envelope);

        await().atMost(WAIT).untilAsserted(() ->
                assertEquals("CANCELLED", api.orderStatus(orderId)));

        assertEquals(1, orderDb.processedEventCount(eventId));
        Instant updatedAtAfterCancel =
                orderDb.orderUpdatedAt(orderId).orElseThrow();

        topics.produce(E2eStack.INVENTORY_TOPIC, orderId.toString(), envelope);
        UUID barrierEventId = produceNoOpReservedBarrier(orderId);

        await().atMost(WAIT).untilAsserted(() ->
                assertTrue(orderDb.processedEventExists(barrierEventId)));

        assertEquals("CANCELLED", api.orderStatus(orderId),
                "order stays terminal");
        assertEquals(1, orderDb.processedEventCount(eventId),
                "exactly one ledger row for the duplicated failure");
        assertEquals(updatedAtAfterCancel,
                orderDb.orderUpdatedAt(orderId).orElseThrow(),
                "duplicate must not mutate the terminal order");
    }

    @Test
    void republishedOutboxEventIsAbsorbedByConsumerLedger() {
        UUID productId = UUID.randomUUID();
        inventoryDb.seedInventory(productId, 10);

        UUID orderId = UUID.fromString(
                api.createOrder(UUID.randomUUID()).get("id").asText());
        JsonNode withItem = api.addItem(orderId, productId, 3, "12.50");
        UUID orderItemId = UUID.fromString(
                withItem.get("items").get(0).get("id").asText());

        api.submit(orderId);

        await().atMost(WAIT).untilAsserted(() ->
                assertEquals("CONFIRMED", api.orderStatus(orderId)));

        Db.OutboxRow reservedRow = inventoryDb
                .outboxRow(orderId, INVENTORY_RESERVED)
                .orElseThrow();
        await().atMost(WAIT).untilAsserted(() -> assertNotNull(
                inventoryDb.outboxRow(orderId, INVENTORY_RESERVED)
                        .orElseThrow().publishedAt()));

        assertEquals(1, orderDb.processedEventCount(reservedRow.id()));

        // With the real payment service in the stack, confirmation opens an
        // async payment leg whose completion bumps updated_at. Snapshot only
        // after that leg settles, so the frozen-updated_at assertion below
        // stays deterministic.
        await().atMost(WAIT).untilAsserted(() -> assertEquals("COMPLETED",
                orderDb.orderPaymentStatus(orderId).orElse(null)));

        Instant updatedAtBefore =
                orderDb.orderUpdatedAt(orderId).orElseThrow();

        // The accepted at-least-once window, made deterministic: clearing
        // published_at is exactly the state a send-timeout-but-broker-accepted
        // race leaves behind, so the publisher re-sends the SAME eventId.
        inventoryDb.markOutboxUnpublished(reservedRow.id());

        await().atMost(WAIT).untilAsserted(() -> assertNotNull(
                inventoryDb.outboxRow(orderId, INVENTORY_RESERVED)
                        .orElseThrow().publishedAt(),
                "publisher must re-send and re-stamp the reopened row"));

        // The republished record hit the partition before this barrier does.
        UUID barrierEventId = produceNoOpReservedBarrier(orderId);
        await().atMost(WAIT).untilAsserted(() ->
                assertTrue(orderDb.processedEventExists(barrierEventId)));

        assertEquals("CONFIRMED", api.orderStatus(orderId));
        assertTrue(orderDb.orderItemReserved(orderItemId).orElseThrow());
        assertEquals(1, orderDb.processedEventCount(reservedRow.id()),
                "consumer ledger absorbs the republished eventId");
        assertEquals(updatedAtBefore,
                orderDb.orderUpdatedAt(orderId).orElseThrow(),
                "republication must have no business effect");
    }

    /**
     * A durable, side-effect-free fence on the order's partition: a valid
     * {@code INVENTORY_RESERVED} for an orderItemId this order does not have.
     * The consumer's claim-then-mutate transaction commits the ledger claim
     * while {@code markItemReserved} no-ops on the unknown item, so the
     * barrier's ledger row proves every earlier record on the key was
     * consumed without itself touching the order.
     */
    private static UUID produceNoOpReservedBarrier(UUID orderId) {
        UUID barrierEventId = UUID.randomUUID();

        topics.produce(
                E2eStack.INVENTORY_TOPIC,
                orderId.toString(),
                Fixtures.write(Fixtures.envelope(
                        Fixtures.INVENTORY_RESERVED,
                        barrierEventId,
                        orderId,
                        payload -> {
                            payload.put("orderItemId",
                                    UUID.randomUUID().toString());
                            payload.put("productId",
                                    UUID.randomUUID().toString());
                        })));

        return barrierEventId;
    }
}
