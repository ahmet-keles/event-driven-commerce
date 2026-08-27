package com.ahmetkeles.e2e;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.UUID;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cross-service compensation: stock reserved for an order that later cancels
 * must come back, exactly once, across both services and the broker.
 * UUID-correlated and sleep-free like the rest of the suite — every wait is
 * an awaitility poll or a Kafka consumer poll on the test's own identifiers.
 */
@Timeout(180)
class CompensationE2eTest {

    private static final Duration WAIT = Duration.ofSeconds(30);
    private static final String ORDER_CANCELLED = "ORDER_CANCELLED";

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

    private static UUID itemIdForProduct(JsonNode order, UUID productId) {
        for (JsonNode item : order.get("items")) {
            if (productId.toString().equals(item.get("productId").asText())) {
                return UUID.fromString(item.get("id").asText());
            }
        }
        throw new AssertionError(
                "order has no item for product " + productId);
    }

    @Test
    void cancellationReleasesEarlierReservationsAndAbsorbsDuplicates() {
        UUID productA = UUID.randomUUID();
        UUID productB = UUID.randomUUID();
        inventoryDb.seedInventory(productA, 10);
        inventoryDb.seedInventory(productB, 2);

        UUID orderId = UUID.fromString(
                api.createOrder(UUID.randomUUID()).get("id").asText());

        // Item A reserves successfully; wait until the reservation is durable
        // in the ledger before introducing the failing item, so this test
        // proves release of a committed reservation, not a lucky ordering.
        // The order is still being assembled (not yet submitted), so A's
        // fast reservation cannot prematurely confirm it.
        JsonNode withA = api.addItem(orderId, productA, 3, "12.50");
        UUID itemAId = UUID.fromString(
                withA.get("items").get(0).get("id").asText());

        await().atMost(WAIT).untilAsserted(() -> assertEquals("RESERVED",
                inventoryDb.reservationStatus(itemAId).orElse(null)));

        Db.InventoryRow held = inventoryDb.inventoryRow(productA).orElseThrow();
        assertEquals(7, held.available());
        assertEquals(3, held.reserved());

        // Item B cannot be satisfied: the failure cancels the order, and the
        // cancellation must release item A's stock.
        JsonNode withB = api.addItem(orderId, productB, 5, "8.00");
        UUID itemBId = itemIdForProduct(withB, productB);

        // All desired items are in place: finalize assembly. Until this
        // submit the order cannot confirm, however fast item A's
        // reservation lands — the failure outcome below then drives the
        // order to CANCELLED.
        api.submit(orderId);

        await().atMost(WAIT).untilAsserted(() ->
                assertEquals("CANCELLED", api.orderStatus(orderId)));

        await().atMost(WAIT).untilAsserted(() -> {
            Db.InventoryRow restored =
                    inventoryDb.inventoryRow(productA).orElseThrow();
            assertEquals(10, restored.available(),
                    "item A's stock must return to its original level");
            assertEquals(0, restored.reserved());
        });

        assertEquals("RELEASED",
                inventoryDb.reservationStatus(itemAId).orElseThrow(),
                "item A's ledger row must be RELEASED");
        assertTrue(inventoryDb.reservationStatus(itemBId).isEmpty(),
                "item B never reserved, so it must have no ledger row");
        assertEquals(1, inventoryDb.reservationCount(orderId));
        assertEquals("CANCELLED",
                inventoryDb.orderInventoryState(orderId).orElseThrow());

        Db.InventoryRow untouched =
                inventoryDb.inventoryRow(productB).orElseThrow();
        assertEquals(2, untouched.available());
        assertEquals(0, untouched.reserved());

        // Redeliver the same ORDER_CANCELLED (the at-least-once window a
        // publisher restart reopens): the release must not run twice.
        Db.OutboxRow cancelledRow = orderDb
                .outboxRow(orderId, ORDER_CANCELLED)
                .orElseThrow();

        await().atMost(WAIT).untilAsserted(() ->
                assertEquals(1,
                        inventoryDb.processedEventCount(cancelledRow.id()),
                        "the first delivery must be in the ledger before "
                                + "the duplicate is sent"));

        orderDb.markOutboxUnpublished(cancelledRow.id());

        await().atMost(WAIT).untilAsserted(() -> assertNotNull(
                orderDb.outboxRow(orderId, ORDER_CANCELLED)
                        .orElseThrow().publishedAt(),
                "the publisher must re-send the reopened row"));

        // Same-partition barrier: keyed by the same orderId, this envelope is
        // ordered after the republished duplicate, so once it is processed
        // the duplicate has been too.
        UUID barrierOrderId = UUID.randomUUID();
        UUID barrierEventId = UUID.randomUUID();
        topics.produce(
                E2eStack.ORDER_TOPIC,
                orderId.toString(),
                Fixtures.write(Fixtures.envelope(
                        Fixtures.ORDER_CANCELLED,
                        barrierEventId,
                        barrierOrderId,
                        payload -> {
                        })));

        await().atMost(WAIT).untilAsserted(() -> assertTrue(
                inventoryDb.processedEventExists(barrierEventId)));

        Db.InventoryRow afterDuplicate =
                inventoryDb.inventoryRow(productA).orElseThrow();
        assertEquals(10, afterDuplicate.available(),
                "a duplicate cancellation must not double-release");
        assertEquals(0, afterDuplicate.reserved());
        assertEquals(1,
                inventoryDb.processedEventCount(cancelledRow.id()),
                "exactly one processed row for the duplicated cancellation");
        assertEquals("RELEASED",
                inventoryDb.reservationStatus(itemAId).orElseThrow());
    }
}
