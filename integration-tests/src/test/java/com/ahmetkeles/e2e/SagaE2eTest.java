package com.ahmetkeles.e2e;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.UUID;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The saga across both real services, a real broker, and both real databases:
 * REST in, Kafka between, JDBC underneath. Every test mints its own UUIDs and
 * asserts only on rows and records carrying them — no cleanup, no counts.
 */
@Timeout(120)
class SagaE2eTest {

    private static final Duration WAIT = Duration.ofSeconds(30);

    private static E2eStack stack;
    private static OrderApi api;
    private static Db orderDb;
    private static Db inventoryDb;

    @BeforeAll
    static void startStack() {
        stack = E2eStack.get();
        api = new OrderApi(stack.orderApiBaseUrl());
        orderDb = new Db(stack.orderDb);
        inventoryDb = new Db(stack.inventoryDb);
    }

    @Test
    void singleItemSagaEndsConfirmed() {
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

        Db.InventoryRow row = inventoryDb.inventoryRow(productId).orElseThrow();
        assertEquals(7, row.available());
        assertEquals(3, row.reserved());

        assertTrue(orderDb.orderItemReserved(orderItemId).orElseThrow(),
                "order_items.reserved should be persisted as true");

        // The transactional-outbox chain, end to end: the ORDER_ITEM_ADDED
        // outbox row's id is the envelope eventId, which must appear in
        // inventory-service's processed-event ledger.
        Db.OutboxRow itemAdded = orderDb
                .outboxRow(orderId, "ORDER_ITEM_ADDED")
                .orElseThrow();
        assertTrue(inventoryDb.processedEventExists(itemAdded.id()),
                "inventory ledger should record the consumed event id");

        // Both services' outbox rows for this order end up marked published.
        await().atMost(WAIT).untilAsserted(() -> {
            var orderRows = orderDb.outboxRows(orderId);
            assertEquals(2, orderRows.size(),
                    "expected ORDER_CREATED + ORDER_ITEM_ADDED for " + orderId);
            orderRows.forEach(outboxRow -> assertNotNull(
                    outboxRow.publishedAt(),
                    outboxRow.eventType() + " should be marked published"));

            Db.OutboxRow reserved = inventoryDb
                    .outboxRow(orderId, "INVENTORY_RESERVED")
                    .orElseThrow();
            assertNotNull(reserved.publishedAt());
        });

        // The reservation event correlates to the exact item, not just the order.
        ObjectNode reservedPayload = outboxPayload(
                inventoryDb, orderId, "INVENTORY_RESERVED");
        assertEquals(orderItemId.toString(),
                reservedPayload.get("orderItemId").asText());
    }

    @Test
    void insufficientInventoryEndsCancelled() {
        UUID productId = UUID.randomUUID();
        inventoryDb.seedInventory(productId, 2);

        UUID orderId = UUID.fromString(
                api.createOrder(UUID.randomUUID()).get("id").asText());

        JsonNode withItem = api.addItem(orderId, productId, 3, "12.50");
        UUID orderItemId = UUID.fromString(
                withItem.get("items").get(0).get("id").asText());

        await().atMost(WAIT).untilAsserted(() ->
                assertEquals("CANCELLED", api.orderStatus(orderId)));

        // Stock untouched, item never reserved.
        Db.InventoryRow row = inventoryDb.inventoryRow(productId).orElseThrow();
        assertEquals(2, row.available());
        assertEquals(0, row.reserved());
        assertFalse(orderDb.orderItemReserved(orderItemId).orElseThrow());

        ObjectNode failedPayload = outboxPayload(
                inventoryDb, orderId, "INVENTORY_RESERVATION_FAILED");
        assertEquals("INSUFFICIENT_INVENTORY",
                failedPayload.get("reason").asText());
        assertEquals(orderItemId.toString(),
                failedPayload.get("orderItemId").asText());
    }

    @Test
    void unknownProductEndsCancelled() {
        UUID productId = UUID.randomUUID();
        // Deliberately not seeded: inventory has never heard of this product.

        UUID orderId = UUID.fromString(
                api.createOrder(UUID.randomUUID()).get("id").asText());
        api.addItem(orderId, productId, 3, "12.50");

        await().atMost(WAIT).untilAsserted(() ->
                assertEquals("CANCELLED", api.orderStatus(orderId)));

        assertTrue(inventoryDb.inventoryRow(productId).isEmpty(),
                "failure must not create an inventory row");

        ObjectNode failedPayload = outboxPayload(
                inventoryDb, orderId, "INVENTORY_RESERVATION_FAILED");
        assertEquals("INVENTORY_ITEM_NOT_FOUND",
                failedPayload.get("reason").asText());
    }

    /** Parses an outbox row's payload so assertions read JSON, not substrings. */
    private static ObjectNode outboxPayload(
            Db db, UUID aggregateId, String eventType) {
        Db.OutboxRow row = db.outboxRow(aggregateId, eventType).orElseThrow(
                () -> new AssertionError(
                        "No " + eventType + " outbox row for " + aggregateId));

        return Fixtures.parseJson(row.payload());
    }
}
