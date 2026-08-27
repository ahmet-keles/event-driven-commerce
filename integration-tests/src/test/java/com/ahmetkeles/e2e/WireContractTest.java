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
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Pins the cross-service wire contract to the checked-in golden fixtures, in
 * both directions:
 *
 * <p><b>Capture:</b> every event type each service really publishes is compared
 * field-for-field against its fixture — identity fields (UUIDs, timestamps) by
 * presence and parseability, everything else by value. Renaming or dropping a
 * field in any of the duplicated event records turns this red.
 *
 * <p><b>Injection:</b> the fixtures are retargeted to test-scoped UUIDs and
 * produced onto the real topics, proving each consumer accepts the documented
 * shape: {@code ORDER_ITEM_ADDED} into inventory-service and
 * {@code INVENTORY_RESERVATION_FAILED} into order-service.
 * {@code INVENTORY_RESERVED} and {@code ORDER_CREATED} are capture-only:
 * the happy-path saga already proves the real reserved event is accepted (the
 * order could not reach CONFIRMED otherwise), and nothing consumes
 * ORDER_CREATED today; injecting a reserved event alongside the live flow
 * would race it.
 */
@Timeout(120)
class WireContractTest {

    private static final Duration WAIT = Duration.ofSeconds(30);

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
    void wireContractMatchesGoldenFixtures() {
        capturedOrderCreatedAndItemAddedMatchFixtures();
        capturedReservationFailedMatchesFixture();
        injectedOrderItemAddedFixtureIsAcceptedByInventory();
        injectedReservationFailedFixtureIsAcceptedByOrder();
    }

    private void capturedOrderCreatedAndItemAddedMatchFixtures() {
        UUID customerId = UUID.randomUUID();
        UUID productId = UUID.randomUUID();
        inventoryDb.seedInventory(productId, 10);

        UUID orderId = UUID.fromString(
                api.createOrder(customerId).get("id").asText());

        JsonNode created = topics.awaitEnvelope(
                E2eStack.ORDER_TOPIC,
                "ORDER_CREATED for " + orderId,
                Topics.envelopeFor(orderId, "ORDER_CREATED"),
                WAIT);

        assertSameShape(Fixtures.load(Fixtures.ORDER_CREATED), created);
        ObjectNode createdPayload = Fixtures.payloadOf(created);
        assertEquals(orderId.toString(), createdPayload.get("orderId").asText());
        assertEquals(customerId.toString(),
                createdPayload.get("customerId").asText());
        assertEquals("USD", createdPayload.get("currency").asText());
        assertEquals("PENDING", createdPayload.get("status").asText());

        // Quantity and price mirror the fixture's values so non-identity
        // fields compare exactly.
        JsonNode withItem = api.addItem(orderId, productId, 3, "12.5");
        UUID orderItemId = UUID.fromString(
                withItem.get("items").get(0).get("id").asText());

        JsonNode itemAdded = topics.awaitEnvelope(
                E2eStack.ORDER_TOPIC,
                "ORDER_ITEM_ADDED for " + orderId,
                Topics.envelopeFor(orderId, "ORDER_ITEM_ADDED"),
                WAIT);

        assertSameShape(Fixtures.load(Fixtures.ORDER_ITEM_ADDED), itemAdded);
        ObjectNode itemPayload = Fixtures.payloadOf(itemAdded);
        assertEquals(orderId.toString(), itemPayload.get("orderId").asText());
        assertEquals(orderItemId.toString(),
                itemPayload.get("orderItemId").asText());
        assertEquals(productId.toString(),
                itemPayload.get("productId").asText());
        assertEquals(3, itemPayload.get("quantity").asInt());
        assertDecimalEquals("12.5", itemPayload.get("unitPrice"));
        assertDecimalEquals("37.5", itemPayload.get("totalAmount"));

        JsonNode reserved = topics.awaitEnvelope(
                E2eStack.INVENTORY_TOPIC,
                "INVENTORY_RESERVED for " + orderId,
                Topics.envelopeFor(orderId, "INVENTORY_RESERVED"),
                WAIT);

        assertSameShape(Fixtures.load(Fixtures.INVENTORY_RESERVED), reserved);
        ObjectNode reservedPayload = Fixtures.payloadOf(reserved);
        assertEquals(orderItemId.toString(),
                reservedPayload.get("orderItemId").asText(),
                "orderItemId must round-trip through inventory unchanged");
        assertEquals(3, reservedPayload.get("quantity").asInt());
    }

    private void capturedReservationFailedMatchesFixture() {
        UUID productId = UUID.randomUUID();
        inventoryDb.seedInventory(productId, 2);

        UUID orderId = UUID.fromString(
                api.createOrder(UUID.randomUUID()).get("id").asText());
        JsonNode withItem = api.addItem(orderId, productId, 3, "12.5");
        UUID orderItemId = UUID.fromString(
                withItem.get("items").get(0).get("id").asText());

        JsonNode failed = topics.awaitEnvelope(
                E2eStack.INVENTORY_TOPIC,
                "INVENTORY_RESERVATION_FAILED for " + orderId,
                Topics.envelopeFor(orderId, "INVENTORY_RESERVATION_FAILED"),
                WAIT);

        ObjectNode fixture = Fixtures.load(Fixtures.INVENTORY_RESERVATION_FAILED);
        assertSameShape(fixture, failed);
        ObjectNode failedPayload = Fixtures.payloadOf(failed);
        assertEquals(orderItemId.toString(),
                failedPayload.get("orderItemId").asText());
        assertEquals(3, failedPayload.get("requestedQuantity").asInt());
        assertEquals(
                Fixtures.payloadOf(fixture).get("reason").asText(),
                failedPayload.get("reason").asText());
    }

    private void injectedOrderItemAddedFixtureIsAcceptedByInventory() {
        UUID eventId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        UUID orderItemId = UUID.randomUUID();
        UUID productId = UUID.randomUUID();
        inventoryDb.seedInventory(productId, 10);

        ObjectNode envelope = Fixtures.load(Fixtures.ORDER_ITEM_ADDED);
        retargetEnvelope(envelope, eventId, orderId);
        ObjectNode payload = Fixtures.payloadOf(envelope);
        payload.put("orderId", orderId.toString());
        payload.put("orderItemId", orderItemId.toString());
        payload.put("productId", productId.toString());
        Fixtures.setPayload(envelope, payload);

        topics.produce(
                E2eStack.ORDER_TOPIC,
                orderId.toString(),
                Fixtures.write(envelope));

        await().atMost(WAIT).untilAsserted(() -> {
            Db.InventoryRow row =
                    inventoryDb.inventoryRow(productId).orElseThrow();
            assertEquals(7, row.available(),
                    "inventory must reserve from the documented shape");
            assertEquals(3, row.reserved());
        });

        await().atMost(WAIT).untilAsserted(() ->
                assertEquals(true, inventoryDb.processedEventExists(eventId),
                        "the fixture's eventId must reach the ledger"));
    }

    private void injectedReservationFailedFixtureIsAcceptedByOrder() {
        UUID eventId = UUID.randomUUID();

        // A real PENDING order with no items: the live inventory flow never
        // sees it, so the injected failure is the only event that can move it.
        UUID orderId = UUID.fromString(
                api.createOrder(UUID.randomUUID()).get("id").asText());

        ObjectNode envelope =
                Fixtures.load(Fixtures.INVENTORY_RESERVATION_FAILED);
        retargetEnvelope(envelope, eventId, orderId);
        ObjectNode payload = Fixtures.payloadOf(envelope);
        payload.put("orderId", orderId.toString());
        payload.put("orderItemId", UUID.randomUUID().toString());
        payload.put("productId", UUID.randomUUID().toString());
        Fixtures.setPayload(envelope, payload);

        topics.produce(
                E2eStack.INVENTORY_TOPIC,
                orderId.toString(),
                Fixtures.write(envelope));

        await().atMost(WAIT).untilAsserted(() ->
                assertEquals("CANCELLED", api.orderStatus(orderId),
                        "order must cancel from the documented failure shape"));
    }

    private static void retargetEnvelope(
            ObjectNode envelope, UUID eventId, UUID aggregateId) {
        envelope.put("eventId", eventId.toString());
        envelope.put("aggregateId", aggregateId.toString());
        envelope.put("occurredAt", Instant.now().toString());
    }

    /**
     * Shape equality: identical field-name sets on the envelope and on the
     * nested payload, plus the type-discriminating values. Identity fields are
     * checked for parseability, not value.
     */
    private static void assertSameShape(JsonNode fixture, JsonNode live) {
        assertEquals(
                Fixtures.fieldNames(fixture),
                Fixtures.fieldNames(live),
                "envelope field names must match the golden fixture");
        assertEquals(
                Fixtures.fieldNames(Fixtures.payloadOf(fixture)),
                Fixtures.fieldNames(Fixtures.payloadOf(live)),
                "payload field names must match the golden fixture");

        assertEquals(fixture.get("aggregateType").asText(),
                live.get("aggregateType").asText());
        assertEquals(fixture.get("eventType").asText(),
                live.get("eventType").asText());

        assertDoesNotThrow(
                () -> UUID.fromString(live.get("eventId").asText()),
                "eventId must be a UUID");
        assertDoesNotThrow(
                () -> UUID.fromString(live.get("aggregateId").asText()),
                "aggregateId must be a UUID");
        assertDoesNotThrow(
                () -> Instant.parse(live.get("occurredAt").asText()),
                "occurredAt must be an ISO-8601 instant");
    }

    /** Scale-insensitive decimal comparison (12.5 == 12.50). */
    private static void assertDecimalEquals(String expected, JsonNode actual) {
        assertEquals(0,
                new BigDecimal(expected)
                        .compareTo(new BigDecimal(actual.asText())),
                "expected " + expected + " but was " + actual.asText());
    }
}
