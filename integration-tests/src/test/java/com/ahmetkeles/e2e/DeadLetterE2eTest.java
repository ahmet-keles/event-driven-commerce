package com.ahmetkeles.e2e;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The retry/DLT seam, black-box: a contract-violating record on a supported
 * event type must land on the source topic's {@code .DLT} — key and value
 * preserved, correlated back by the recoverer's original-topic header — with
 * zero business effect and without stalling the partition; a business failure
 * must never be dead-lettered, because it is an outcome, not an error.
 *
 * <p>The malformed shape used everywhere is an {@code orderItemId} of
 * {@code "not-a-uuid"} inside an otherwise-valid payload of a supported event
 * type: it fails payload deserialization deterministically, which the
 * consumers wrap in their non-retryable contract-violation exception, while
 * keeping real, assertable ids (product, order) in the rest of the payload to
 * prove nothing was touched.
 */
@Timeout(120)
class DeadLetterE2eTest {

    private static final Duration WAIT = Duration.ofSeconds(30);

    private static final String ORDER_DLT = E2eStack.ORDER_TOPIC + ".DLT";
    private static final String INVENTORY_DLT =
            E2eStack.INVENTORY_TOPIC + ".DLT";

    /** Header added by Spring Kafka's DeadLetterPublishingRecoverer. */
    private static final String ORIGINAL_TOPIC_HEADER =
            "kafka_dlt-original-topic";

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
    void malformedOrderEventIsDeadLetteredWithoutBusinessEffect() {
        UUID eventId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        UUID productId = UUID.randomUUID();
        inventoryDb.seedInventory(productId, 10);

        String malformed = Fixtures.write(Fixtures.envelope(
                Fixtures.ORDER_ITEM_ADDED, eventId, orderId, payload -> {
                    payload.put("orderItemId", "not-a-uuid");
                    payload.put("productId", productId.toString());
                }));

        topics.produce(E2eStack.ORDER_TOPIC, orderId.toString(), malformed);

        ConsumerRecord<String, String> deadLetter =
                topics.awaitRecordWithKey(ORDER_DLT, orderId.toString(), WAIT);

        // Exact correlation: key, value, and origin all point back at the
        // one record this test produced.
        assertEquals(orderId.toString(), deadLetter.key());
        JsonNode envelope = Fixtures.parseJson(deadLetter.value());
        assertEquals(eventId.toString(), envelope.get("eventId").asText(),
                "dead-lettered value must be the original envelope verbatim");
        assertEquals("ORDER_ITEM_ADDED", envelope.get("eventType").asText());
        assertEquals(E2eStack.ORDER_TOPIC, header(deadLetter,
                ORIGINAL_TOPIC_HEADER));

        // No business mutation from the poisoned record.
        Db.InventoryRow untouched =
                inventoryDb.inventoryRow(productId).orElseThrow();
        assertEquals(10, untouched.available());
        assertEquals(0, untouched.reserved());
        assertFalse(inventoryDb.processedEventExists(eventId),
                "a dead-lettered event must never reach the ledger");

        // The partition keeps moving: a valid reservation queued behind the
        // poisoned record is processed normally.
        UUID barrierEventId = UUID.randomUUID();
        UUID barrierItemId = UUID.randomUUID();
        topics.produce(
                E2eStack.ORDER_TOPIC,
                orderId.toString(),
                Fixtures.write(Fixtures.envelope(
                        Fixtures.ORDER_ITEM_ADDED,
                        barrierEventId,
                        orderId,
                        payload -> {
                            payload.put("orderItemId",
                                    barrierItemId.toString());
                            payload.put("productId", productId.toString());
                        })));

        await().atMost(WAIT).untilAsserted(() ->
                assertTrue(inventoryDb.processedEventExists(barrierEventId)));

        Db.InventoryRow afterBarrier =
                inventoryDb.inventoryRow(productId).orElseThrow();
        assertEquals(7, afterBarrier.available());
        assertEquals(3, afterBarrier.reserved());
        assertFalse(inventoryDb.processedEventExists(eventId));
    }

    @Test
    void malformedInventoryEventIsDeadLetteredWithoutBusinessEffect() {
        UUID eventId = UUID.randomUUID();

        // A real PENDING order is the mutation target the poisoned record
        // must not touch.
        UUID orderId = UUID.fromString(
                api.createOrder(UUID.randomUUID()).get("id").asText());
        Instant updatedAtBefore = orderDb.orderUpdatedAt(orderId).orElseThrow();

        String malformed = Fixtures.write(Fixtures.envelope(
                Fixtures.INVENTORY_RESERVED, eventId, orderId, payload -> {
                    payload.put("orderItemId", "not-a-uuid");
                    payload.put("productId", UUID.randomUUID().toString());
                }));

        topics.produce(E2eStack.INVENTORY_TOPIC, orderId.toString(), malformed);

        ConsumerRecord<String, String> deadLetter = topics.awaitRecordWithKey(
                INVENTORY_DLT, orderId.toString(), WAIT);

        assertEquals(orderId.toString(), deadLetter.key());
        JsonNode envelope = Fixtures.parseJson(deadLetter.value());
        assertEquals(eventId.toString(), envelope.get("eventId").asText());
        assertEquals("INVENTORY_RESERVED",
                envelope.get("eventType").asText());
        assertEquals(E2eStack.INVENTORY_TOPIC, header(deadLetter,
                ORIGINAL_TOPIC_HEADER));

        // Partition continues: a valid no-op reservation behind the poisoned
        // record still claims its ledger row.
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

        await().atMost(WAIT).untilAsserted(() ->
                assertTrue(orderDb.processedEventExists(barrierEventId)));

        // No business mutation: state, timestamp, and ledger all untouched
        // by the dead-lettered event.
        assertEquals("PENDING", api.orderStatus(orderId));
        assertEquals(updatedAtBefore,
                orderDb.orderUpdatedAt(orderId).orElseThrow());
        assertFalse(orderDb.processedEventExists(eventId));
    }

    @Test
    void businessReservationFailureIsNotDeadLettered() {
        UUID productId = UUID.randomUUID();
        inventoryDb.seedInventory(productId, 2);

        UUID orderId = UUID.fromString(
                api.createOrder(UUID.randomUUID()).get("id").asText());
        JsonNode withItem = api.addItem(orderId, productId, 3, "12.50");
        UUID orderItemId = UUID.fromString(
                withItem.get("items").get(0).get("id").asText());

        await().atMost(WAIT).untilAsserted(() ->
                assertEquals("CANCELLED", api.orderStatus(orderId)));

        // Sync every leg of the flow before the negative check: the
        // cancellation's ORDER_CANCELLED round-trip is complete once its
        // outbox eventId is in inventory's ledger.
        Db.OutboxRow cancelled = orderDb
                .outboxRow(orderId, "ORDER_CANCELLED")
                .orElseThrow();
        await().atMost(WAIT).untilAsserted(() ->
                assertTrue(inventoryDb.processedEventExists(cancelled.id())));

        // The business outcome is fully recorded...
        Db.InventoryRow row = inventoryDb.inventoryRow(productId).orElseThrow();
        assertEquals(2, row.available());
        assertEquals(0, row.reserved());
        assertNotNull(inventoryDb
                .outboxRow(orderId, "INVENTORY_RESERVATION_FAILED")
                .orElseThrow());
        assertFalse(orderDb.orderItemReserved(orderItemId).orElseThrow());

        // ...and nothing for this order was dead-lettered on either topic.
        assertEquals(0,
                topics.recordsWithKey(ORDER_DLT, orderId.toString()).size(),
                "business failure must not dead-letter on " + ORDER_DLT);
        assertEquals(0,
                topics.recordsWithKey(INVENTORY_DLT, orderId.toString()).size(),
                "business failure must not dead-letter on " + INVENTORY_DLT);
    }

    private static String header(
            ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        assertNotNull(header, "expected header " + name);
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
