package com.ahmetkeles.e2e;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.UUID;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Order-side payment saga, end to end. The payment service itself is not part
 * of this repository yet, so its outcomes are injected onto the payment topic
 * from the documented golden fixtures — exactly what a real payment service
 * will publish. UUID-correlated and sleep-free like the rest of the suite.
 */
@Timeout(180)
class PaymentSagaE2eTest {

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
    void completedPaymentSettlesTheConfirmedOrder() {
        UUID orderId = confirmedOrder(UUID.randomUUID());

        UUID eventId = UUID.randomUUID();
        topics.produce(
                E2eStack.PAYMENT_TOPIC,
                orderId.toString(),
                Fixtures.write(Fixtures.envelope(
                        Fixtures.PAYMENT_COMPLETED, eventId, orderId,
                        payload -> {
                        })));

        await().atMost(WAIT).untilAsserted(() -> assertEquals("COMPLETED",
                orderDb.orderPaymentStatus(orderId).orElse(null)));

        assertEquals("CONFIRMED", api.orderStatus(orderId),
                "a paid order stays confirmed");
        assertTrue(orderDb.processedEventExists(eventId));
    }

    @Test
    void failedPaymentCancelsTheOrderAndReleasesItsInventory() {
        UUID productId = UUID.randomUUID();
        UUID orderId = confirmedOrderForProduct(productId);

        Db.InventoryRow held = inventoryDb.inventoryRow(productId).orElseThrow();
        assertEquals(7, held.available());
        assertEquals(3, held.reserved());

        topics.produce(
                E2eStack.PAYMENT_TOPIC,
                orderId.toString(),
                Fixtures.write(Fixtures.envelope(
                        Fixtures.PAYMENT_FAILED, UUID.randomUUID(), orderId,
                        payload -> {
                        })));

        await().atMost(WAIT).untilAsserted(() ->
                assertEquals("CANCELLED", api.orderStatus(orderId),
                        "a failed payment must cancel the confirmed order"));
        assertEquals("FAILED",
                orderDb.orderPaymentStatus(orderId).orElseThrow());

        // The payment-failure cancellation feeds the existing compensation:
        // ORDER_CANCELLED flows to inventory, which releases the held stock.
        await().atMost(WAIT).untilAsserted(() -> {
            Db.InventoryRow restored =
                    inventoryDb.inventoryRow(productId).orElseThrow();
            assertEquals(10, restored.available(),
                    "inventory must be released after the payment failure");
            assertEquals(0, restored.reserved());
        });
        assertEquals("CANCELLED",
                inventoryDb.orderInventoryState(orderId).orElseThrow());
    }

    private UUID confirmedOrder(UUID customerId) {
        return confirm(customerId, UUID.randomUUID());
    }

    private UUID confirmedOrderForProduct(UUID productId) {
        return confirm(UUID.randomUUID(), productId);
    }

    private UUID confirm(UUID customerId, UUID productId) {
        inventoryDb.seedInventory(productId, 10);

        UUID orderId = UUID.fromString(
                api.createOrder(customerId).get("id").asText());
        api.addItem(orderId, productId, 3, "12.50");
        api.submit(orderId);

        await().atMost(WAIT).untilAsserted(() ->
                assertEquals("CONFIRMED", api.orderStatus(orderId)));
        await().atMost(WAIT).untilAsserted(() -> assertEquals("PENDING",
                orderDb.orderPaymentStatus(orderId).orElse(null),
                "confirmation must open the payment leg"));

        return orderId;
    }
}
