package com.ahmetkeles.e2e;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.InputStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Golden wire-contract fixtures: one canonical envelope per event type,
 * checked in as JSON. The field structure comes from the fixture file; tests
 * retarget only identity fields (ids, timestamps) before producing, so what
 * goes over the wire is the documented shape with test-scoped UUIDs.
 */
final class Fixtures {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    static final String ORDER_CREATED = "order-created.envelope.json";
    static final String ORDER_ITEM_ADDED = "order-item-added.envelope.json";
    static final String INVENTORY_RESERVED = "inventory-reserved.envelope.json";
    static final String INVENTORY_RESERVATION_FAILED =
            "inventory-reservation-failed.envelope.json";

    private Fixtures() {
    }

    static ObjectNode load(String name) {
        try (InputStream stream = Fixtures.class.getResourceAsStream(
                "/fixtures/" + name)) {
            if (stream == null) {
                throw new IllegalStateException("Missing fixture: " + name);
            }
            return (ObjectNode) MAPPER.readTree(stream);
        } catch (Exception exception) {
            throw new IllegalStateException(
                    "Failed to load fixture " + name, exception);
        }
    }

    /** The envelope's payload field is a JSON string; parse it to a tree. */
    static ObjectNode payloadOf(JsonNode envelope) {
        return parseJson(envelope.get("payload").asText());
    }

    static ObjectNode parseJson(String json) {
        try {
            return (ObjectNode) MAPPER.readTree(json);
        } catch (Exception exception) {
            throw new IllegalStateException(
                    "Not a JSON object: " + json, exception);
        }
    }

    /** Re-embeds an edited payload tree as the envelope's payload string. */
    static void setPayload(ObjectNode envelope, ObjectNode payload) {
        try {
            envelope.put("payload", MAPPER.writeValueAsString(payload));
        } catch (Exception exception) {
            throw new IllegalStateException(
                    "Failed to serialize payload", exception);
        }
    }

    /**
     * Builds a producible envelope from a fixture: identity fields
     * (eventId, aggregateId/orderId, occurredAt) are retargeted to the
     * caller's UUIDs, everything else keeps the fixture's documented shape
     * and values; {@code payloadEdits} adjusts the remaining payload ids.
     */
    static ObjectNode envelope(
            String fixtureName,
            java.util.UUID eventId,
            java.util.UUID orderId,
            java.util.function.Consumer<ObjectNode> payloadEdits
    ) {
        ObjectNode envelope = load(fixtureName);
        envelope.put("eventId", eventId.toString());
        envelope.put("aggregateId", orderId.toString());
        envelope.put("occurredAt", java.time.Instant.now().toString());

        ObjectNode payload = payloadOf(envelope);
        payload.put("orderId", orderId.toString());
        payloadEdits.accept(payload);
        setPayload(envelope, payload);

        return envelope;
    }

    static String write(JsonNode node) {
        try {
            return MAPPER.writeValueAsString(node);
        } catch (Exception exception) {
            throw new IllegalStateException(
                    "Failed to serialize envelope", exception);
        }
    }

    static Set<String> fieldNames(JsonNode node) {
        Set<String> names = new HashSet<>();
        Iterator<String> iterator = node.fieldNames();
        iterator.forEachRemaining(names::add);
        return names;
    }
}
