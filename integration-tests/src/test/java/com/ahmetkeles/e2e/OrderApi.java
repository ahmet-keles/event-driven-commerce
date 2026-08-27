package com.ahmetkeles.e2e;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Drives order-service through its public REST API only. */
final class OrderApi {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * POST /items races with the Kafka consumer's writes to the same order
     * row (markItemReserved / cancel). The service maps the lost
     * optimistic-lock race to 409 concurrent_modification, whose contract is
     * "retry the request" — so this client does, briefly. The terminal 409
     * order_not_modifiable is never retried.
     */
    private static final int MAX_CONFLICT_RETRIES = 50;
    private static final Duration CONFLICT_RETRY_PAUSE = Duration.ofMillis(100);

    private final HttpClient client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();

    private final String baseUrl;

    OrderApi(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    JsonNode createOrder(UUID customerId) {
        JsonNode body = post(
                "/api/orders",
                "{\"customerId\":\"%s\",\"currency\":\"USD\"}"
                        .formatted(customerId),
                201);

        assertEquals("PENDING", body.get("status").asText());
        return body;
    }

    JsonNode addItem(UUID orderId, UUID productId, int quantity, String unitPrice) {
        HttpRequest request = postRequest(
                "/api/orders/" + orderId + "/items",
                "{\"productId\":\"%s\",\"quantity\":%d,\"unitPrice\":%s}"
                        .formatted(productId, quantity, unitPrice));

        HttpResponse<String> response = exchange(request);

        for (int retry = 0;
             retry < MAX_CONFLICT_RETRIES && isRetryableConflict(response);
             retry++) {
            pause();
            response = exchange(request);
        }

        return assertStatusAndParse(request, response, 200);
    }

    JsonNode getOrder(UUID orderId) {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/api/orders/" + orderId))
                .timeout(Duration.ofSeconds(10))
                .GET()
                .build();

        return send(request, 200);
    }

    String orderStatus(UUID orderId) {
        return getOrder(orderId).get("status").asText();
    }

    private static boolean isRetryableConflict(HttpResponse<String> response) {
        if (response.statusCode() != 409) {
            return false;
        }

        try {
            JsonNode body = MAPPER.readTree(response.body());
            return body.hasNonNull("error")
                    && "concurrent_modification".equals(body.get("error").asText());
        } catch (Exception exception) {
            return false;
        }
    }

    private static void pause() {
        try {
            Thread.sleep(CONFLICT_RETRY_PAUSE.toMillis());
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "Interrupted while backing off a conflicted request",
                    exception);
        }
    }

    private HttpRequest postRequest(String path, String json) {
        return HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + path))
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .build();
    }

    private JsonNode post(String path, String json, int expectedStatus) {
        return send(postRequest(path, json), expectedStatus);
    }

    private JsonNode send(HttpRequest request, int expectedStatus) {
        return assertStatusAndParse(request, exchange(request), expectedStatus);
    }

    private HttpResponse<String> exchange(HttpRequest request) {
        try {
            return client.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (Exception exception) {
            throw new IllegalStateException(
                    "Request failed: " + request.method() + " " + request.uri(),
                    exception);
        }
    }

    private static JsonNode assertStatusAndParse(
            HttpRequest request,
            HttpResponse<String> response,
            int expectedStatus
    ) {
        assertEquals(
                expectedStatus,
                response.statusCode(),
                () -> request.method() + " " + request.uri()
                        + " returned unexpected status; body: "
                        + response.body());

        try {
            return MAPPER.readTree(response.body());
        } catch (Exception exception) {
            throw new IllegalStateException(
                    "Unparseable response body from "
                            + request.method() + " " + request.uri(),
                    exception);
        }
    }
}
