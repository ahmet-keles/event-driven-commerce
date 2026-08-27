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
        return post(
                "/api/orders/" + orderId + "/items",
                "{\"productId\":\"%s\",\"quantity\":%d,\"unitPrice\":%s}"
                        .formatted(productId, quantity, unitPrice),
                200);
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

    private JsonNode post(String path, String json, int expectedStatus) {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + path))
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .build();

        return send(request, expectedStatus);
    }

    private JsonNode send(HttpRequest request, int expectedStatus) {
        try {
            HttpResponse<String> response =
                    client.send(request, HttpResponse.BodyHandlers.ofString());

            assertEquals(
                    expectedStatus,
                    response.statusCode(),
                    () -> request.method() + " " + request.uri()
                            + " returned unexpected status; body: "
                            + response.body());

            return MAPPER.readTree(response.body());
        } catch (Exception exception) {
            throw new IllegalStateException(
                    "Request failed: " + request.method() + " " + request.uri(),
                    exception);
        }
    }
}
