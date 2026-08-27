package com.ahmetkeles.orderservice.api;

import com.ahmetkeles.orderservice.PostgreSQLIntegrationTest;
import com.ahmetkeles.orderservice.domain.Order;
import com.ahmetkeles.orderservice.outbox.OutboxEventRepository;
import com.ahmetkeles.orderservice.repository.OrderRepository;
import com.ahmetkeles.orderservice.service.OrderService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import static org.hamcrest.Matchers.hasSize;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@AutoConfigureMockMvc
class OrderApiIntegrationTest extends PostgreSQLIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private OrderService orderService;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @BeforeEach
    void clearOrders() {
        orderRepository.deleteAll();
    }

    @Test
    void createsOrder() throws Exception {
        UUID customerId = UUID.randomUUID();

        mockMvc.perform(post("/api/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"customerId\":\"%s\",\"currency\":\"USD\"}".formatted(customerId)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.customerId").value(customerId.toString()))
                .andExpect(jsonPath("$.status").value("PENDING"))
                .andExpect(jsonPath("$.currency").value("USD"))
                .andExpect(jsonPath("$.totalAmount").value(0))
                .andExpect(jsonPath("$.items", hasSize(0)));
    }

    @Test
    void retrievesOrder() throws Exception {
        UUID customerId = UUID.randomUUID();
        UUID orderId = createOrder(customerId);

        mockMvc.perform(get("/api/orders/{orderId}", orderId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(orderId.toString()))
                .andExpect(jsonPath("$.customerId").value(customerId.toString()))
                .andExpect(jsonPath("$.items", hasSize(0)));
    }

    @Test
    void returnsNotFoundForUnknownOrder() throws Exception {
        mockMvc.perform(get("/api/orders/{orderId}", UUID.randomUUID()))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.error").value("not_found"));
    }

    @Test
    void addsItemToOrder() throws Exception {
        UUID orderId = createOrder(UUID.randomUUID());

        mockMvc.perform(post("/api/orders/{orderId}/items", orderId)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(itemRequest(UUID.randomUUID(), 2, "15.00")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.items", hasSize(1)))
                .andExpect(jsonPath("$.items[0].quantity").value(2))
                .andExpect(jsonPath("$.items[0].unitPrice").value(15.00))
                .andExpect(jsonPath("$.items[0].subtotal").value(30.00));
    }

    @Test
    void addingItemsUpdatesOrderTotal() throws Exception {
        UUID orderId = createOrder(UUID.randomUUID());

        mockMvc.perform(post("/api/orders/{orderId}/items", orderId)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(itemRequest(UUID.randomUUID(), 2, "15.00")))
                .andExpect(status().isOk());

        mockMvc.perform(post("/api/orders/{orderId}/items", orderId)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(itemRequest(UUID.randomUUID(), 3, "4.50")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalAmount").value(43.50))
                .andExpect(jsonPath("$.items", hasSize(2)));
    }

    @Test
    void rejectsInvalidCreateRequest() throws Exception {
        mockMvc.perform(post("/api/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"currency\":\" \"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("validation_error"));
    }

    @Test
    void rejectsInvalidItemRequest() throws Exception {
        UUID orderId = createOrder(UUID.randomUUID());

        mockMvc.perform(post("/api/orders/{orderId}/items", orderId)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"quantity\":0,\"unitPrice\":-1}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("validation_error"));
    }

    @Test
    void returnsNotFoundWhenAddingItemToUnknownOrder() throws Exception {
        mockMvc.perform(post("/api/orders/{orderId}/items", UUID.randomUUID())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(itemRequest(UUID.randomUUID(), 1, "1.00")))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.error").value("not_found"));
    }

    @Test
    void rejectsAddingItemToCancelledOrder() throws Exception {
        UUID orderId = createOrder(UUID.randomUUID());

        mockMvc.perform(post("/api/orders/{orderId}/items", orderId)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(itemRequest(UUID.randomUUID(), 2, "15.00")))
                .andExpect(status().isOk());

        orderService.cancelOrder(orderId);

        Order before = orderRepository.findWithItemsById(orderId).orElseThrow();
        Long versionBefore = before.getVersion();
        Instant updatedAtBefore = before.getUpdatedAt();
        BigDecimal totalBefore = before.getTotalAmount();
        long itemAddedEventsBefore = itemAddedEventCount(orderId);

        mockMvc.perform(post("/api/orders/{orderId}/items", orderId)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(itemRequest(UUID.randomUUID(), 1, "5.00")))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.error").value("order_not_modifiable"));

        Order after = orderRepository.findWithItemsById(orderId).orElseThrow();
        assertEquals("CANCELLED", after.getStatus().name());
        assertEquals(versionBefore, after.getVersion());
        assertEquals(updatedAtBefore, after.getUpdatedAt());
        assertEquals(0, totalBefore.compareTo(after.getTotalAmount()));
        assertEquals(1, after.getItems().size());
        assertEquals(itemAddedEventsBefore, itemAddedEventCount(orderId));
    }

    @Test
    void rejectsAddingItemToConfirmedOrder() throws Exception {
        UUID orderId = createOrder(UUID.randomUUID());

        String response = mockMvc.perform(post("/api/orders/{orderId}/items", orderId)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(itemRequest(UUID.randomUUID(), 2, "15.00")))
                .andExpect(status().isOk())
                .andReturn()
                .getResponse()
                .getContentAsString();

        UUID itemId = UUID.fromString(
                objectMapper.readTree(response)
                        .get("items").get(0).get("id").asText());

        mockMvc.perform(post("/api/orders/{orderId}/submit", orderId))
                .andExpect(status().isOk());
        orderService.markItemReserved(orderId, itemId);

        Order before = orderRepository.findWithItemsById(orderId).orElseThrow();
        assertEquals("CONFIRMED", before.getStatus().name());
        Long versionBefore = before.getVersion();
        Instant updatedAtBefore = before.getUpdatedAt();
        BigDecimal totalBefore = before.getTotalAmount();
        long itemAddedEventsBefore = itemAddedEventCount(orderId);

        mockMvc.perform(post("/api/orders/{orderId}/items", orderId)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(itemRequest(UUID.randomUUID(), 1, "5.00")))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.error").value("order_not_modifiable"));

        Order after = orderRepository.findWithItemsById(orderId).orElseThrow();
        assertEquals("CONFIRMED", after.getStatus().name());
        assertEquals(versionBefore, after.getVersion());
        assertEquals(updatedAtBefore, after.getUpdatedAt());
        assertEquals(0, totalBefore.compareTo(after.getTotalAmount()));
        assertEquals(1, after.getItems().size());
        assertEquals(itemAddedEventsBefore, itemAddedEventCount(orderId));
    }

    @Test
    void submitsOrderAndPersistsSubmission() throws Exception {
        UUID orderId = createOrder(UUID.randomUUID());

        mockMvc.perform(post("/api/orders/{orderId}/items", orderId)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(itemRequest(UUID.randomUUID(), 2, "15.00")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.submitted").value(false));

        mockMvc.perform(post("/api/orders/{orderId}/submit", orderId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.submitted").value(true))
                .andExpect(jsonPath("$.status").value("PENDING"));

        Order persisted = orderRepository.findWithItemsById(orderId).orElseThrow();
        assertEquals(true, persisted.isSubmitted());

        mockMvc.perform(get("/api/orders/{orderId}", orderId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.submitted").value(true));
    }

    @Test
    void duplicateSubmitIsIdempotent() throws Exception {
        UUID orderId = createOrder(UUID.randomUUID());

        mockMvc.perform(post("/api/orders/{orderId}/items", orderId)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(itemRequest(UUID.randomUUID(), 1, "5.00")))
                .andExpect(status().isOk());

        mockMvc.perform(post("/api/orders/{orderId}/submit", orderId))
                .andExpect(status().isOk());

        Order afterFirst = orderRepository.findWithItemsById(orderId).orElseThrow();
        Long versionAfterFirst = afterFirst.getVersion();
        Instant updatedAtAfterFirst = afterFirst.getUpdatedAt();

        mockMvc.perform(post("/api/orders/{orderId}/submit", orderId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.submitted").value(true));

        Order afterSecond = orderRepository.findWithItemsById(orderId).orElseThrow();
        assertEquals(versionAfterFirst, afterSecond.getVersion(),
                "a duplicate submit must be a DB-visible no-op");
        assertEquals(updatedAtAfterFirst, afterSecond.getUpdatedAt());
    }

    @Test
    void rejectsSubmittingEmptyOrder() throws Exception {
        UUID orderId = createOrder(UUID.randomUUID());

        mockMvc.perform(post("/api/orders/{orderId}/submit", orderId))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.error").value("order_empty"));

        Order after = orderRepository.findWithItemsById(orderId).orElseThrow();
        assertEquals(false, after.isSubmitted());
    }

    @Test
    void returnsNotFoundWhenSubmittingUnknownOrder() throws Exception {
        mockMvc.perform(post("/api/orders/{orderId}/submit", UUID.randomUUID()))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.error").value("not_found"));
    }

    @Test
    void rejectsAddingItemToSubmittedOrder() throws Exception {
        UUID orderId = createOrder(UUID.randomUUID());

        mockMvc.perform(post("/api/orders/{orderId}/items", orderId)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(itemRequest(UUID.randomUUID(), 2, "15.00")))
                .andExpect(status().isOk());

        mockMvc.perform(post("/api/orders/{orderId}/submit", orderId))
                .andExpect(status().isOk());

        Order before = orderRepository.findWithItemsById(orderId).orElseThrow();
        Long versionBefore = before.getVersion();
        Instant updatedAtBefore = before.getUpdatedAt();
        BigDecimal totalBefore = before.getTotalAmount();
        long itemAddedEventsBefore = itemAddedEventCount(orderId);

        mockMvc.perform(post("/api/orders/{orderId}/items", orderId)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(itemRequest(UUID.randomUUID(), 1, "5.00")))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.error").value("order_not_modifiable"));

        Order after = orderRepository.findWithItemsById(orderId).orElseThrow();
        assertEquals("PENDING", after.getStatus().name());
        assertEquals(true, after.isSubmitted());
        assertEquals(versionBefore, after.getVersion());
        assertEquals(updatedAtBefore, after.getUpdatedAt());
        assertEquals(0, totalBefore.compareTo(after.getTotalAmount()));
        assertEquals(1, after.getItems().size());
        assertEquals(itemAddedEventsBefore, itemAddedEventCount(orderId));
    }

    private long itemAddedEventCount(UUID orderId) {
        return outboxEventRepository.findAll().stream()
                .filter(event -> orderId.equals(event.getAggregateId()))
                .filter(event -> "ORDER_ITEM_ADDED".equals(event.getEventType()))
                .count();
    }

    private UUID createOrder(UUID customerId) throws Exception {
        String response = mockMvc.perform(post("/api/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"customerId\":\"%s\",\"currency\":\"USD\"}".formatted(customerId)))
                .andExpect(status().isCreated())
                .andReturn()
                .getResponse()
                .getContentAsString();
        JsonNode body = objectMapper.readTree(response);
        return UUID.fromString(body.get("id").asText());
    }

    private String itemRequest(UUID productId, int quantity, String unitPrice) {
        return "{\"productId\":\"%s\",\"quantity\":%d,\"unitPrice\":%s}"
                .formatted(productId, quantity, unitPrice);
    }
}
