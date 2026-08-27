package com.ahmetkeles.orderservice.outbox;

import com.ahmetkeles.orderservice.PostgreSQLIntegrationTest;
import com.ahmetkeles.orderservice.domain.Order;
import com.ahmetkeles.orderservice.repository.OrderRepository;
import com.ahmetkeles.orderservice.service.OrderService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OrderOutboxIntegrationTest extends PostgreSQLIntegrationTest {

    @Autowired
    private OrderService orderService;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void clearDatabase() {
        outboxEventRepository.deleteAll();
        orderRepository.deleteAll();
    }

    @Test
    void creatingOrderWritesOutboxEvent() throws Exception {
        UUID customerId = UUID.randomUUID();

        Order order = orderService.createOrder(customerId, "USD");

        List<OutboxEvent> events =
                outboxEventRepository.findTop100ByPublishedAtIsNullOrderByOccurredAtAsc();

        assertEquals(1, events.size());

        OutboxEvent event = events.getFirst();

        assertEquals("Order", event.getAggregateType());
        assertEquals(order.getId(), event.getAggregateId());
        assertEquals("ORDER_CREATED", event.getEventType());
        assertNull(event.getPublishedAt());

        JsonNode payload = objectMapper.readTree(event.getPayload());

        assertEquals(order.getId().toString(), payload.get("orderId").asText());
        assertEquals(customerId.toString(), payload.get("customerId").asText());
        assertEquals("USD", payload.get("currency").asText());
        assertEquals("PENDING", payload.get("status").asText());
    }

    @Test
    void addingItemWritesOutboxEvent() throws Exception {
        Order order = orderService.createOrder(UUID.randomUUID(), "USD");
        outboxEventRepository.deleteAll();

        UUID productId = UUID.randomUUID();

        orderService.addItem(
                order.getId(),
                productId,
                2,
                new BigDecimal("15.00")
        );

        List<OutboxEvent> events =
                outboxEventRepository.findTop100ByPublishedAtIsNullOrderByOccurredAtAsc();

        assertEquals(1, events.size());

        OutboxEvent event = events.getFirst();

        assertEquals("Order", event.getAggregateType());
        assertEquals(order.getId(), event.getAggregateId());
        assertEquals("ORDER_ITEM_ADDED", event.getEventType());
        assertNull(event.getPublishedAt());

        JsonNode payload = objectMapper.readTree(event.getPayload());

        assertEquals(order.getId().toString(), payload.get("orderId").asText());
        assertEquals(productId.toString(), payload.get("productId").asText());
        assertEquals(
                orderRepository.findWithItemsById(order.getId())
                        .orElseThrow()
                        .getItems()
                        .getFirst()
                        .getId()
                        .toString(),
                payload.get("orderItemId").asText()
        );
        assertEquals(2, payload.get("quantity").asInt());
        assertEquals("15.0", payload.get("unitPrice").asText());
        assertEquals("30.0", payload.get("totalAmount").asText());
    }

    @Test
    void cancellingOrderWritesSingleOutboxEventInSameTransaction() throws Exception {
        Order order = orderService.createOrder(UUID.randomUUID(), "USD");
        outboxEventRepository.deleteAll();

        orderService.cancelOrder(order.getId());

        List<OutboxEvent> events =
                outboxEventRepository.findTop100ByPublishedAtIsNullOrderByOccurredAtAsc();

        assertEquals(1, events.size());

        OutboxEvent event = events.getFirst();

        assertEquals("Order", event.getAggregateType());
        assertEquals(order.getId(), event.getAggregateId());
        assertEquals("ORDER_CANCELLED", event.getEventType());
        assertNull(event.getPublishedAt());

        JsonNode payload = objectMapper.readTree(event.getPayload());

        assertEquals(order.getId().toString(), payload.get("orderId").asText());
        assertEquals(1, payload.size(),
                "the cancellation payload carries only the order id — "
                        + "inventory releases from its own ledger");

        // Duplicate cancellations perform no transition and emit nothing.
        orderService.cancelOrder(order.getId());

        assertEquals(1, outboxEventRepository.count());
    }

    @Test
    void cancellingTerminalOrderEmitsNothing() {
        Order order = orderService.createOrder(UUID.randomUUID(), "USD");
        UUID itemId = orderService.addItem(
                        order.getId(), UUID.randomUUID(), 1,
                        new BigDecimal("10.00"))
                .getItems().getFirst().getId();
        orderService.markItemReserved(order.getId(), itemId);
        outboxEventRepository.deleteAll();

        orderService.cancelOrder(order.getId());

        assertEquals(0, outboxEventRepository.count(),
                "a CONFIRMED order cannot be cancelled, so no event may leak");
    }

    @Test
    void cancellationRollsBackWithItsOutboxEventWhenOutboxWriteFails() {
        Order order = orderService.createOrder(UUID.randomUUID(), "USD");
        outboxEventRepository.deleteAll();

        jdbcTemplate.execute(
                "ALTER TABLE outbox_events " +
                "ADD CONSTRAINT reject_outbox_events CHECK (false)"
        );

        try {
            assertThrows(
                    RuntimeException.class,
                    () -> orderService.cancelOrder(order.getId())
            );

            // The status change and the outbox row share one transaction, so
            // a failed outbox write must leave the order un-cancelled — the
            // same guarantee an optimistic-lock conflict at commit relies on.
            assertEquals("PENDING",
                    orderRepository.findWithItemsById(order.getId())
                            .orElseThrow().getStatus().name());
        } finally {
            jdbcTemplate.execute(
                    "ALTER TABLE outbox_events " +
                    "DROP CONSTRAINT IF EXISTS reject_outbox_events"
            );
        }
    }

    @Test
    void orderCreationRollsBackWhenOutboxWriteFails() {
        jdbcTemplate.execute(
                "ALTER TABLE outbox_events " +
                "ADD CONSTRAINT reject_outbox_events CHECK (false)"
        );

        try {
            assertThrows(
                    RuntimeException.class,
                    () -> orderService.createOrder(UUID.randomUUID(), "USD")
            );

            assertEquals(0, orderRepository.count());
            assertEquals(0, outboxEventRepository.count());
        } finally {
            jdbcTemplate.execute(
                    "ALTER TABLE outbox_events " +
                    "DROP CONSTRAINT IF EXISTS reject_outbox_events"
            );
        }
    }
}
