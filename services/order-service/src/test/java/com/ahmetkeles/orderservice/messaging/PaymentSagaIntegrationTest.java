package com.ahmetkeles.orderservice.messaging;

import com.ahmetkeles.orderservice.PostgreSQLIntegrationTest;
import com.ahmetkeles.orderservice.domain.Order;
import com.ahmetkeles.orderservice.domain.OrderStatus;
import com.ahmetkeles.orderservice.domain.PaymentStatus;
import com.ahmetkeles.orderservice.outbox.OutboxEvent;
import com.ahmetkeles.orderservice.outbox.OutboxEventRepository;
import com.ahmetkeles.orderservice.repository.OrderRepository;
import com.ahmetkeles.orderservice.service.OrderService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The order-side payment saga against real PostgreSQL: ORDER_CONFIRMED is
 * emitted exactly once by the final item reservation, payment outcomes are
 * claimed idempotently, and a failed payment cancels the confirmed order with
 * its ORDER_CANCELLED row in the same transaction.
 */
class PaymentSagaIntegrationTest extends PostgreSQLIntegrationTest {

    private static final String PAYMENT_COMPLETED = "PAYMENT_COMPLETED";
    private static final String PAYMENT_FAILED = "PAYMENT_FAILED";

    @Autowired
    private OrderService orderService;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private ProcessedEventRepository processedEventRepository;

    @Autowired
    private PaymentEventProcessor eventProcessor;

    @Autowired
    private ObjectMapper objectMapper;

    @BeforeEach
    void clearDatabase() {
        processedEventRepository.deleteAll();
        outboxEventRepository.deleteAll();
        orderRepository.deleteAll();
    }

    @Test
    void finalItemReservationOnSubmittedOrderEmitsOrderConfirmedExactlyOnce()
            throws Exception {
        Order order = orderService.createOrder(UUID.randomUUID(), "USD");
        UUID orderId = order.getId();
        UUID firstItem = addItem(orderId, "10.00");
        UUID secondItem = addItem(orderId, "20.00");
        orderService.submitOrder(orderId);
        outboxEventRepository.deleteAll();

        orderService.markItemReserved(orderId, firstItem);

        assertEquals(0, outboxEventRepository.count(),
                "a non-final reservation must not emit ORDER_CONFIRMED");

        orderService.markItemReserved(orderId, secondItem);

        List<OutboxEvent> events = outboxEventRepository.findAll();
        assertEquals(1, events.size());
        OutboxEvent event = events.getFirst();
        assertEquals("ORDER_CONFIRMED", event.getEventType());
        assertEquals(orderId, event.getAggregateId());

        JsonNode payload = objectMapper.readTree(event.getPayload());
        assertEquals(orderId.toString(), payload.get("orderId").asText());
        assertEquals(order.getCustomerId().toString(),
                payload.get("customerId").asText());
        assertEquals(0, new BigDecimal("30.00").compareTo(
                new BigDecimal(payload.get("totalAmount").asText())));
        assertEquals("USD", payload.get("currency").asText());
        assertEquals(4, payload.size(),
                "ORDER_CONFIRMED carries exactly the agreed contract fields");

        Order confirmed = orderRepository.findWithItemsById(orderId)
                .orElseThrow();
        assertEquals(OrderStatus.CONFIRMED, confirmed.getStatus());
        assertEquals(PaymentStatus.PENDING, confirmed.getPaymentStatus(),
                "confirmation must start the payment leg in the same "
                        + "transaction");

        // Redelivered or unknown reservation events emit nothing further.
        orderService.markItemReserved(orderId, secondItem);
        orderService.markItemReserved(orderId, UUID.randomUUID());

        assertEquals(1, outboxEventRepository.count());
    }

    @Test
    void submitOverFullyReservedItemsEmitsOrderConfirmedExactlyOnce() {
        Order order = orderService.createOrder(UUID.randomUUID(), "USD");
        UUID orderId = order.getId();
        UUID itemId = addItem(orderId, "30.00");
        orderService.markItemReserved(orderId, itemId);
        outboxEventRepository.deleteAll();

        assertEquals(0, outboxEventRepository.count(),
                "reserved but unsubmitted: nothing may be announced");

        orderService.submitOrder(orderId);

        assertEquals(1, outboxEventRepository.count());
        assertEquals("ORDER_CONFIRMED",
                outboxEventRepository.findAll().getFirst().getEventType(),
                "the confirming submit is the emitting call");
        assertEquals(PaymentStatus.PENDING,
                orderRepository.findWithItemsById(orderId)
                        .orElseThrow().getPaymentStatus());

        // A duplicate submit is a no-op and emits nothing further.
        orderService.submitOrder(orderId);

        assertEquals(1, outboxEventRepository.count());
    }

    @Test
    void reservationBeforeSubmissionNeitherConfirmsNorEmits() {
        Order order = orderService.createOrder(UUID.randomUUID(), "USD");
        UUID orderId = order.getId();
        UUID itemId = addItem(orderId, "30.00");
        outboxEventRepository.deleteAll();

        orderService.markItemReserved(orderId, itemId);

        Order loaded = orderRepository.findWithItemsById(orderId)
                .orElseThrow();
        assertEquals(OrderStatus.PENDING, loaded.getStatus(),
                "reservation completeness alone is not customer intent");
        assertEquals(PaymentStatus.NOT_STARTED, loaded.getPaymentStatus());
        assertEquals(0, outboxEventRepository.count());
    }

    @Test
    void paymentCompletedIsAppliedOnceAndDuplicateIsANoOp() {
        UUID orderId = confirmedOrder();
        PaymentEventEnvelope envelope = paymentEnvelope(
                UUID.randomUUID(), orderId, PAYMENT_COMPLETED);
        PaymentCompletedEvent event = new PaymentCompletedEvent(
                orderId, UUID.randomUUID(), new BigDecimal("30.00"), "USD");

        assertTrue(eventProcessor.processCompleted(envelope, event));
        assertFalse(eventProcessor.processCompleted(envelope, event),
                "the same eventId must not win a second claim");

        Order order = orderRepository.findWithItemsById(orderId).orElseThrow();
        assertEquals(PaymentStatus.COMPLETED, order.getPaymentStatus());
        assertEquals(OrderStatus.CONFIRMED, order.getStatus());
        assertEquals(1, processedEventRepository.count());
    }

    @Test
    void paymentFailureCancelsTheOrderAndEmitsOrderCancelledInSameTransaction() {
        UUID orderId = confirmedOrder();

        assertTrue(eventProcessor.processFailed(
                paymentEnvelope(UUID.randomUUID(), orderId, PAYMENT_FAILED),
                failedEvent(orderId)));

        Order order = orderRepository.findWithItemsById(orderId).orElseThrow();
        assertEquals(PaymentStatus.FAILED, order.getPaymentStatus());
        assertEquals(OrderStatus.CANCELLED, order.getStatus(),
                "a failed payment must cancel the confirmed order");
        assertEquals(1, cancelledOutboxRows(orderId),
                "exactly one ORDER_CANCELLED row, written with the "
                        + "cancellation");

        // Duplicate delivery (same eventId) and a replayed failure under a
        // fresh eventId: neither may cancel or announce again.
        PaymentEventEnvelope duplicate = paymentEnvelope(
                UUID.randomUUID(), orderId, PAYMENT_FAILED);
        assertTrue(eventProcessor.processFailed(duplicate, failedEvent(orderId)),
                "a fresh eventId wins its claim but must be a domain no-op");

        order = orderRepository.findWithItemsById(orderId).orElseThrow();
        assertEquals(PaymentStatus.FAILED, order.getPaymentStatus());
        assertEquals(1, cancelledOutboxRows(orderId),
                "no second ORDER_CANCELLED for a payment already failed");
    }

    @Test
    void firstTerminalOutcomeWinsAcrossOpposingEvents() {
        UUID orderId = confirmedOrder();

        assertTrue(eventProcessor.processCompleted(
                paymentEnvelope(UUID.randomUUID(), orderId, PAYMENT_COMPLETED),
                new PaymentCompletedEvent(orderId, UUID.randomUUID(),
                        new BigDecimal("30.00"), "USD")));

        assertTrue(eventProcessor.processFailed(
                paymentEnvelope(UUID.randomUUID(), orderId, PAYMENT_FAILED),
                failedEvent(orderId)),
                "the late failure claims its event but changes nothing");

        Order order = orderRepository.findWithItemsById(orderId).orElseThrow();
        assertEquals(PaymentStatus.COMPLETED, order.getPaymentStatus(),
                "COMPLETED must never become FAILED");
        assertEquals(OrderStatus.CONFIRMED, order.getStatus());
        assertEquals(0, cancelledOutboxRows(orderId));
    }

    private UUID confirmedOrder() {
        Order order = orderService.createOrder(UUID.randomUUID(), "USD");
        UUID itemId = addItem(order.getId(), "30.00");
        orderService.submitOrder(order.getId());
        orderService.markItemReserved(order.getId(), itemId);
        outboxEventRepository.deleteAll();
        return order.getId();
    }

    private UUID addItem(UUID orderId, String unitPrice) {
        return orderService
                .addItem(orderId, UUID.randomUUID(), 1,
                        new BigDecimal(unitPrice))
                .getItems().getLast().getId();
    }

    private long cancelledOutboxRows(UUID orderId) {
        return outboxEventRepository.findAll().stream()
                .filter(event -> event.getAggregateId().equals(orderId))
                .filter(event -> "ORDER_CANCELLED".equals(event.getEventType()))
                .count();
    }

    private static PaymentEventEnvelope paymentEnvelope(
            UUID eventId, UUID orderId, String eventType) {
        return new PaymentEventEnvelope(
                eventId, "Order", orderId, eventType, "{}", Instant.now());
    }

    private static PaymentFailedEvent failedEvent(UUID orderId) {
        return new PaymentFailedEvent(
                orderId, UUID.randomUUID(), new BigDecimal("30.00"), "USD",
                "CARD_DECLINED");
    }
}
