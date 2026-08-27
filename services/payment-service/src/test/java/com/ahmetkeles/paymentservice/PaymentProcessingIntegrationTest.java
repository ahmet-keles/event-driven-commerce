package com.ahmetkeles.paymentservice;

import com.ahmetkeles.paymentservice.outbox.OutboxEvent;
import com.ahmetkeles.paymentservice.outbox.OutboxEventRepository;
import com.ahmetkeles.paymentservice.payment.Payment;
import com.ahmetkeles.paymentservice.payment.PaymentRepository;
import com.ahmetkeles.paymentservice.payment.PaymentService;
import com.ahmetkeles.paymentservice.payment.PaymentStatus;
import com.ahmetkeles.paymentservice.payment.ProcessedEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Idempotency and immutability proof against a real PostgreSQL instance.
 * Amounts below 1000.00 are approved by the simulated gateway; amounts at or
 * above it are declined (see application.properties).
 */
class PaymentProcessingIntegrationTest extends PostgreSQLIntegrationTest {

    private static final BigDecimal APPROVED_AMOUNT = new BigDecimal("42.50");
    private static final BigDecimal DECLINED_AMOUNT = new BigDecimal("1000.00");

    @Autowired
    private PaymentService paymentService;

    @Autowired
    private PaymentRepository paymentRepository;

    @Autowired
    private ProcessedEventRepository processedEventRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void cleanDatabase() {
        outboxEventRepository.deleteAll();
        processedEventRepository.deleteAll();
        paymentRepository.deleteAll();
    }

    private void process(UUID eventId, UUID orderId, BigDecimal amount) {
        paymentService.processOrderConfirmed(
                eventId,
                "ORDER_CONFIRMED",
                orderId,
                UUID.randomUUID(),
                amount,
                "USD"
        );
    }

    @Test
    void approvedChargeWritesCompletedPaymentAndOutboxEvent() throws Exception {
        UUID eventId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();

        process(eventId, orderId, APPROVED_AMOUNT);

        Payment payment = paymentRepository.findByOrderId(orderId).orElseThrow();

        assertEquals(PaymentStatus.COMPLETED, payment.getStatus());
        assertNull(payment.getFailureReason());
        assertNotNull(payment.getGatewayReference());
        assertTrue(processedEventRepository.existsById(eventId));

        List<OutboxEvent> events = outboxEventRepository.findAll();
        assertEquals(1, events.size());

        OutboxEvent event = events.getFirst();
        assertEquals("PAYMENT_COMPLETED", event.getEventType());
        assertEquals(orderId, event.getAggregateId());
        assertNull(event.getPublishedAt());

        JsonNode payload = objectMapper.readTree(event.getPayload());
        assertEquals(orderId.toString(), payload.get("orderId").asText());
        assertEquals(payment.getId().toString(), payload.get("paymentId").asText());
        assertEquals("USD", payload.get("currency").asText());
        assertEquals(
                0,
                APPROVED_AMOUNT.compareTo(
                        new BigDecimal(payload.get("amount").asText())
                )
        );
    }

    @Test
    void declinedChargeWritesFailedPaymentAndFailureEvent() throws Exception {
        UUID eventId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();

        process(eventId, orderId, DECLINED_AMOUNT);

        Payment payment = paymentRepository.findByOrderId(orderId).orElseThrow();

        assertEquals(PaymentStatus.FAILED, payment.getStatus());
        assertNotNull(payment.getFailureReason());

        List<OutboxEvent> events = outboxEventRepository.findAll();
        assertEquals(1, events.size());
        assertEquals("PAYMENT_FAILED", events.getFirst().getEventType());

        JsonNode payload = objectMapper.readTree(events.getFirst().getPayload());
        assertEquals(payment.getFailureReason(), payload.get("reason").asText());
        assertEquals(payment.getId().toString(), payload.get("paymentId").asText());
    }

    @Test
    void duplicateEventIdIsANoOp() {
        UUID eventId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();

        process(eventId, orderId, APPROVED_AMOUNT);
        process(eventId, orderId, APPROVED_AMOUNT);

        assertEquals(1, paymentRepository.count());
        assertEquals(1, outboxEventRepository.count());
        assertEquals(1, processedEventRepository.count());
    }

    @Test
    void reEmittedOrderConfirmedWithNewEventIdNeverCreatesASecondPayment() {
        UUID orderId = UUID.randomUUID();
        UUID firstEventId = UUID.randomUUID();
        UUID reEmittedEventId = UUID.randomUUID();

        process(firstEventId, orderId, APPROVED_AMOUNT);

        Payment original = paymentRepository.findByOrderId(orderId).orElseThrow();

        process(reEmittedEventId, orderId, APPROVED_AMOUNT);

        // One payment, one emitted event; the duplicate's eventId is recorded
        // so its own redeliveries dedup cheaply as well.
        assertEquals(1, paymentRepository.count());
        assertEquals(1, outboxEventRepository.count());
        assertTrue(processedEventRepository.existsById(reEmittedEventId));

        Payment after = paymentRepository.findByOrderId(orderId).orElseThrow();
        assertEquals(original.getId(), after.getId());
        assertEquals(original.getStatus(), after.getStatus());
    }

    @Test
    void terminalOutcomeIsImmutableEvenAcrossConflictingReplays() {
        UUID orderId = UUID.randomUUID();

        // First delivery declines. A later (contract-violating) replay with an
        // approvable amount must not resurrect or rewrite the outcome.
        process(UUID.randomUUID(), orderId, DECLINED_AMOUNT);
        process(UUID.randomUUID(), orderId, APPROVED_AMOUNT);

        Payment payment = paymentRepository.findByOrderId(orderId).orElseThrow();
        assertEquals(PaymentStatus.FAILED, payment.getStatus());
        assertEquals(1, outboxEventRepository.count());
        assertEquals("PAYMENT_FAILED", outboxEventRepository.findAll().getFirst().getEventType());
    }

    @Test
    void databaseRejectsASecondPaymentRowForTheSameOrder() {
        UUID orderId = UUID.randomUUID();

        process(UUID.randomUUID(), orderId, APPROVED_AMOUNT);

        // The unique constraint is the backstop beneath the service-level
        // guard: even a code path that skipped the guard could not commit a
        // second payment for this order.
        assertThrows(
                DataIntegrityViolationException.class,
                () -> jdbcTemplate.update(
                        """
                        INSERT INTO payments (
                            id, order_id, customer_id, amount, currency,
                            status, failure_reason, gateway_reference, created_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, NULL, ?, now())
                        """,
                        UUID.randomUUID(),
                        orderId,
                        UUID.randomUUID(),
                        APPROVED_AMOUNT,
                        "USD",
                        "COMPLETED",
                        "sim-backstop"
                )
        );
    }
}
