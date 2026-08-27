package com.ahmetkeles.paymentservice;

import com.ahmetkeles.paymentservice.messaging.OrderConfirmedEvent;
import com.ahmetkeles.paymentservice.messaging.OrderEventEnvelope;
import com.ahmetkeles.paymentservice.payment.Payment;
import com.ahmetkeles.paymentservice.payment.PaymentRepository;
import com.ahmetkeles.paymentservice.payment.PaymentStatus;
import com.ahmetkeles.paymentservice.payment.ProcessedEventRepository;
import com.ahmetkeles.paymentservice.outbox.OutboxEventRepository;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * The full seam: an ORDER_CONFIRMED record on order.events becomes a payment
 * row and a payment event on payment.events, published through the outbox.
 */
@SpringBootTest
class KafkaPaymentIntegrationTest {

    private static final String ORDER_EVENTS_TOPIC = "order.events";
    private static final String PAYMENT_EVENTS_TOPIC = "payment.events";

    private static final PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:17-alpine");

    private static final KafkaContainer kafka =
            new KafkaContainer("apache/kafka:4.0.0");

    static {
        postgres.start();
        kafka.start();

        try (AdminClient admin = AdminClient.create(
                Map.of("bootstrap.servers", kafka.getBootstrapServers())
        )) {
            // payment.events is declared by the service itself; order.events
            // belongs to order-service, so the test provides it.
            admin.createTopics(
                    List.of(new NewTopic(ORDER_EVENTS_TOPIC, 3, (short) 1))
            ).all().get();
        } catch (Exception exception) {
            throw new ExceptionInInitializerError(exception);
        }
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);

        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add(
                "spring.kafka.consumer.group-id",
                () -> "payment-integration-test"
        );
        registry.add("app.outbox.publish-interval-ms", () -> "100");
    }

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private PaymentRepository paymentRepository;

    @Autowired
    private ProcessedEventRepository processedEventRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @BeforeEach
    void cleanDatabase() {
        outboxEventRepository.deleteAll();
        processedEventRepository.deleteAll();
        paymentRepository.deleteAll();
    }

    @Test
    void orderConfirmedProducesCompletedPaymentEvent() throws Exception {
        UUID orderId = UUID.randomUUID();

        sendOrderConfirmed(UUID.randomUUID(), orderId, new BigDecimal("42.50"));

        awaitTrue(
                () -> paymentRepository.findByOrderId(orderId).isPresent(),
                Duration.ofSeconds(20),
                "payment row was not created"
        );

        Payment payment = paymentRepository.findByOrderId(orderId).orElseThrow();
        assertEquals(PaymentStatus.COMPLETED, payment.getStatus());

        ConsumerRecord<String, String> record = awaitPaymentEvent(
                r -> orderId.toString().equals(r.key()),
                Duration.ofSeconds(20)
        );

        assertNotNull(record);

        JsonNode envelope = objectMapper.readTree(record.value());
        assertEquals("PAYMENT_COMPLETED", envelope.get("eventType").asText());
        assertEquals(orderId.toString(), envelope.get("aggregateId").asText());

        JsonNode payload = objectMapper.readTree(envelope.get("payload").asText());
        assertEquals(payment.getId().toString(), payload.get("paymentId").asText());
        assertEquals("USD", payload.get("currency").asText());
    }

    @Test
    void declinedAmountProducesFailedPaymentEvent() throws Exception {
        UUID orderId = UUID.randomUUID();

        sendOrderConfirmed(UUID.randomUUID(), orderId, new BigDecimal("1000.00"));

        ConsumerRecord<String, String> record = awaitPaymentEvent(
                r -> orderId.toString().equals(r.key()),
                Duration.ofSeconds(20)
        );

        JsonNode envelope = objectMapper.readTree(record.value());
        assertEquals("PAYMENT_FAILED", envelope.get("eventType").asText());

        JsonNode payload = objectMapper.readTree(envelope.get("payload").asText());
        assertNotNull(payload.get("reason").asText());
        assertEquals(orderId.toString(), payload.get("orderId").asText());

        Payment payment = paymentRepository.findByOrderId(orderId).orElseThrow();
        assertEquals(PaymentStatus.FAILED, payment.getStatus());
    }

    @Test
    void duplicateOrderConfirmedChargesOnlyOnce() throws Exception {
        UUID eventId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();

        sendOrderConfirmed(eventId, orderId, new BigDecimal("10.00"));

        awaitTrue(
                () -> processedEventRepository.existsById(eventId),
                Duration.ofSeconds(20),
                "first delivery was not processed"
        );

        // Same eventId again, and a re-emitted confirmation with a new one.
        sendOrderConfirmed(eventId, orderId, new BigDecimal("10.00"));
        UUID reEmitted = UUID.randomUUID();
        sendOrderConfirmed(reEmitted, orderId, new BigDecimal("10.00"));

        awaitTrue(
                () -> processedEventRepository.existsById(reEmitted),
                Duration.ofSeconds(20),
                "re-emitted confirmation was not recorded"
        );

        assertEquals(1, paymentRepository.count());
        assertEquals(1, outboxEventRepository.count());
    }

    private void sendOrderConfirmed(
            UUID eventId,
            UUID orderId,
            BigDecimal totalAmount
    ) throws Exception {
        String payload = objectMapper.writeValueAsString(
                new OrderConfirmedEvent(
                        orderId,
                        UUID.randomUUID(),
                        totalAmount,
                        "USD"
                )
        );

        String message = objectMapper.writeValueAsString(
                new OrderEventEnvelope(
                        eventId,
                        "Order",
                        orderId,
                        "ORDER_CONFIRMED",
                        payload,
                        Instant.now()
                )
        );

        kafkaTemplate.send(ORDER_EVENTS_TOPIC, orderId.toString(), message).get();
    }

    private static ConsumerRecord<String, String> awaitPaymentEvent(
            Predicate<ConsumerRecord<String, String>> matcher,
            Duration timeout
    ) {
        Properties properties = new Properties();
        properties.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafka.getBootstrapServers()
        );
        properties.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                "payment-events-verifier-" + UUID.randomUUID()
        );
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName()
        );
        properties.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName()
        );

        long deadline = System.currentTimeMillis() + timeout.toMillis();

        try (KafkaConsumer<String, String> consumer =
                     new KafkaConsumer<>(properties)) {
            consumer.subscribe(List.of(PAYMENT_EVENTS_TOPIC));

            while (System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, String> record
                        : consumer.poll(Duration.ofMillis(250))) {
                    if (matcher.test(record)) {
                        return record;
                    }
                }
            }
        }

        fail("Timed out waiting for a payment event");
        return null;
    }

    private static void awaitTrue(
            Supplier<Boolean> condition,
            Duration timeout,
            String failureMessage
    ) {
        long deadline = System.currentTimeMillis() + timeout.toMillis();

        while (System.currentTimeMillis() < deadline) {
            if (Boolean.TRUE.equals(condition.get())) {
                return;
            }

            try {
                Thread.sleep(100);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                fail("Interrupted while waiting: " + failureMessage);
            }
        }

        fail(failureMessage);
    }
}
