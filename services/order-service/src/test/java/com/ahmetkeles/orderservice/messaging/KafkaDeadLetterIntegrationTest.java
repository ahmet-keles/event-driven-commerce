package com.ahmetkeles.orderservice.messaging;

import com.ahmetkeles.orderservice.OrderServiceApplication;
import com.ahmetkeles.orderservice.domain.Order;
import com.ahmetkeles.orderservice.domain.OrderItem;
import com.ahmetkeles.orderservice.domain.OrderStatus;
import com.ahmetkeles.orderservice.service.OrderService;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Verifies the explicit consumer failure policy: bounded retries with backoff,
 * dead-letter publication for exhausted records, and continued processing
 * afterwards. Modeled business outcomes (e.g. a failed reservation cancelling
 * an order) must never reach the dead-letter topic.
 */
@SpringBootTest(
        classes = {
                OrderServiceApplication.class,
                KafkaDeadLetterIntegrationTest.FlakyListenerConfig.class
        }
)
class KafkaDeadLetterIntegrationTest {

    private static final String INVENTORY_EVENTS_TOPIC = "inventory.events";
    private static final String INVENTORY_EVENTS_DLT = "inventory.events.DLT";
    private static final String RETRY_TEST_TOPIC = "dlt.retry.test";
    private static final String RETRY_TEST_DLT = "dlt.retry.test.DLT";

    private static final int CONFIGURED_ATTEMPTS = 3;

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
            // inventory.events.DLT is not created here: the service itself
            // declares it as a NewTopic bean and KafkaAdmin creates it on
            // context startup.
            admin.createTopics(
                    List.of(
                            new NewTopic(INVENTORY_EVENTS_TOPIC, 3, (short) 1),
                            new NewTopic(RETRY_TEST_TOPIC, 1, (short) 1),
                            new NewTopic(RETRY_TEST_DLT, 1, (short) 1)
                    )
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
        registry.add("spring.kafka.consumer.group-id", () -> "order-dlt-test");
        // The service's application.properties leaves auto-offset-reset at the
        // Kafka default (latest); records sent before the listener's first
        // partition assignment would be skipped, so pin earliest for the test.
        registry.add("spring.kafka.consumer.auto-offset-reset", () -> "earliest");

        // Keep the policy shape but make it fast enough for tests.
        registry.add("app.kafka.retry.attempts", () -> CONFIGURED_ATTEMPTS);
        registry.add("app.kafka.retry.initial-interval", () -> "100ms");
        registry.add("app.kafka.retry.multiplier", () -> "2.0");
        registry.add("app.kafka.retry.max-interval", () -> "200ms");
    }

    /**
     * Test-only listener whose failures are scripted by message prefix. It runs
     * on the same auto-configured container factory and error handler as the
     * production consumers, so retry counts and dead-letter routing observed
     * here are exactly what the business listeners get.
     */
    @TestConfiguration
    static class FlakyListenerConfig {

        static final Map<String, AtomicInteger> deliveries =
                new ConcurrentHashMap<>();
        static final List<String> processed = new CopyOnWriteArrayList<>();

        @Bean
        FlakyListener flakyListener() {
            return new FlakyListener();
        }

        static class FlakyListener {

            @KafkaListener(topics = RETRY_TEST_TOPIC)
            public void consume(String message) {
                int attempt = deliveries
                        .computeIfAbsent(message, key -> new AtomicInteger())
                        .incrementAndGet();

                if (message.startsWith("fail-forever:")) {
                    throw new IllegalStateException(
                            "scripted permanent failure for " + message
                    );
                }

                if (message.startsWith("fail-twice:") && attempt <= 2) {
                    throw new IllegalStateException(
                            "scripted transient failure " + attempt + " for " + message
                    );
                }

                if (message.startsWith("fail-lock-twice:") && attempt <= 2) {
                    throw new CannotAcquireLockException(
                            "scripted lock timeout " + attempt + " for " + message
                    );
                }

                if (message.startsWith("fail-dup-twice:") && attempt <= 2) {
                    throw new DuplicateKeyException(
                            "scripted duplicate key " + attempt + " for " + message
                    );
                }

                if (message.startsWith("fail-invalid:")) {
                    throw new InvalidEventException(
                            "scripted contract violation for " + message,
                            new RuntimeException("bad payload")
                    );
                }

                if (message.startsWith("fail-integrity:")) {
                    throw new DataIntegrityViolationException(
                            "scripted integrity violation for " + message
                    );
                }

                processed.add(message);
            }
        }
    }

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private OrderService orderService;

    @Test
    void transientFailureIsRetriedAndEventuallyProcessed() throws Exception {
        String message = "fail-twice:" + UUID.randomUUID();

        kafkaTemplate.send(RETRY_TEST_TOPIC, message).get();

        awaitTrue(
                () -> FlakyListenerConfig.processed.contains(message),
                Duration.ofSeconds(20),
                "message was not processed after transient failures"
        );

        assertEquals(
                CONFIGURED_ATTEMPTS,
                FlakyListenerConfig.deliveries.get(message).get(),
                "expected first delivery plus two retries"
        );

        assertTrue(
                drainDeadLetters(RETRY_TEST_DLT, record -> record.value().equals(message)).isEmpty(),
                "recovered message must not reach the dead-letter topic"
        );
    }

    @Test
    void exhaustedFailureReachesDeadLetterAndConsumerContinues() throws Exception {
        String poison = "fail-forever:" + UUID.randomUUID();

        kafkaTemplate.send(RETRY_TEST_TOPIC, poison).get();

        ConsumerRecord<String, String> deadLetter = awaitDeadLetter(
                RETRY_TEST_DLT,
                record -> record.value().equals(poison),
                Duration.ofSeconds(20)
        );

        assertNotNull(deadLetter, "poison message must be dead-lettered");
        assertEquals(
                CONFIGURED_ATTEMPTS,
                FlakyListenerConfig.deliveries.get(poison).get(),
                "retries must stop after the configured number of attempts"
        );

        String followUp = "ok:" + UUID.randomUUID();

        kafkaTemplate.send(RETRY_TEST_TOPIC, followUp).get();

        awaitTrue(
                () -> FlakyListenerConfig.processed.contains(followUp),
                Duration.ofSeconds(20),
                "consumer must keep processing records after a dead-letter hand-off"
        );
        assertEquals(1, FlakyListenerConfig.deliveries.get(followUp).get());
    }

    @Test
    void malformedRecordIsDeadLetteredAndProcessingContinues() throws Exception {
        String malformed = "not-json-" + UUID.randomUUID();

        kafkaTemplate.send(INVENTORY_EVENTS_TOPIC, malformed).get();

        ConsumerRecord<String, String> deadLetter = awaitDeadLetter(
                INVENTORY_EVENTS_DLT,
                record -> record.value().equals(malformed),
                Duration.ofSeconds(20)
        );

        assertNotNull(deadLetter, "malformed record must be dead-lettered");

        // The real consumer must still process the next, valid record.
        Order order = createOrderWithItem();

        sendInventoryReserved(order);

        awaitTrue(
                () -> orderService.getOrder(order.getId()).getStatus()
                        == OrderStatus.CONFIRMED,
                Duration.ofSeconds(20),
                "valid event following a dead-lettered record was not processed"
        );
    }

    @Test
    void successfulEventDoesNotReachDeadLetter() throws Exception {
        Order order = createOrderWithItem();

        sendInventoryReserved(order);

        awaitTrue(
                () -> orderService.getOrder(order.getId()).getStatus()
                        == OrderStatus.CONFIRMED,
                Duration.ofSeconds(20),
                "valid event was not processed"
        );

        assertTrue(
                drainDeadLetters(
                        INVENTORY_EVENTS_DLT,
                        record -> order.getId().toString().equals(record.key())
                ).isEmpty(),
                "successful event must not be dead-lettered"
        );
    }

    @Test
    void reservationFailureIsBusinessOutcomeNotDeadLetter() throws Exception {
        Order order = createOrderWithItem();
        OrderItem item = order.getItems().get(0);

        sendInventoryEvent(
                order.getId(),
                "INVENTORY_RESERVATION_FAILED",
                objectMapper.writeValueAsString(
                        new InventoryReservationFailedEvent(
                                order.getId(),
                                item.getId(),
                                item.getProductId(),
                                5,
                                "INSUFFICIENT_INVENTORY"
                        )
                )
        );

        awaitTrue(
                () -> orderService.getOrder(order.getId()).getStatus()
                        == OrderStatus.CANCELLED,
                Duration.ofSeconds(20),
                "reservation failure must cancel the order"
        );

        assertTrue(
                drainDeadLetters(
                        INVENTORY_EVENTS_DLT,
                        record -> order.getId().toString().equals(record.key())
                ).isEmpty(),
                "modeled business outcomes must not be dead-lettered"
        );
    }

    @Test
    void transientDatabaseFailureIsRetriedAndEventuallyProcessed() throws Exception {
        String message = "fail-lock-twice:" + UUID.randomUUID();

        kafkaTemplate.send(RETRY_TEST_TOPIC, message).get();

        awaitTrue(
                () -> FlakyListenerConfig.processed.contains(message),
                Duration.ofSeconds(20),
                "message was not processed after transient lock timeouts"
        );

        assertEquals(
                CONFIGURED_ATTEMPTS,
                FlakyListenerConfig.deliveries.get(message).get(),
                "lock-acquisition failures must be retried"
        );

        assertTrue(
                drainDeadLetters(RETRY_TEST_DLT, record -> record.value().equals(message)).isEmpty(),
                "recovered lock timeout must not reach the dead-letter topic"
        );
    }

    @Test
    void duplicateKeyIsRetriedAsIdempotencyRace() throws Exception {
        String message = "fail-dup-twice:" + UUID.randomUUID();

        kafkaTemplate.send(RETRY_TEST_TOPIC, message).get();

        awaitTrue(
                () -> FlakyListenerConfig.processed.contains(message),
                Duration.ofSeconds(20),
                "message was not processed after duplicate-key failures"
        );

        assertEquals(
                CONFIGURED_ATTEMPTS,
                FlakyListenerConfig.deliveries.get(message).get(),
                "duplicate-key failures must be retried"
        );

        assertTrue(
                drainDeadLetters(RETRY_TEST_DLT, record -> record.value().equals(message)).isEmpty(),
                "recovered duplicate-key race must not reach the dead-letter topic"
        );
    }

    @Test
    void invalidEventIsDeadLetteredWithoutRetries() throws Exception {
        String message = "fail-invalid:" + UUID.randomUUID();

        kafkaTemplate.send(RETRY_TEST_TOPIC, message).get();

        ConsumerRecord<String, String> deadLetter = awaitDeadLetter(
                RETRY_TEST_DLT,
                record -> record.value().equals(message),
                Duration.ofSeconds(20)
        );

        assertNotNull(deadLetter);
        assertEquals(
                1,
                FlakyListenerConfig.deliveries.get(message).get(),
                "contract violations must be dead-lettered without retries"
        );
    }

    @Test
    void integrityViolationIsDeadLetteredWithoutRetries() throws Exception {
        String message = "fail-integrity:" + UUID.randomUUID();

        kafkaTemplate.send(RETRY_TEST_TOPIC, message).get();

        ConsumerRecord<String, String> deadLetter = awaitDeadLetter(
                RETRY_TEST_DLT,
                record -> record.value().equals(message),
                Duration.ofSeconds(20)
        );

        assertNotNull(deadLetter);
        assertEquals(
                1,
                FlakyListenerConfig.deliveries.get(message).get(),
                "integrity violations must be dead-lettered without retries"
        );
    }

    /** Creates an order carrying one real item, so item-level reservation events can target it. */
    private Order createOrderWithItem() {
        Order order = orderService.createOrder(UUID.randomUUID(), "USD");

        return orderService.addItem(
                order.getId(),
                UUID.randomUUID(),
                3,
                new BigDecimal("12.50")
        );
    }

    private void sendInventoryReserved(Order order) throws Exception {
        OrderItem item = order.getItems().get(0);

        sendInventoryEvent(
                order.getId(),
                "INVENTORY_RESERVED",
                objectMapper.writeValueAsString(
                        new InventoryReservedEvent(
                                order.getId(),
                                item.getId(),
                                item.getProductId(),
                                item.getQuantity()
                        )
                )
        );
    }

    private void sendInventoryEvent(
            UUID orderId,
            String eventType,
            String payload
    ) throws Exception {
        String message = objectMapper.writeValueAsString(
                new InventoryEventEnvelope(
                        UUID.randomUUID(),
                        "Order",
                        orderId,
                        eventType,
                        payload,
                        Instant.now()
                )
        );

        kafkaTemplate.send(
                INVENTORY_EVENTS_TOPIC,
                orderId.toString(),
                message
        ).get();
    }

    private static KafkaConsumer<String, String> newDeadLetterConsumer(String topic) {
        Properties properties = new Properties();
        properties.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafka.getBootstrapServers()
        );
        properties.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                "dlt-verifier-" + UUID.randomUUID()
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

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(List.of(topic));
        return consumer;
    }

    /** Polls the dead-letter topic until a matching record arrives or the deadline passes. */
    private static ConsumerRecord<String, String> awaitDeadLetter(
            String topic,
            Predicate<ConsumerRecord<String, String>> matcher,
            Duration timeout
    ) {
        long deadline = System.currentTimeMillis() + timeout.toMillis();

        try (KafkaConsumer<String, String> consumer = newDeadLetterConsumer(topic)) {
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(250));

                for (ConsumerRecord<String, String> record : records) {
                    if (matcher.test(record)) {
                        return record;
                    }
                }
            }
        }

        fail("Timed out waiting for a dead-letter record on " + topic);
        return null;
    }

    /** Reads everything currently on the dead-letter topic and returns matching records. */
    private static List<ConsumerRecord<String, String>> drainDeadLetters(
            String topic,
            Predicate<ConsumerRecord<String, String>> matcher
    ) {
        List<ConsumerRecord<String, String>> matches = new ArrayList<>();
        long deadline = System.currentTimeMillis() + 3_000;

        try (KafkaConsumer<String, String> consumer = newDeadLetterConsumer(topic)) {
            while (System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, String> record
                        : consumer.poll(Duration.ofMillis(250))) {
                    if (matcher.test(record)) {
                        matches.add(record);
                    }
                }
            }
        }

        return matches;
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
