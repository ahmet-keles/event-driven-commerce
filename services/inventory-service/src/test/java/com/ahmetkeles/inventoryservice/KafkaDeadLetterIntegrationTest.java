package com.ahmetkeles.inventoryservice;

import com.ahmetkeles.inventoryservice.inventory.InventoryItem;
import com.ahmetkeles.inventoryservice.inventory.InventoryItemRepository;
import com.ahmetkeles.inventoryservice.inventory.ProcessedEventRepository;
import com.ahmetkeles.inventoryservice.messaging.InvalidEventException;
import com.ahmetkeles.inventoryservice.messaging.OrderEventEnvelope;
import com.ahmetkeles.inventoryservice.messaging.OrderItemAddedEvent;
import com.ahmetkeles.inventoryservice.outbox.OutboxEventRepository;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.OptimisticLockingFailureException;
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
 * afterwards. Modeled business outcomes (e.g. insufficient inventory) must
 * never reach the dead-letter topic.
 */
@SpringBootTest(
        classes = {
                InventoryServiceApplication.class,
                KafkaDeadLetterIntegrationTest.FlakyListenerConfig.class
        }
)
class KafkaDeadLetterIntegrationTest {

    private static final String ORDER_EVENTS_TOPIC = "order.events";
    private static final String ORDER_EVENTS_DLT = "order.events.DLT";
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
            admin.createTopics(
                    List.of(
                            new NewTopic(ORDER_EVENTS_TOPIC, 3, (short) 1),
                            new NewTopic(ORDER_EVENTS_DLT, 1, (short) 1),
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
        registry.add("spring.kafka.consumer.group-id", () -> "inventory-dlt-test");

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
                    throw new OptimisticLockingFailureException(
                            "scripted lock conflict " + attempt + " for " + message
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
    private InventoryItemRepository inventoryItemRepository;

    @Autowired
    private ProcessedEventRepository processedEventRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @BeforeEach
    void cleanDatabase() {
        outboxEventRepository.deleteAll();
        processedEventRepository.deleteAll();
        inventoryItemRepository.deleteAll();
    }

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

        kafkaTemplate.send(ORDER_EVENTS_TOPIC, malformed).get();

        ConsumerRecord<String, String> deadLetter = awaitDeadLetter(
                ORDER_EVENTS_DLT,
                record -> record.value().equals(malformed),
                Duration.ofSeconds(20)
        );

        assertNotNull(deadLetter, "malformed record must be dead-lettered");

        // The real consumer must still process the next, valid record.
        UUID eventId = UUID.randomUUID();
        UUID productId = UUID.randomUUID();

        inventoryItemRepository.saveAndFlush(new InventoryItem(productId, 10));

        sendOrderItemAdded(eventId, UUID.randomUUID(), productId, 3);

        awaitTrue(
                () -> processedEventRepository.existsById(eventId),
                Duration.ofSeconds(20),
                "valid event following a dead-lettered record was not processed"
        );

        InventoryItem item =
                inventoryItemRepository.findById(productId).orElseThrow();
        assertEquals(7, item.getAvailableQuantity());
        assertEquals(3, item.getReservedQuantity());
    }

    @Test
    void successfulEventDoesNotReachDeadLetter() throws Exception {
        UUID eventId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        UUID productId = UUID.randomUUID();

        inventoryItemRepository.saveAndFlush(new InventoryItem(productId, 10));

        sendOrderItemAdded(eventId, orderId, productId, 4);

        awaitTrue(
                () -> processedEventRepository.existsById(eventId),
                Duration.ofSeconds(20),
                "valid event was not processed"
        );

        assertTrue(
                drainDeadLetters(
                        ORDER_EVENTS_DLT,
                        record -> orderId.toString().equals(record.key())
                ).isEmpty(),
                "successful event must not be dead-lettered"
        );
    }

    @Test
    void insufficientInventoryIsBusinessOutcomeNotDeadLetter() throws Exception {
        UUID eventId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        UUID productId = UUID.randomUUID();

        inventoryItemRepository.saveAndFlush(new InventoryItem(productId, 1));

        sendOrderItemAdded(eventId, orderId, productId, 5);

        awaitTrue(
                () -> processedEventRepository.existsById(eventId),
                Duration.ofSeconds(20),
                "reservation failure was not recorded as processed"
        );

        assertTrue(
                outboxEventRepository.findAll().stream()
                        .anyMatch(event ->
                                orderId.equals(event.getAggregateId())
                                        && "INVENTORY_RESERVATION_FAILED"
                                                .equals(event.getEventType())
                        ),
                "insufficient inventory must produce the modeled failure event"
        );

        assertTrue(
                drainDeadLetters(
                        ORDER_EVENTS_DLT,
                        record -> orderId.toString().equals(record.key())
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
                "message was not processed after transient lock conflicts"
        );

        assertEquals(
                CONFIGURED_ATTEMPTS,
                FlakyListenerConfig.deliveries.get(message).get(),
                "optimistic locking failures must be retried"
        );

        assertTrue(
                drainDeadLetters(RETRY_TEST_DLT, record -> record.value().equals(message)).isEmpty(),
                "recovered lock conflict must not reach the dead-letter topic"
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

    private void sendOrderItemAdded(
            UUID eventId,
            UUID orderId,
            UUID productId,
            int quantity
    ) throws Exception {
        String payload = objectMapper.writeValueAsString(
                new OrderItemAddedEvent(
                        orderId,
                        productId,
                        quantity,
                        new BigDecimal("12.50"),
                        new BigDecimal("37.50")
                )
        );

        String message = objectMapper.writeValueAsString(
                new OrderEventEnvelope(
                        eventId,
                        "Order",
                        orderId,
                        "ORDER_ITEM_ADDED",
                        payload,
                        Instant.now()
                )
        );

        kafkaTemplate.send(ORDER_EVENTS_TOPIC, orderId.toString(), message).get();
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
