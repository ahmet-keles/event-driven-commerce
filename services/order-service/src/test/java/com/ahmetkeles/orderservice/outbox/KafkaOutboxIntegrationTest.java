package com.ahmetkeles.orderservice.outbox;

import com.ahmetkeles.orderservice.domain.Order;
import com.ahmetkeles.orderservice.service.OrderService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class KafkaOutboxIntegrationTest {

    private static final PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:17-alpine");

    private static final KafkaContainer kafka =
            new KafkaContainer("apache/kafka:4.0.0");

    static {
        postgres.start();
        kafka.start();
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);

        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("app.kafka.enabled", () -> "true");
        registry.add("app.outbox.publish-interval-ms", () -> "100");
    }

    @Autowired
    private OrderService orderService;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Test
    void orderCreatedEventIsPublishedFromOutboxToKafka() {
        UUID customerId = UUID.randomUUID();

        Order order = orderService.createOrder(customerId, "USD");

        ConsumerRecord<String, String> record =
                waitForKafkaRecord(order.getId());

        assertNotNull(record);
        assertEquals(order.getId().toString(), record.key());
        assertTrue(record.value().contains("\"eventType\":\"ORDER_CREATED\""));
        assertTrue(record.value().contains(
                "\"aggregateId\":\"" + order.getId() + "\""
        ));

        OutboxEvent outboxEvent = waitForPublishedOutboxEvent(order.getId());

        assertNotNull(outboxEvent.getPublishedAt());
        assertEquals("ORDER_CREATED", outboxEvent.getEventType());
        assertEquals(order.getId(), outboxEvent.getAggregateId());
    }

    private ConsumerRecord<String, String> waitForKafkaRecord(UUID orderId) {
        Properties properties = new Properties();
        properties.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafka.getBootstrapServers()
        );
        properties.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                "test-" + UUID.randomUUID()
        );
        properties.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest"
        );
        properties.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class
        );
        properties.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class
        );

        try (KafkaConsumer<String, String> consumer =
                     new KafkaConsumer<>(properties)) {

            consumer.subscribe(List.of("order.events"));

            long deadline = System.currentTimeMillis() + 10_000;

            while (System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, String> record :
                        consumer.poll(Duration.ofMillis(500))) {

                    if (orderId.toString().equals(record.key())) {
                        return record;
                    }
                }
            }
        }

        fail("Timed out waiting for ORDER_CREATED Kafka event");
        return null;
    }

    private OutboxEvent waitForPublishedOutboxEvent(UUID orderId) {
        long deadline = System.currentTimeMillis() + 10_000;

        while (System.currentTimeMillis() < deadline) {
            List<OutboxEvent> events = outboxEventRepository.findAll();

            for (OutboxEvent event : events) {
                if (orderId.equals(event.getAggregateId())
                        && event.getPublishedAt() != null) {
                    return event;
                }
            }

            try {
                Thread.sleep(100);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                fail("Interrupted while waiting for outbox publication");
            }
        }

        fail("Timed out waiting for outbox event to be marked published");
        return null;
    }
}
