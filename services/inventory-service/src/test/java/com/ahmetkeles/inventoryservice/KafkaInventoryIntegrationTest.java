package com.ahmetkeles.inventoryservice;

import com.ahmetkeles.inventoryservice.inventory.InventoryItem;
import com.ahmetkeles.inventoryservice.inventory.InventoryItemRepository;
import com.ahmetkeles.inventoryservice.inventory.InventoryReservationRepository;
import com.ahmetkeles.inventoryservice.inventory.ProcessedEventRepository;
import com.ahmetkeles.inventoryservice.messaging.OrderEventEnvelope;
import com.ahmetkeles.inventoryservice.messaging.OrderItemAddedEvent;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

@SpringBootTest
class KafkaInventoryIntegrationTest {

    private static final String TOPIC = "order.events";

    private static final PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:17-alpine");

    private static final KafkaContainer kafka =
            new KafkaContainer("apache/kafka:4.0.0");

    static {
        postgres.start();
        kafka.start();

        try (AdminClient admin = AdminClient.create(
                Map.of(
                        "bootstrap.servers",
                        kafka.getBootstrapServers()
                )
        )) {
            admin.createTopics(
                    List.of(new NewTopic(TOPIC, 3, (short) 1))
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

        registry.add(
                "spring.kafka.bootstrap-servers",
                kafka::getBootstrapServers
        );
        registry.add(
                "spring.kafka.consumer.group-id",
                () -> "inventory-integration-test"
        );
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
    private InventoryReservationRepository inventoryReservationRepository;

    @BeforeEach
    void cleanDatabase() {
        processedEventRepository.deleteAll();
        inventoryReservationRepository.deleteAll();
        inventoryItemRepository.deleteAll();
    }

    @Test
    void orderItemAddedEventReservesInventory() throws Exception {
        UUID eventId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        UUID productId = UUID.randomUUID();

        inventoryItemRepository.saveAndFlush(
                new InventoryItem(productId, 10)
        );

        sendOrderItemAdded(
                eventId,
                orderId,
                productId,
                3
        );

        waitUntilProcessed(eventId);

        InventoryItem item = inventoryItemRepository
                .findById(productId)
                .orElseThrow();

        assertEquals(7, item.getAvailableQuantity());
        assertEquals(3, item.getReservedQuantity());
        assertEquals(1, processedEventRepository.count());
    }

    @Test
    void duplicateKafkaEventReservesInventoryOnlyOnce() throws Exception {
        UUID eventId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        UUID productId = UUID.randomUUID();

        inventoryItemRepository.saveAndFlush(
                new InventoryItem(productId, 10)
        );

        sendOrderItemAdded(
                eventId,
                orderId,
                productId,
                3
        );

        waitUntilProcessed(eventId);

        UUID barrierEventId = UUID.randomUUID();
        UUID barrierProductId = UUID.randomUUID();

        inventoryItemRepository.saveAndFlush(
                new InventoryItem(barrierProductId, 5)
        );

        sendOrderItemAdded(
                eventId,
                orderId,
                productId,
                3
        );

        sendOrderItemAdded(
                barrierEventId,
                orderId,
                barrierProductId,
                1
        );

        waitUntilProcessed(barrierEventId);

        InventoryItem item = inventoryItemRepository
                .findById(productId)
                .orElseThrow();

        InventoryItem barrierItem = inventoryItemRepository
                .findById(barrierProductId)
                .orElseThrow();

        assertEquals(7, item.getAvailableQuantity());
        assertEquals(3, item.getReservedQuantity());

        assertEquals(4, barrierItem.getAvailableQuantity());
        assertEquals(1, barrierItem.getReservedQuantity());

        assertEquals(2, processedEventRepository.count());
    }

    private void sendOrderItemAdded(
            UUID eventId,
            UUID orderId,
            UUID productId,
            int quantity
    ) throws Exception {
        UUID orderItemId = UUID.randomUUID();

        String payload = objectMapper.writeValueAsString(
                new OrderItemAddedEvent(
                        orderId,
                        orderItemId,
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

        kafkaTemplate.send(
                TOPIC,
                orderId.toString(),
                message
        ).get();
    }

    private void waitUntilProcessed(UUID eventId) {
        long deadline = System.currentTimeMillis() + 10_000;

        while (System.currentTimeMillis() < deadline) {
            if (processedEventRepository.existsById(eventId)) {
                return;
            }

            try {
                Thread.sleep(100);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                fail("Interrupted while waiting for Kafka event");
            }
        }

        fail("Timed out waiting for Kafka event to be processed");
    }
}
