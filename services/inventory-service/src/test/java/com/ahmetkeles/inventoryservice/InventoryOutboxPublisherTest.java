package com.ahmetkeles.inventoryservice;

import com.ahmetkeles.inventoryservice.outbox.OutboxEvent;
import com.ahmetkeles.inventoryservice.outbox.OutboxEventRepository;
import com.ahmetkeles.inventoryservice.outbox.OutboxPublisher;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import tools.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class InventoryOutboxPublisherTest {

    @Test
    void successfulPublishMarksEventPublished() {
        OutboxEventRepository repository =
                mock(OutboxEventRepository.class);

        KafkaTemplate<String, String> kafkaTemplate =
                mock(KafkaTemplate.class);

        ObjectMapper objectMapper = new ObjectMapper();

        OutboxEvent event = new OutboxEvent(
                "Order",
                UUID.randomUUID(),
                "INVENTORY_RESERVED",
                "{\"quantity\":3}"
        );

        when(repository
                .findTop100ByPublishedAtIsNullOrderByOccurredAtAsc())
                .thenReturn(List.of(event));

        when(kafkaTemplate.send(
                anyString(),
                anyString(),
                anyString()
        )).thenReturn(CompletableFuture.completedFuture(null));

        OutboxPublisher publisher = new OutboxPublisher(
                repository,
                kafkaTemplate,
                objectMapper,
                "inventory.events"
        );

        publisher.publishPendingEvents();

        assertNotNull(event.getPublishedAt());
        verify(repository).save(event);

        verify(kafkaTemplate).send(
                eq("inventory.events"),
                eq(event.getAggregateId().toString()),
                anyString()
        );
    }

    @Test
    void failedPublishLeavesEventUnpublishedForRetry() {
        OutboxEventRepository repository =
                mock(OutboxEventRepository.class);

        KafkaTemplate<String, String> kafkaTemplate =
                mock(KafkaTemplate.class);

        ObjectMapper objectMapper = new ObjectMapper();

        OutboxEvent event = new OutboxEvent(
                "Order",
                UUID.randomUUID(),
                "INVENTORY_RESERVED",
                "{\"quantity\":3}"
        );

        when(repository
                .findTop100ByPublishedAtIsNullOrderByOccurredAtAsc())
                .thenReturn(List.of(event));

        when(kafkaTemplate.send(
                anyString(),
                anyString(),
                anyString()
        )).thenReturn(
                CompletableFuture.failedFuture(
                        new RuntimeException("Kafka unavailable")
                )
        );

        OutboxPublisher publisher = new OutboxPublisher(
                repository,
                kafkaTemplate,
                objectMapper,
                "inventory.events"
        );

        publisher.publishPendingEvents();

        assertNull(event.getPublishedAt());
        verify(repository, never()).save(event);
    }
}
