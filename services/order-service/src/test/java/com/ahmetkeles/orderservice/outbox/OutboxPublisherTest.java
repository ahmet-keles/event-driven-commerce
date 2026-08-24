package com.ahmetkeles.orderservice.outbox;

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

class OutboxPublisherTest {

    @Test
    void successfulPublishMarksEventPublished() {
        OutboxEventRepository repository = mock(OutboxEventRepository.class);
        KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
        ObjectMapper objectMapper = new ObjectMapper();

        OutboxEvent event = new OutboxEvent(
                "Order",
                UUID.randomUUID(),
                "ORDER_CREATED",
                "{\"orderId\":\"123\"}"
        );

        when(repository.findTop100ByPublishedAtIsNullOrderByOccurredAtAsc())
                .thenReturn(List.of(event));

        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));

        OutboxPublisher publisher = new OutboxPublisher(
                repository,
                kafkaTemplate,
                objectMapper,
                "order.events"
        );

        publisher.publishPendingEvents();

        assertNotNull(event.getPublishedAt());
        verify(repository).save(event);
    }

    @Test
    void failedPublishLeavesEventUnpublishedForRetry() {
        OutboxEventRepository repository = mock(OutboxEventRepository.class);
        KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
        ObjectMapper objectMapper = new ObjectMapper();

        OutboxEvent event = new OutboxEvent(
                "Order",
                UUID.randomUUID(),
                "ORDER_CREATED",
                "{\"orderId\":\"123\"}"
        );

        when(repository.findTop100ByPublishedAtIsNullOrderByOccurredAtAsc())
                .thenReturn(List.of(event));

        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.failedFuture(
                        new RuntimeException("Kafka unavailable")
                ));

        OutboxPublisher publisher = new OutboxPublisher(
                repository,
                kafkaTemplate,
                objectMapper,
                "order.events"
        );

        publisher.publishPendingEvents();

        assertNull(event.getPublishedAt());
        verify(repository, never()).save(event);
    }
}
