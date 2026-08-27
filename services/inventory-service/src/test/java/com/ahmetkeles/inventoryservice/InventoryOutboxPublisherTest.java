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
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Covers the publisher's per-event decision logic. The locking behaviour that
 * makes this safe across replicas is a PostgreSQL guarantee and is verified
 * against a real database in
 * {@link com.ahmetkeles.inventoryservice.outbox.OutboxPublisherConcurrencyIntegrationTest};
 * it is deliberately not simulated here.
 */
class InventoryOutboxPublisherTest {

    private static final int BATCH_SIZE = 25;
    private static final long SEND_TIMEOUT_MS = 2_000;
    private static final long POLL_DEADLINE_MS = 4_000;

    private OutboxPublisher publisher(
            OutboxEventRepository repository,
            KafkaTemplate<String, String> kafkaTemplate
    ) {
        return new OutboxPublisher(
                repository,
                kafkaTemplate,
                new ObjectMapper(),
                "inventory.events",
                BATCH_SIZE,
                SEND_TIMEOUT_MS,
                POLL_DEADLINE_MS
        );
    }

    private OutboxEvent event(UUID aggregateId, String eventType) {
        return new OutboxEvent(
                "Order",
                aggregateId,
                eventType,
                "{\"quantity\":3}"
        );
    }

    /**
     * Stubs the claim and the cross-replica ordering guard as if this replica
     * holds every aggregate's oldest pending event — the single-replica case.
     */
    private void stubClaim(
            OutboxEventRepository repository,
            List<OutboxEvent> events
    ) {
        when(repository.lockPendingEvents(anyInt()))
                .thenReturn(events);

        when(repository.findOldestPendingEventIds(anyCollection()))
                .thenReturn(events.stream().map(OutboxEvent::getId).toList());
    }

    @Test
    void successfulPublishMarksEventPublished() {
        OutboxEventRepository repository =
                mock(OutboxEventRepository.class);

        KafkaTemplate<String, String> kafkaTemplate =
                mock(KafkaTemplate.class);

        OutboxEvent event = event(UUID.randomUUID(), "INVENTORY_RESERVED");
        stubClaim(repository, List.of(event));

        when(kafkaTemplate.send(
                anyString(),
                anyString(),
                anyString()
        )).thenReturn(CompletableFuture.completedFuture(null));

        publisher(repository, kafkaTemplate).publishPendingEvents();

        assertNotNull(event.getPublishedAt());
        verify(repository).save(event);
        verify(repository).lockPendingEvents(BATCH_SIZE);

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

        OutboxEvent event = event(UUID.randomUUID(), "INVENTORY_RESERVED");
        stubClaim(repository, List.of(event));

        when(kafkaTemplate.send(
                anyString(),
                anyString(),
                anyString()
        )).thenReturn(
                CompletableFuture.failedFuture(
                        new RuntimeException("Kafka unavailable")
                )
        );

        publisher(repository, kafkaTemplate).publishPendingEvents();

        assertNull(event.getPublishedAt());
        verify(repository, never()).save(event);
    }

    @Test
    void failedEventBlocksLaterEventsForTheSameAggregate() {
        OutboxEventRepository repository =
                mock(OutboxEventRepository.class);

        KafkaTemplate<String, String> kafkaTemplate =
                mock(KafkaTemplate.class);

        UUID aggregateId = UUID.randomUUID();
        OutboxEvent first = event(aggregateId, "INVENTORY_RESERVED");
        OutboxEvent second = event(aggregateId, "INVENTORY_RESERVATION_FAILED");
        stubClaim(repository, List.of(first, second));

        when(kafkaTemplate.send(
                anyString(),
                anyString(),
                anyString()
        )).thenReturn(
                CompletableFuture.failedFuture(
                        new RuntimeException("Kafka rejected this record")
                )
        );

        publisher(repository, kafkaTemplate).publishPendingEvents();

        assertNull(first.getPublishedAt());
        assertNull(second.getPublishedAt());

        // The second event must not even be attempted: sending it after the
        // first one failed would reorder this aggregate's stream.
        verify(kafkaTemplate, times(1))
                .send(anyString(), anyString(), anyString());
    }

    @Test
    void failedAggregateDoesNotBlockOtherAggregates() {
        OutboxEventRepository repository =
                mock(OutboxEventRepository.class);

        KafkaTemplate<String, String> kafkaTemplate =
                mock(KafkaTemplate.class);

        UUID failingAggregate = UUID.randomUUID();
        UUID healthyAggregate = UUID.randomUUID();

        OutboxEvent failing = event(failingAggregate, "INVENTORY_RESERVED");
        OutboxEvent healthy = event(healthyAggregate, "INVENTORY_RESERVED");
        stubClaim(repository, List.of(failing, healthy));

        when(kafkaTemplate.send(
                anyString(),
                eq(failingAggregate.toString()),
                anyString()
        )).thenReturn(
                CompletableFuture.failedFuture(
                        new RuntimeException("Kafka rejected this record")
                )
        );

        when(kafkaTemplate.send(
                anyString(),
                eq(healthyAggregate.toString()),
                anyString()
        )).thenReturn(CompletableFuture.completedFuture(null));

        publisher(repository, kafkaTemplate).publishPendingEvents();

        assertNull(failing.getPublishedAt());
        assertNotNull(healthy.getPublishedAt());
    }

    @Test
    void aggregateWhoseOldestEventIsHeldElsewhereIsDeferred() {
        OutboxEventRepository repository =
                mock(OutboxEventRepository.class);

        KafkaTemplate<String, String> kafkaTemplate =
                mock(KafkaTemplate.class);

        UUID contestedAggregate = UUID.randomUUID();
        UUID unclaimedOlderEventId = UUID.randomUUID();

        OutboxEvent laterEvent =
                event(contestedAggregate, "INVENTORY_RESERVATION_FAILED");

        when(repository.lockPendingEvents(anyInt()))
                .thenReturn(List.of(laterEvent));

        // The ordering guard reports an older pending event this replica does
        // not hold — another replica has it claimed right now.
        when(repository.findOldestPendingEventIds(anyCollection()))
                .thenReturn(List.of(unclaimedOlderEventId));

        publisher(repository, kafkaTemplate).publishPendingEvents();

        assertNull(laterEvent.getPublishedAt());
        verifyNoInteractions(kafkaTemplate);
    }
}
