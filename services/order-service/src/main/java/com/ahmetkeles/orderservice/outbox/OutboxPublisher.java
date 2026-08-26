package com.ahmetkeles.orderservice.outbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Polls the transactional outbox and relays pending events to Kafka.
 *
 * <h2>Multi-replica safety</h2>
 * Each poll claims its batch with {@code SELECT ... FOR UPDATE SKIP LOCKED}
 * inside a single transaction. Rows held by another replica's open transaction
 * are skipped, so concurrent publishers partition the pending work between
 * themselves instead of racing for the same rows. Locks are released by commit,
 * rollback, or connection loss, so a replica that dies mid-batch hands its
 * unpublished rows straight back to the next poller.
 *
 * <h2>Delivery semantics: at-least-once, not exactly-once</h2>
 * {@code published_at} is only ever set after the broker has acknowledged the
 * send, never before. That ordering means a failure between the send and the
 * commit re-sends the event on a later poll rather than losing it. Two windows
 * produce duplicates, both of them deliberate:
 * <ul>
 *   <li>the send succeeds but the transaction fails to commit, so the whole
 *       claimed batch is retried;</li>
 *   <li>the send exceeds {@code app.outbox.send-timeout-ms} but the broker
 *       accepts the record anyway, so the row stays pending and is sent again.
 *       Note this timeout is a local wait, not a cancellation: the producer's
 *       own {@code delivery.timeout.ms} still governs the in-flight record.</li>
 * </ul>
 * Consumers must therefore deduplicate on the envelope's {@code eventId}.
 *
 * <h2>Ordering</h2>
 * Within one claimed batch, a failed send blocks the rest of that aggregate's
 * events for the remainder of the poll, so a single publisher never emits an
 * aggregate's events out of order or with a gap. Other aggregates continue, so
 * one poisonous row cannot stall the whole outbox.
 *
 * <p>This guarantee is per publisher instance. Across replicas it does not
 * hold: two replicas can claim different events for the same aggregate in the
 * same instant and send them in either order. Strict per-aggregate ordering
 * under scale-out needs the outbox partitioned by aggregate, which this class
 * deliberately does not attempt.
 */
@Component
@ConditionalOnProperty(
        name = "app.kafka.enabled",
        havingValue = "true",
        matchIfMissing = true
)
public class OutboxPublisher {

    private static final Logger log =
            LoggerFactory.getLogger(OutboxPublisher.class);

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String topicName;
    private final int batchSize;
    private final long sendTimeoutMs;

    public OutboxPublisher(
            OutboxEventRepository outboxEventRepository,
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper,
            @Value("${app.kafka.order-events-topic}") String topicName,
            @Value("${app.outbox.batch-size:100}") int batchSize,
            @Value("${app.outbox.send-timeout-ms:10000}") long sendTimeoutMs
    ) {
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.topicName = topicName;
        this.batchSize = batchSize;
        this.sendTimeoutMs = sendTimeoutMs;
    }

    /**
     * Claims and relays one batch of pending events.
     *
     * <p>The transaction spans the whole batch because the row locks it holds
     * are what keep other replicas off these rows. Worst-case lock hold is
     * therefore {@code batch-size × send-timeout-ms}; both are tunable so the
     * bound can be brought down where the broker is slow or replicas are many.
     */
    @Scheduled(
            fixedDelayString = "${app.outbox.publish-interval-ms:1000}"
    )
    @Transactional
    public void publishPendingEvents() {
        List<OutboxEvent> events =
                outboxEventRepository.lockPendingEvents(batchSize);

        Set<UUID> blockedAggregates = new HashSet<>();

        for (OutboxEvent event : events) {
            UUID aggregateId = event.getAggregateId();

            // An earlier event for this aggregate failed. Publishing this one
            // now would reorder the aggregate's stream, so leave it pending.
            if (blockedAggregates.contains(aggregateId)) {
                continue;
            }

            if (!publish(event)) {
                blockedAggregates.add(aggregateId);
            }
        }
    }

    /**
     * @return {@code true} when the broker acknowledged the event and the row
     *         was marked published, {@code false} when it must stay pending.
     */
    private boolean publish(OutboxEvent event) {
        try {
            String message = objectMapper.writeValueAsString(
                    PublishedOutboxEvent.from(event)
            );

            kafkaTemplate.send(
                    topicName,
                    event.getAggregateId().toString(),
                    message
            ).get(sendTimeoutMs, TimeUnit.MILLISECONDS);

            // Only ever after the broker has acknowledged the send.
            event.markPublished();
            outboxEventRepository.save(event);

            return true;
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();

            log.warn(
                    "Interrupted while publishing order outbox event {} for aggregate {}. Event remains unpublished for retry.",
                    event.getId(),
                    event.getAggregateId()
            );

            return false;
        } catch (Exception exception) {
            log.error(
                    "Failed to publish order outbox event {} for aggregate {}. Event remains unpublished for retry.",
                    event.getId(),
                    event.getAggregateId(),
                    exception
            );

            return false;
        }
    }
}
