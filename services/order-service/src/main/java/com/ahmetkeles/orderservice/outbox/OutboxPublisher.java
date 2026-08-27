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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
 * <h2>Bounded lock hold</h2>
 * The claim transaction pins row locks and one pooled connection, so its
 * duration is capped three ways rather than left to batch arithmetic:
 * <ul>
 *   <li>{@code app.outbox.poll-deadline-ms}: no new send starts after the
 *       deadline; whatever was not reached stays pending for the next poll;</li>
 *   <li>a send that times out locally aborts the rest of the poll — a local
 *       timeout means the broker is degraded, and giving the remaining rows
 *       their own timeouts would multiply the hold for no benefit;</li>
 *   <li>{@code max.block.ms} is configured down so a metadata stall inside
 *       {@code send()} cannot pin the transaction for the producer default of
 *       60 seconds.</li>
 * </ul>
 * Worst case is therefore {@code poll-deadline + max.block + send-timeout} —
 * independent of batch size — and with the shipped defaults comes to
 * 4s + 2s + 2s = 8 seconds, on exactly one connection per replica (the
 * scheduler is single-threaded and {@code fixedDelay} prevents overlap).
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
 * Per-aggregate order is preserved, including across replicas. Within a batch,
 * a failed send blocks the rest of that aggregate's events for the remainder
 * of the poll, so this publisher never emits an aggregate's events out of
 * order or with a gap; other aggregates continue, so one poisonous row cannot
 * stall the whole outbox. Across replicas, an aggregate is only published by
 * the replica whose claim holds that aggregate's oldest pending event (checked
 * against {@link OutboxEventRepository#findOldestPendingEventIds} inside the
 * claim transaction); a replica holding only a later event defers the
 * aggregate to a later poll instead of publishing it early.
 *
 * <p>Today's consumers converge regardless of publish order — this topic's
 * consumer ignores {@code ORDER_CREATED} and treats each
 * {@code ORDER_ITEM_ADDED} as an independent, idempotent reservation, and the
 * reservation outcomes flowing back are commutative under per-item reservation
 * state — so the guard is not load-bearing for current event types. It stays
 * because per-aggregate order is the outbox's contract for whatever ships
 * next — an {@code ORDER_CANCELLED}, payment authorization/capture, a
 * reservation release are all sequence-dependent — and because it costs one
 * indexed query per poll and can only defer, never publish early.
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

    private enum SendOutcome {
        PUBLISHED,
        FAILED,
        TIMED_OUT
    }

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String topicName;
    private final int batchSize;
    private final long sendTimeoutMs;
    private final long pollDeadlineMs;

    public OutboxPublisher(
            OutboxEventRepository outboxEventRepository,
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper,
            @Value("${app.kafka.order-events-topic}") String topicName,
            @Value("${app.outbox.batch-size:25}") int batchSize,
            @Value("${app.outbox.send-timeout-ms:2000}") long sendTimeoutMs,
            @Value("${app.outbox.poll-deadline-ms:4000}") long pollDeadlineMs
    ) {
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.topicName = topicName;
        this.batchSize = batchSize;
        this.sendTimeoutMs = sendTimeoutMs;
        this.pollDeadlineMs = pollDeadlineMs;
    }

    @Scheduled(
            fixedDelayString = "${app.outbox.publish-interval-ms:1000}"
    )
    @Transactional
    public void publishPendingEvents() {
        List<OutboxEvent> events =
                outboxEventRepository.lockPendingEvents(batchSize);

        if (events.isEmpty()) {
            return;
        }

        Set<UUID> blockedAggregates = deferAggregatesClaimedElsewhere(events);

        long deadlineNanos =
                System.nanoTime()
                        + TimeUnit.MILLISECONDS.toNanos(pollDeadlineMs);

        for (OutboxEvent event : events) {
            UUID aggregateId = event.getAggregateId();

            // An earlier event for this aggregate failed here or is held by
            // another replica. Publishing this one now would reorder the
            // aggregate's stream, so leave it pending.
            if (blockedAggregates.contains(aggregateId)) {
                continue;
            }

            if (System.nanoTime() >= deadlineNanos) {
                log.info(
                        "Outbox poll deadline of {} ms reached; leaving remaining claimed events for the next poll.",
                        pollDeadlineMs
                );
                break;
            }

            SendOutcome outcome = publish(event);

            if (outcome == SendOutcome.FAILED) {
                blockedAggregates.add(aggregateId);
            }

            if (outcome == SendOutcome.TIMED_OUT) {
                // A local timeout means the broker is degraded, not that this
                // record is bad. Burning a timeout per remaining row would
                // multiply the lock hold for nothing; retry everything on the
                // next poll instead.
                break;
            }
        }
    }

    /**
     * Cross-replica ordering guard: an aggregate may only be published by the
     * replica whose claim holds its oldest pending event. The claim takes the
     * globally oldest unlocked rows first, so holding a later event of an
     * aggregate without its earlier one can only mean another replica has the
     * earlier one in flight — defer the aggregate rather than overtake it.
     */
    private Set<UUID> deferAggregatesClaimedElsewhere(
            List<OutboxEvent> events
    ) {
        Map<UUID, UUID> firstClaimedPerAggregate = new LinkedHashMap<>();

        for (OutboxEvent event : events) {
            firstClaimedPerAggregate.putIfAbsent(
                    event.getAggregateId(),
                    event.getId()
            );
        }

        Set<UUID> oldestPendingIds = new HashSet<>(
                outboxEventRepository.findOldestPendingEventIds(
                        firstClaimedPerAggregate.keySet()
                )
        );

        Set<UUID> deferred = new HashSet<>();

        firstClaimedPerAggregate.forEach((aggregateId, firstClaimedId) -> {
            if (!oldestPendingIds.contains(firstClaimedId)) {
                deferred.add(aggregateId);
            }
        });

        return deferred;
    }

    private SendOutcome publish(OutboxEvent event) {
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

            return SendOutcome.PUBLISHED;
        } catch (TimeoutException exception) {
            log.error(
                    "Timed out after {} ms publishing order outbox event {} for aggregate {}. Event remains unpublished for retry.",
                    sendTimeoutMs,
                    event.getId(),
                    event.getAggregateId()
            );

            return SendOutcome.TIMED_OUT;
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();

            log.warn(
                    "Interrupted while publishing order outbox event {} for aggregate {}. Event remains unpublished for retry.",
                    event.getId(),
                    event.getAggregateId()
            );

            return SendOutcome.TIMED_OUT;
        } catch (Exception exception) {
            log.error(
                    "Failed to publish order outbox event {} for aggregate {}. Event remains unpublished for retry.",
                    event.getId(),
                    event.getAggregateId(),
                    exception
            );

            return SendOutcome.FAILED;
        }
    }
}
