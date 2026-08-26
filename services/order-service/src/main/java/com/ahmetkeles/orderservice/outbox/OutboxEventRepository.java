package com.ahmetkeles.orderservice.outbox;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.UUID;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {

    List<OutboxEvent> findTop100ByPublishedAtIsNullOrderByOccurredAtAsc();

    /**
     * Claims a batch of unpublished events for the calling publisher instance.
     *
     * <p>{@code FOR UPDATE SKIP LOCKED} is what makes the outbox safe to poll
     * from more than one replica. Rows already locked by another in-flight
     * transaction are skipped rather than waited on, so two replicas never
     * claim the same row and neither one blocks the other.
     *
     * <p>The row locks live for exactly as long as the calling transaction, so
     * callers must run inside a transaction and should keep it short. A replica
     * that crashes mid-batch releases its locks when its connection dies; the
     * rows it never published simply reappear on the next poll.
     *
     * <p>Ordering is {@code occurred_at} then {@code id}. The {@code id}
     * tiebreaker makes the order deterministic and stable across polls, but
     * because ids are random UUIDs it is arbitrary among rows sharing a
     * timestamp. See {@link OutboxPublisher} for the ordering guarantees this
     * does and does not provide.
     */
    @Query(
            value = """
                    SELECT *
                    FROM outbox_events
                    WHERE published_at IS NULL
                    ORDER BY occurred_at ASC, id ASC
                    LIMIT :batchSize
                    FOR UPDATE SKIP LOCKED
                    """,
            nativeQuery = true
    )
    List<OutboxEvent> lockPendingEvents(@Param("batchSize") int batchSize);
}
