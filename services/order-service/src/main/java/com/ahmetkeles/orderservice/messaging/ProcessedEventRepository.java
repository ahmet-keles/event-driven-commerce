package com.ahmetkeles.orderservice.messaging;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

public interface ProcessedEventRepository
        extends JpaRepository<ProcessedEvent, UUID> {

    /**
     * Atomically claims an event for processing. Returns 1 when this caller
     * won the claim and 0 when a committed row for the same event already
     * exists.
     *
     * <p>The insert itself is the duplicate check, arbitrated by the primary
     * key: there is no window between looking and writing. When two
     * transactions race on the same event id, PostgreSQL blocks the second
     * insert until the first transaction resolves — 0 is only ever returned
     * after a competing claim has committed, and an aborted competitor lets
     * this insert proceed. No exception is thrown in either case, so the
     * surrounding transaction stays usable.
     */
    @Modifying
    @Query(
            value = """
                    INSERT INTO processed_events
                        (event_id, event_type, aggregate_id, processed_at)
                    VALUES
                        (:eventId, :eventType, :aggregateId, :processedAt)
                    ON CONFLICT (event_id) DO NOTHING
                    """,
            nativeQuery = true
    )
    int claim(
            @Param("eventId") UUID eventId,
            @Param("eventType") String eventType,
            @Param("aggregateId") UUID aggregateId,
            @Param("processedAt") Instant processedAt
    );

    /**
     * Deletes one bounded batch of ledger rows older than the cutoff, oldest
     * first. The cutoff must be far beyond the longest plausible redelivery
     * window: a deleted row means the same eventId can be claimed — and its
     * mutation applied — again.
     *
     * <p>{@code FOR UPDATE SKIP LOCKED} keeps concurrent replicas on disjoint
     * victims and skips rows a live claim transaction still holds. Requires
     * the caller's transaction so the lock-and-delete pair stays atomic.
     */
    @Transactional(propagation = Propagation.MANDATORY)
    @Modifying
    @Query(
            value = """
                    DELETE FROM processed_events
                    WHERE event_id IN (
                        SELECT event_id
                        FROM processed_events
                        WHERE processed_at < :cutoff
                        ORDER BY processed_at ASC, event_id ASC
                        LIMIT :batchSize
                        FOR UPDATE SKIP LOCKED
                    )
                    """,
            nativeQuery = true
    )
    int deleteProcessedBatch(
            @Param("cutoff") Instant cutoff,
            @Param("batchSize") int batchSize
    );
}
