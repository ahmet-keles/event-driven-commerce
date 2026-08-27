package com.ahmetkeles.paymentservice.payment;

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
     * Retention: deletes one bounded batch of ledger rows processed before the
     * cutoff. This ledger is the idempotency dedup window, not history — a
     * deleted row re-enables its eventId, so the retention age must always
     * exceed the source topic's retention plus the operational replay / DLT
     * redrive window (see {@code RetentionProperties#processedEventsMaxAge}).
     *
     * <p>{@code FOR UPDATE SKIP LOCKED} keeps concurrent replicas on disjoint
     * victims and skips rows a live transaction still holds. Requires the
     * caller's transaction so the lock-and-delete pair stays atomic.
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
