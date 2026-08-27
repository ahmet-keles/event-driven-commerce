package com.ahmetkeles.inventoryservice.inventory;

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
     * Deletes one bounded batch of processed-event rows older than the
     * cutoff, returning how many rows went.
     *
     * <p>Deleting a row re-opens idempotency for that event id: a redelivery
     * arriving after the delete is processed as new. The retention age
     * therefore bounds the deduplication window and must stay longer than
     * any redelivery horizon (broker retention, dead-letter redrive) — see
     * EVENT_FLOW.md, "Retention".
     *
     * <p>{@code LIMIT} bounds every statement; {@code FOR UPDATE SKIP LOCKED}
     * lets concurrent replicas partition eligible rows instead of lock-waiting;
     * {@code REQUIRES_NEW} keeps each batch in its own short transaction.
     */
    @Modifying
    @Transactional(propagation = Propagation.REQUIRES_NEW)
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
    int deleteBatchOlderThan(
            @Param("cutoff") Instant cutoff,
            @Param("batchSize") int batchSize
    );
}
