package com.ahmetkeles.orderservice.messaging;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

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
}
