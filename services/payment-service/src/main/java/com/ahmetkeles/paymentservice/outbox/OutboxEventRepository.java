package com.ahmetkeles.paymentservice.outbox;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

public interface OutboxEventRepository
        extends JpaRepository<OutboxEvent, UUID> {

    List<OutboxEvent>
    findTop100ByPublishedAtIsNullOrderByOccurredAtAsc();

    /**
     * Claims a batch of unpublished events for the calling publisher instance.
     *
     * <p>{@code FOR UPDATE SKIP LOCKED} is what makes the outbox safe to poll
     * from more than one replica. Rows already locked by another in-flight
     * transaction are skipped rather than waited on, so two replicas never
     * claim the same row and neither one blocks the other.
     *
     * <p>The row locks live for exactly as long as the calling transaction, so
     * the transaction is mandatory — a call without one would silently release
     * each lock at statement end and defeat the guarantee, so it fails fast
     * instead. Keep the transaction short. A replica
     * that crashes mid-batch releases its locks when its connection dies; the
     * rows it never published simply reappear on the next poll.
     *
     * <p>Ordering is {@code occurred_at} then {@code id}. The {@code id}
     * tiebreaker makes the order deterministic and stable across polls, but
     * because ids are random UUIDs it is arbitrary among rows sharing a
     * timestamp. See {@link OutboxPublisher} for the ordering guarantees this
     * does and does not provide.
     */
    @Transactional(propagation = Propagation.MANDATORY)
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

    /**
     * Returns, for each of the given aggregates, the id of its oldest
     * unpublished event.
     *
     * <p>This is the cross-replica ordering guard. A batch claimed with
     * {@code SKIP LOCKED} can hold a later event of an aggregate whose earlier
     * event is currently locked by another replica; publishing it would invert
     * the aggregate's stream. Comparing each claimed aggregate's first event
     * against this result inside the same transaction detects that case: only
     * the replica holding the aggregate's oldest pending event may publish it,
     * everyone else defers the aggregate to a later poll.
     *
     * <p>Runs under READ COMMITTED, so a row another replica already committed
     * as published is correctly invisible here, and a row it failed or is
     * still publishing still counts as pending. Both outcomes make this guard
     * conservative: it can only defer needlessly, never publish early.
     */
    @Transactional(propagation = Propagation.MANDATORY)
    @Query(
            value = """
                    SELECT DISTINCT ON (aggregate_id) id
                    FROM outbox_events
                    WHERE published_at IS NULL
                      AND aggregate_id IN (:aggregateIds)
                    ORDER BY aggregate_id, occurred_at ASC, id ASC
                    """,
            nativeQuery = true
    )
    List<UUID> findOldestPendingEventIds(
            @Param("aggregateIds") Collection<UUID> aggregateIds
    );

    /**
     * Retention: deletes one bounded batch of rows that were published before
     * the cutoff. The inner select filters on {@code published_at IS NOT NULL},
     * so unpublished rows — undelivered work — are structurally out of reach,
     * whatever their age, and the lock set is disjoint from the publisher's
     * claim transaction, which only locks unpublished rows. Requires the
     * caller's transaction so the lock-and-delete pair stays atomic and short;
     * {@code FOR UPDATE SKIP LOCKED} keeps concurrent replicas on disjoint
     * victims.
     */
    @Transactional(propagation = Propagation.MANDATORY)
    @Modifying
    @Query(
            value = """
                    DELETE FROM outbox_events
                    WHERE id IN (
                        SELECT id
                        FROM outbox_events
                        WHERE published_at IS NOT NULL
                          AND published_at < :cutoff
                        ORDER BY published_at ASC, id ASC
                        LIMIT :batchSize
                        FOR UPDATE SKIP LOCKED
                    )
                    """,
            nativeQuery = true
    )
    int deletePublishedBatch(
            @Param("cutoff") Instant cutoff,
            @Param("batchSize") int batchSize
    );
}
