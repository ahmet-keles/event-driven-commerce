package com.ahmetkeles.paymentservice.retention;

import com.ahmetkeles.paymentservice.PostgreSQLIntegrationTest;
import com.ahmetkeles.paymentservice.payment.ProcessedEventRepository;
import com.ahmetkeles.paymentservice.outbox.OutboxEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Proves the bounded retention contract against a real PostgreSQL:
 *
 * <ul>
 * <li>only <em>published</em> outbox rows past the retention age are deleted —
 *     an unpublished row survives at any age, because it is undelivered
 *     work;</li>
 * <li>ledger rows are deleted only past their own retention age;</li>
 * <li>one run never deletes more than
 *     {@code batchSize * maxBatchesPerRun} rows per table;</li>
 * <li>two replicas purging concurrently delete each eligible row exactly once
 *     and nothing else ({@code FOR UPDATE SKIP LOCKED} partitions the
 *     victims).</li>
 * </ul>
 *
 * <p>The scheduled trigger is disabled in the test base; each test drives its
 * own {@link RetentionJob} instance synchronously with the policy under test.
 */
class RetentionJobIntegrationTest extends PostgreSQLIntegrationTest {

    private static final Instant OLD =
            Instant.now().minus(10, ChronoUnit.DAYS);
    private static final Instant RECENT =
            Instant.now().minus(1, ChronoUnit.HOURS);

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private ProcessedEventRepository processedEventRepository;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void clearDatabase() {
        jdbcTemplate.update("DELETE FROM outbox_events");
        jdbcTemplate.update("DELETE FROM processed_events");
    }

    @Test
    void deletesOnlyPublishedOutboxRowsOlderThanTheRetentionAge() {
        seedOutbox(3, OLD, OLD);        // published, expired -> deleted
        seedOutbox(2, OLD, RECENT);     // published recently  -> kept
        seedOutbox(2, OLD, null);       // unpublished, ancient -> kept
        seedOutbox(1, RECENT, null);    // unpublished, recent  -> kept

        RetentionJob.RetentionResult result =
                retentionJob(defaultPolicy()).purgeExpired();

        assertEquals(3, result.outboxDeleted());
        assertEquals(5, countOutbox());
        assertEquals(3, countUnpublishedOutbox(),
                "no unpublished row may ever be deleted, whatever its age");
    }

    @Test
    void deletesOnlyProcessedEventsOlderThanTheRetentionAge() {
        seedProcessedEvents(3, OLD);
        seedProcessedEvents(2, RECENT);

        RetentionJob.RetentionResult result =
                retentionJob(defaultPolicy()).purgeExpired();

        assertEquals(3, result.processedEventsDeleted());
        assertEquals(2, countProcessedEvents());
    }

    @Test
    void oneRunNeverDeletesMoreThanTheConfiguredBound() {
        seedOutbox(10, OLD, OLD);

        RetentionProperties policy = defaultPolicy();
        policy.setBatchSize(2);
        policy.setMaxBatchesPerRun(2);
        RetentionJob job = retentionJob(policy);

        assertEquals(4, job.purgeExpired().outboxDeleted(),
                "a run is capped at batchSize * maxBatchesPerRun");
        assertEquals(6, countOutbox());

        assertEquals(4, job.purgeExpired().outboxDeleted());
        assertEquals(2, job.purgeExpired().outboxDeleted(),
                "the backlog drains across runs");
        assertEquals(0, countOutbox());
    }

    @Test
    void concurrentReplicasDeleteEachEligibleRowExactlyOnce()
            throws Exception {
        seedOutbox(40, OLD, OLD);
        seedOutbox(5, OLD, null);
        seedProcessedEvents(40, OLD);

        RetentionProperties policy = defaultPolicy();
        policy.setBatchSize(5);
        policy.setMaxBatchesPerRun(20);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            List<Future<RetentionJob.RetentionResult>> results =
                    executor.invokeAll(List.of(
                            () -> retentionJob(policy).purgeExpired(),
                            () -> retentionJob(policy).purgeExpired()
                    ));

            int outboxDeleted = 0;
            int processedDeleted = 0;
            for (Future<RetentionJob.RetentionResult> result : results) {
                RetentionJob.RetentionResult r =
                        result.get(60, TimeUnit.SECONDS);
                outboxDeleted += r.outboxDeleted();
                processedDeleted += r.processedEventsDeleted();
            }

            assertEquals(40, outboxDeleted,
                    "the two replicas must partition the eligible rows, "
                            + "not double-delete or skip any");
            assertEquals(40, processedDeleted);
            assertEquals(5, countOutbox(),
                    "only the unpublished rows may remain");
            assertEquals(5, countUnpublishedOutbox());
            assertEquals(0, countProcessedEvents());
        } finally {
            executor.shutdownNow();
        }
    }

    // -- harness --------------------------------------------------------------

    private RetentionJob retentionJob(RetentionProperties properties) {
        return new RetentionJob(
                outboxEventRepository,
                processedEventRepository,
                transactionManager,
                properties
        );
    }

    private static RetentionProperties defaultPolicy() {
        RetentionProperties properties = new RetentionProperties();
        properties.setOutboxMaxAge(Duration.ofDays(7));
        properties.setProcessedEventsMaxAge(Duration.ofDays(7));
        return properties;
    }

    private void seedOutbox(int count, Instant occurredAt, Instant publishedAt) {
        for (int i = 0; i < count; i++) {
            jdbcTemplate.update("""
                    INSERT INTO outbox_events
                        (id, aggregate_type, aggregate_id, event_type,
                         payload, occurred_at, published_at)
                    VALUES (?, 'Order', ?, 'PAYMENT_COMPLETED', '{}', ?, ?)
                    """,
                    UUID.randomUUID(), UUID.randomUUID(),
                    occurredAt.atOffset(ZoneOffset.UTC),
                    publishedAt == null
                            ? null
                            : publishedAt.atOffset(ZoneOffset.UTC));
        }
    }

    private void seedProcessedEvents(int count, Instant processedAt) {
        for (int i = 0; i < count; i++) {
            jdbcTemplate.update("""
                    INSERT INTO processed_events
                        (event_id, event_type, processed_at)
                    VALUES (?, 'ORDER_CONFIRMED', ?)
                    """,
                    UUID.randomUUID(),
                    processedAt.atOffset(ZoneOffset.UTC));
        }
    }

    private int countOutbox() {
        return jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM outbox_events", Integer.class);
    }

    private int countUnpublishedOutbox() {
        return jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM outbox_events WHERE published_at IS NULL",
                Integer.class);
    }

    private int countProcessedEvents() {
        return jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM processed_events", Integer.class);
    }
}
