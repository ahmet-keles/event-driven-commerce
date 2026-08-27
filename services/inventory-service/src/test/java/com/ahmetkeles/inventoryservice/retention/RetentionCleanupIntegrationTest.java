package com.ahmetkeles.inventoryservice.retention;

import com.ahmetkeles.inventoryservice.PostgreSQLIntegrationTest;
import com.ahmetkeles.inventoryservice.inventory.ProcessedEvent;
import com.ahmetkeles.inventoryservice.inventory.ProcessedEventRepository;
import com.ahmetkeles.inventoryservice.outbox.OutboxEvent;
import com.ahmetkeles.inventoryservice.outbox.OutboxEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Drives {@link RetentionCleanupJob} directly against real PostgreSQL with
 * the default retention ages (7d published outbox, 30d processed events) and
 * a deliberately tiny batch configuration so bounding is observable.
 */
class RetentionCleanupIntegrationTest extends PostgreSQLIntegrationTest {

    @DynamicPropertySource
    static void retentionProperties(DynamicPropertyRegistry registry) {
        // The job is driven explicitly; the scheduler must not race the tests.
        registry.add("app.scheduling.enabled", () -> "false");
        registry.add("app.retention.batch-size", () -> "2");
        registry.add("app.retention.max-batches-per-run", () -> "2");
    }

    @Autowired
    private RetentionCleanupJob retentionCleanupJob;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private ProcessedEventRepository processedEventRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private DataSource dataSource;

    @BeforeEach
    void cleanDatabase() {
        outboxEventRepository.deleteAll();
        processedEventRepository.deleteAll();
    }

    @Test
    void deletesOldPublishedOutboxRowsAndKeepsRecentOnes() {
        UUID oldPublished = insertOutboxRow(ageDays(30), true);
        UUID recentPublished = insertOutboxRow(Duration.ZERO, true);

        retentionCleanupJob.runCleanup();

        assertFalse(outboxEventRepository.existsById(oldPublished));
        assertTrue(outboxEventRepository.existsById(recentPublished));
    }

    @Test
    void neverDeletesUnpublishedOutboxRowsAtAnyAge() {
        UUID ancientUnpublished = insertOutboxRow(ageDays(365), false);
        UUID oldPublished = insertOutboxRow(ageDays(365), true);

        retentionCleanupJob.runCleanup();

        assertTrue(
                outboxEventRepository.existsById(ancientUnpublished),
                "an unpublished row must survive retention regardless of age"
        );
        assertFalse(outboxEventRepository.existsById(oldPublished));
    }

    @Test
    void deletesOldProcessedEventsAndKeepsRecentOnes() {
        UUID oldEvent = insertProcessedEvent(ageDays(60));
        UUID recentEvent = insertProcessedEvent(Duration.ZERO);

        retentionCleanupJob.runCleanup();

        assertFalse(processedEventRepository.existsById(oldEvent));
        assertTrue(processedEventRepository.existsById(recentEvent));
    }

    @Test
    void oneRunDeletesAtMostBatchSizeTimesMaxBatchesPerTable() {
        for (int i = 0; i < 7; i++) {
            insertOutboxRow(ageDays(30), true);
        }

        // batch-size 2 x max-batches-per-run 2 = at most 4 rows per run.
        retentionCleanupJob.runCleanup();
        assertEquals(3, outboxEventRepository.count());

        retentionCleanupJob.runCleanup();
        assertEquals(0, outboxEventRepository.count());
    }

    @Test
    void rowsLockedByAnotherTransactionAreSkippedNotWaitedOn()
            throws Exception {
        UUID lockedRow = insertOutboxRow(ageDays(30), true);
        UUID freeRow = insertOutboxRow(ageDays(30), true);

        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);

            try (PreparedStatement lock = connection.prepareStatement(
                    "SELECT id FROM outbox_events WHERE id = ? FOR UPDATE"
            )) {
                lock.setObject(1, lockedRow);
                lock.executeQuery();

                // The competing lock is held for the whole cleanup run; the
                // job must skip the locked row rather than block on it.
                retentionCleanupJob.runCleanup();
            } finally {
                connection.rollback();
            }
        }

        assertTrue(outboxEventRepository.existsById(lockedRow));
        assertFalse(outboxEventRepository.existsById(freeRow));

        // With the lock released, the skipped row is eligible again.
        retentionCleanupJob.runCleanup();
        assertFalse(outboxEventRepository.existsById(lockedRow));
    }

    private Duration ageDays(int days) {
        return Duration.ofDays(days);
    }

    private UUID insertOutboxRow(Duration age, boolean published) {
        OutboxEvent event = new OutboxEvent(
                "Order",
                UUID.randomUUID(),
                "INVENTORY_RESERVED",
                "{\"quantity\":1}"
        );
        outboxEventRepository.saveAndFlush(event);

        Instant occurredAt = Instant.now().minus(age);
        Instant publishedAt = published
                ? occurredAt.plusSeconds(1)
                : null;

        // Entity timestamps are always "now"; retention eligibility is a
        // function of stored time, so age the row directly in the database.
        jdbcTemplate.update(
                "UPDATE outbox_events SET occurred_at = ?, published_at = ? WHERE id = ?",
                java.sql.Timestamp.from(occurredAt),
                publishedAt == null ? null : java.sql.Timestamp.from(publishedAt),
                event.getId()
        );

        return event.getId();
    }

    private UUID insertProcessedEvent(Duration age) {
        ProcessedEvent event = new ProcessedEvent(
                UUID.randomUUID(),
                "ORDER_ITEM_ADDED"
        );
        processedEventRepository.saveAndFlush(event);

        jdbcTemplate.update(
                "UPDATE processed_events SET processed_at = ? WHERE event_id = ?",
                java.sql.Timestamp.from(Instant.now().minus(age)),
                event.getEventId()
        );

        return event.getEventId();
    }
}
