package com.ahmetkeles.inventoryservice.retention;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import org.hibernate.validator.constraints.time.DurationMin;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

/**
 * Bounded retention for the two append-only bookkeeping tables.
 *
 * <p>Only rows whose job is done are ever eligible: published outbox rows
 * (the broker acknowledged them) and processed-event rows older than the
 * deduplication window. Unpublished outbox rows are never deleted at any age.
 *
 * <p>All bounds are validated at startup and the application refuses to boot
 * on a non-positive value. A negative max age would be silently catastrophic
 * rather than merely wrong: {@code now().minus(-d)} places the cutoff in the
 * future, making every published outbox row and every processed-event row
 * eligible in one sweep.
 */
@Validated
@ConfigurationProperties(prefix = "app.retention")
public class RetentionProperties {

    /**
     * Minimum age of a published outbox row, measured from {@code published_at},
     * before it may be deleted. Kafka — not the outbox — is the replay log once
     * a row is published, so this only needs to cover operational forensics.
     */
    @NotNull
    @DurationMin(millis = 1)
    private Duration publishedOutboxMaxAge = Duration.ofDays(7);

    /**
     * Minimum age of a processed-event row before it may be deleted. This
     * bounds the consumer's deduplication window: a duplicate delivered after
     * its row is gone is processed as new.
     *
     * <p>This age MUST exceed the Kafka source topic's retention, plus the
     * worst-case consumer lag, plus any operational replay or dead-letter
     * redrive window. Anyone raising those horizons must raise this in step.
     */
    @NotNull
    @DurationMin(millis = 1)
    private Duration processedEventsMaxAge = Duration.ofDays(30);

    /** Rows deleted per statement — every DELETE carries this LIMIT. */
    @Positive
    private int batchSize = 500;

    /**
     * Batches per table per cleanup run. Together with {@link #batchSize} this
     * caps one run's work; a backlog larger than the cap simply drains over
     * successive runs.
     */
    @Positive
    private int maxBatchesPerRun = 10;

    public Duration getPublishedOutboxMaxAge() {
        return publishedOutboxMaxAge;
    }

    public void setPublishedOutboxMaxAge(Duration publishedOutboxMaxAge) {
        this.publishedOutboxMaxAge = publishedOutboxMaxAge;
    }

    public Duration getProcessedEventsMaxAge() {
        return processedEventsMaxAge;
    }

    public void setProcessedEventsMaxAge(Duration processedEventsMaxAge) {
        this.processedEventsMaxAge = processedEventsMaxAge;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getMaxBatchesPerRun() {
        return maxBatchesPerRun;
    }

    public void setMaxBatchesPerRun(int maxBatchesPerRun) {
        this.maxBatchesPerRun = maxBatchesPerRun;
    }
}
