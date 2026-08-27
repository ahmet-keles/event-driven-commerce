package com.ahmetkeles.inventoryservice.retention;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Bounded retention for the two append-only bookkeeping tables.
 *
 * <p>Only rows whose job is done are ever eligible: published outbox rows
 * (the broker acknowledged them) and processed-event rows older than the
 * deduplication window. Unpublished outbox rows are never deleted at any age.
 */
@ConfigurationProperties(prefix = "app.retention")
public class RetentionProperties {

    /**
     * Minimum age of a published outbox row, measured from {@code published_at},
     * before it may be deleted. Kafka — not the outbox — is the replay log once
     * a row is published, so this only needs to cover operational forensics.
     */
    private Duration publishedOutboxMaxAge = Duration.ofDays(7);

    /**
     * Minimum age of a processed-event row before it may be deleted. This
     * bounds the consumer's deduplication window: a duplicate delivered after
     * its row is gone is processed as new. Keep it longer than any redelivery
     * horizon (broker retention, dead-letter redrive delay).
     */
    private Duration processedEventsMaxAge = Duration.ofDays(14);

    /** Rows deleted per statement — every DELETE carries this LIMIT. */
    private int batchSize = 500;

    /**
     * Batches per table per cleanup run. Together with {@link #batchSize} this
     * caps one run's work; a backlog larger than the cap simply drains over
     * successive runs.
     */
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
