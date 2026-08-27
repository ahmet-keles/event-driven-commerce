package com.ahmetkeles.orderservice.retention;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Bounded retention policy for rows whose job is done: published outbox
 * events and processed-event ledger entries. Unpublished outbox rows are
 * never eligible, whatever their age — they are undelivered work, not
 * history.
 */
@ConfigurationProperties(prefix = "app.retention")
public class RetentionProperties {

    /**
     * Minimum age of a published outbox row before it may be deleted,
     * measured from {@code published_at}.
     */
    private Duration outboxMaxAge = Duration.ofDays(7);

    /**
     * Minimum age of a processed-event row before it may be deleted, measured
     * from {@code processed_at}. Must comfortably exceed the longest plausible
     * redelivery window (consumer retries, DLT replays, offset rewinds):
     * deleting a ledger row early re-opens the door to a duplicate mutation.
     */
    private Duration processedEventsMaxAge = Duration.ofDays(7);

    /** Rows deleted per transaction. */
    private int batchSize = 500;

    /**
     * Upper bound on batches per table per run, so one scheduled tick can
     * never turn into an unbounded delete however large the backlog. A
     * backlog beyond {@code batchSize * maxBatchesPerRun} simply drains over
     * the following runs.
     */
    private int maxBatchesPerRun = 10;

    public Duration getOutboxMaxAge() {
        return outboxMaxAge;
    }

    public void setOutboxMaxAge(Duration outboxMaxAge) {
        this.outboxMaxAge = outboxMaxAge;
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
