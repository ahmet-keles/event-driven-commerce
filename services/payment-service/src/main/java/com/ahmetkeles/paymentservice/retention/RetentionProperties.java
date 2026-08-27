package com.ahmetkeles.paymentservice.retention;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import org.hibernate.validator.constraints.time.DurationMin;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

/**
 * Bounded retention policy for rows whose job is done: published outbox
 * events and processed-event ledger entries. Unpublished outbox rows are
 * never eligible, whatever their age — they are undelivered work, not
 * history.
 *
 * <p>All bounds are validated at startup and the application refuses to boot
 * on a non-positive value. A negative max age would be silently catastrophic
 * rather than merely wrong: {@code now().minus(-d)} places the cutoff in the
 * future, making every published outbox row and every ledger row eligible in
 * one sweep.
 */
@Validated
@ConfigurationProperties(prefix = "app.retention")
public class RetentionProperties {

    /**
     * Minimum age of a published outbox row before it may be deleted,
     * measured from {@code published_at}.
     */
    @NotNull
    @DurationMin(millis = 1)
    private Duration outboxMaxAge = Duration.ofDays(7);

    /**
     * Minimum age of a processed-event row before it may be deleted, measured
     * from {@code processed_at}.
     *
     * <p>This ledger is not archival history: a deleted row re-enables its
     * eventId, so an old event redelivered after deletion would be applied
     * again. The retention age must therefore ALWAYS exceed the Kafka source
     * topic's retention plus the maximum operational replay window — consumer
     * retries, DLT redrives, offset rewinds. Anyone raising those horizons
     * must raise this in step.
     */
    @NotNull
    @DurationMin(millis = 1)
    private Duration processedEventsMaxAge = Duration.ofDays(30);

    /** Rows deleted per transaction. */
    @Positive
    private int batchSize = 500;

    /**
     * Upper bound on batches per table per run, so one scheduled tick can
     * never turn into an unbounded delete however large the backlog. A
     * backlog beyond {@code batchSize * maxBatchesPerRun} simply drains over
     * the following runs.
     */
    @Positive
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
