package com.ahmetkeles.paymentservice.retention;

import com.ahmetkeles.paymentservice.payment.ProcessedEventRepository;
import com.ahmetkeles.paymentservice.outbox.OutboxEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.function.IntSupplier;

/**
 * Deletes rows whose job is done, in small bounded batches:
 *
 * <ul>
 * <li><b>outbox_events</b> — only rows with {@code published_at} set and
 *     older than the retention age. Unpublished rows are pending deliveries
 *     and are never touched, regardless of age.</li>
 * <li><b>processed_events</b> — ledger rows older than the retention age,
 *     which must outlive the longest plausible redelivery window.</li>
 * </ul>
 *
 * <p>Each batch runs in its own short transaction and claims its victims with
 * {@code FOR UPDATE SKIP LOCKED}, so concurrent replicas partition the
 * eligible rows between themselves instead of blocking on or double-scanning
 * the same ones, and a retention batch never waits on the publisher (which
 * only ever locks unpublished rows — the two lock disjoint sets by
 * construction). A run deletes at most
 * {@code batchSize * maxBatchesPerRun} rows per table, so a large backlog
 * drains across runs instead of producing one unbounded delete.
 */
@Component
@ConditionalOnProperty(
        name = "app.retention.enabled",
        havingValue = "true",
        matchIfMissing = true
)
@EnableConfigurationProperties(RetentionProperties.class)
public class RetentionJob {

    private static final Logger log = LoggerFactory.getLogger(RetentionJob.class);

    private final OutboxEventRepository outboxEventRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final TransactionTemplate transactionTemplate;
    private final RetentionProperties properties;

    public RetentionJob(
            OutboxEventRepository outboxEventRepository,
            ProcessedEventRepository processedEventRepository,
            PlatformTransactionManager transactionManager,
            RetentionProperties properties
    ) {
        this.outboxEventRepository = outboxEventRepository;
        this.processedEventRepository = processedEventRepository;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.properties = properties;
    }

    @Scheduled(fixedDelayString = "${app.retention.interval-ms:300000}")
    public void run() {
        purgeExpired();
    }

    /** Runs one bounded purge over both tables; returns what was deleted. */
    public RetentionResult purgeExpired() {
        Instant now = Instant.now();
        Instant outboxCutoff = now.minus(properties.getOutboxMaxAge());
        Instant processedCutoff = now.minus(properties.getProcessedEventsMaxAge());
        int batchSize = properties.getBatchSize();

        int outboxDeleted = purgeInBatches(() ->
                outboxEventRepository.deletePublishedBatch(outboxCutoff, batchSize));
        int processedDeleted = purgeInBatches(() ->
                processedEventRepository.deleteProcessedBatch(processedCutoff, batchSize));

        if (outboxDeleted > 0 || processedDeleted > 0) {
            log.info(
                    "Retention removed {} published outbox event(s) older than {} "
                            + "and {} processed event(s) older than {}",
                    outboxDeleted,
                    outboxCutoff,
                    processedDeleted,
                    processedCutoff
            );
        }

        return new RetentionResult(outboxDeleted, processedDeleted);
    }

    /**
     * Deletes one batch per transaction until a batch comes up short (no more
     * eligible rows) or the per-run cap is reached.
     */
    private int purgeInBatches(IntSupplier deleteBatch) {
        int total = 0;

        for (int batch = 0; batch < properties.getMaxBatchesPerRun(); batch++) {
            Integer deleted = transactionTemplate.execute(
                    status -> deleteBatch.getAsInt());

            total += deleted;

            if (deleted < properties.getBatchSize()) {
                break;
            }
        }

        return total;
    }

    public record RetentionResult(int outboxDeleted, int processedEventsDeleted) {
    }
}
