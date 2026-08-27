package com.ahmetkeles.inventoryservice.retention;

import com.ahmetkeles.inventoryservice.inventory.ProcessedEventRepository;
import com.ahmetkeles.inventoryservice.outbox.OutboxEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.function.IntSupplier;

/**
 * Scheduled bounded retention for published outbox rows and processed-event
 * rows.
 *
 * <p>Every delete statement is capped by {@code app.retention.batch-size} and
 * a run issues at most {@code app.retention.max-batches-per-run} batches per
 * table, so no run can hold long locks or scan without bound — a backlog
 * larger than one run's cap drains across successive runs. Each batch runs in
 * its own transaction ({@code REQUIRES_NEW} on the repository methods) and
 * claims its rows with {@code FOR UPDATE SKIP LOCKED}, so multiple replicas
 * running cleanup concurrently partition the eligible rows between themselves
 * rather than contending, and never touch rows the outbox publisher has
 * claimed (its locks cover only unpublished rows, which retention cannot
 * reach).
 *
 * <p>Deleting a processed-event row re-opens idempotency for that event id;
 * the retention ages and their relationship to the redelivery horizon are
 * documented in EVENT_FLOW.md, "Retention".
 */
@Component
@EnableConfigurationProperties(RetentionProperties.class)
@ConditionalOnProperty(
        name = "app.retention.enabled",
        havingValue = "true",
        matchIfMissing = true
)
public class RetentionCleanupJob {

    private static final Logger log =
            LoggerFactory.getLogger(RetentionCleanupJob.class);

    private final OutboxEventRepository outboxEventRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final RetentionProperties properties;

    public RetentionCleanupJob(
            OutboxEventRepository outboxEventRepository,
            ProcessedEventRepository processedEventRepository,
            RetentionProperties properties
    ) {
        this.outboxEventRepository = outboxEventRepository;
        this.processedEventRepository = processedEventRepository;
        this.properties = properties;
    }

    @Scheduled(
            fixedDelayString = "${app.retention.cleanup-interval-ms:3600000}",
            initialDelayString = "${app.retention.cleanup-interval-ms:3600000}"
    )
    public void runCleanup() {
        Instant outboxCutoff =
                Instant.now().minus(properties.getPublishedOutboxMaxAge());
        Instant processedCutoff =
                Instant.now().minus(properties.getProcessedEventsMaxAge());
        int batchSize = properties.getBatchSize();

        int outboxDeleted = drain(
                () -> outboxEventRepository.deletePublishedBatchOlderThan(
                        outboxCutoff,
                        batchSize
                )
        );

        int processedDeleted = drain(
                () -> processedEventRepository.deleteBatchOlderThan(
                        processedCutoff,
                        batchSize
                )
        );

        if (outboxDeleted > 0 || processedDeleted > 0) {
            log.info(
                    "Retention cleanup deleted {} published outbox row(s) older than {} and {} processed event(s) older than {}",
                    outboxDeleted,
                    properties.getPublishedOutboxMaxAge(),
                    processedDeleted,
                    properties.getProcessedEventsMaxAge()
            );
        }
    }

    /**
     * Runs bounded delete batches until one comes back short (nothing left)
     * or the per-run cap is reached.
     */
    private int drain(IntSupplier deleteBatch) {
        int total = 0;

        for (int i = 0; i < properties.getMaxBatchesPerRun(); i++) {
            int deleted = deleteBatch.getAsInt();
            total += deleted;

            if (deleted < properties.getBatchSize()) {
                break;
            }
        }

        return total;
    }
}
