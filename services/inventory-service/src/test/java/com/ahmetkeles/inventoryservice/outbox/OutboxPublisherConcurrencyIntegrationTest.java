package com.ahmetkeles.inventoryservice.outbox;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

/**
 * Verifies the multi-replica behaviour of the outbox publisher against a real
 * PostgreSQL instance.
 *
 * <p>{@code FOR UPDATE SKIP LOCKED} is a database guarantee, so mocking the
 * repository here would only test the mock. Every claim in this class is
 * exercised through real transactions on real rows, with two threads standing
 * in for two service replicas.
 *
 * <p>Kafka is mocked deliberately: this class is about database locking, and a
 * broker container would add minutes of startup for no additional coverage. The
 * outbox-to-Kafka path has its own coverage in
 * {@link com.ahmetkeles.inventoryservice.KafkaInventoryIntegrationTest}.
 */
@SpringBootTest
class OutboxPublisherConcurrencyIntegrationTest {

    private static final PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:17-alpine");

    static {
        postgres.start();
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);

        // The publisher bean must exist, so it stays enabled and the
        // KafkaTemplate is replaced by a mock below. Nothing here should ever
        // reach out for a broker.
        registry.add("app.outbox.publisher-enabled", () -> "true");
        registry.add("spring.kafka.admin.auto-create", () -> "false");
        registry.add("spring.kafka.listener.auto-startup", () -> "false");

        // Each test drives publishPendingEvents() itself. Push the scheduled
        // poll far out so it cannot claim rows underneath an assertion.
        registry.add("app.outbox.publish-interval-ms", () -> "3600000");

        // Keeps the timeout test quick.
        registry.add("app.outbox.send-timeout-ms", () -> "250");
    }

    @MockitoBean
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private OutboxPublisher publisher;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private PlatformTransactionManager transactionManager;

    private final ConcurrentLinkedQueue<String> sentKeys =
            new ConcurrentLinkedQueue<>();

    private final ConcurrentLinkedQueue<String> sentMessages =
            new ConcurrentLinkedQueue<>();

    @BeforeEach
    void resetState() {
        outboxEventRepository.deleteAll();
        sentKeys.clear();
        sentMessages.clear();

        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenAnswer(invocation -> {
                    sentKeys.add(invocation.getArgument(1));
                    sentMessages.add(invocation.getArgument(2));
                    return CompletableFuture.completedFuture(null);
                });
    }

    // ---------- SKIP LOCKED semantics ----------

    @Test
    void twoConcurrentClaimsNeverOverlap() throws Exception {
        insertPending(4);

        CountDownLatch firstHasClaimed = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);

        ExecutorService workers = Executors.newFixedThreadPool(2);

        try {
            Future<List<UUID>> first = workers.submit(() ->
                    inTransaction(() -> {
                        List<UUID> claimed = claim(2);
                        firstHasClaimed.countDown();
                        awaitQuietly(releaseFirst);
                        return claimed;
                    }));

            assertTrue(
                    firstHasClaimed.await(30, TimeUnit.SECONDS),
                    "first worker never claimed its batch"
            );

            Future<List<UUID>> second = workers.submit(() ->
                    inTransaction(() -> claim(2)));

            // Completing while the first transaction is still open is the
            // whole point: plain FOR UPDATE would block here until timeout.
            List<UUID> secondClaim = second.get(30, TimeUnit.SECONDS);

            releaseFirst.countDown();
            List<UUID> firstClaim = first.get(30, TimeUnit.SECONDS);

            assertEquals(2, firstClaim.size());
            assertEquals(2, secondClaim.size());
            assertTrue(
                    Collections.disjoint(firstClaim, secondClaim),
                    "two replicas claimed the same outbox row: "
                            + firstClaim + " vs " + secondClaim
            );
        } finally {
            releaseFirst.countDown();
            workers.shutdownNow();
        }
    }

    @Test
    void alreadyClaimedRowsAreSkippedRatherThanWaitedFor() throws Exception {
        insertPending(2);

        CountDownLatch firstHasClaimed = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);

        ExecutorService workers = Executors.newFixedThreadPool(2);

        try {
            workers.submit(() -> inTransaction(() -> {
                List<UUID> claimed = claim(10);
                firstHasClaimed.countDown();
                awaitQuietly(releaseFirst);
                return claimed;
            }));

            assertTrue(
                    firstHasClaimed.await(30, TimeUnit.SECONDS),
                    "first worker never claimed its batch"
            );

            Future<List<UUID>> second = workers.submit(() ->
                    inTransaction(() -> claim(10)));

            assertTrue(
                    second.get(30, TimeUnit.SECONDS).isEmpty(),
                    "every row was locked, so the second claim must be empty"
            );
        } finally {
            releaseFirst.countDown();
            workers.shutdownNow();
        }
    }

    // ---------- publisher behaviour ----------

    @Test
    void singlePublisherPublishesAllPendingRows() {
        List<UUID> ids = insertPending(5);

        publisher.publishPendingEvents();

        assertEquals(5, sentKeys.size());
        ids.forEach(id -> assertNotNull(
                publishedAt(id),
                "row " + id + " should have been published"
        ));
    }

    @Test
    void concurrentPublishersSendEachPendingEventOnce() throws Exception {
        List<UUID> ids = insertPending(20);

        CyclicBarrier startTogether = new CyclicBarrier(2);
        ExecutorService workers = Executors.newFixedThreadPool(2);

        try {
            List<Future<?>> runs = new ArrayList<>();

            for (int worker = 0; worker < 2; worker++) {
                runs.add(workers.submit(() -> {
                    startTogether.await(30, TimeUnit.SECONDS);
                    publisher.publishPendingEvents();
                    return null;
                }));
            }

            for (Future<?> run : runs) {
                run.get(60, TimeUnit.SECONDS);
            }
        } finally {
            workers.shutdownNow();
        }

        // Without SKIP LOCKED both replicas claim the same 20 rows and send 40
        // records. The claim is what keeps this at one send per row.
        assertEquals(20, sentKeys.size(), "an event was published twice");
        assertEquals(
                20,
                Set.copyOf(sentKeys).size(),
                "the same aggregate key was sent more than once"
        );

        ids.forEach(id -> assertNotNull(publishedAt(id)));
    }

    @Test
    void successfulSendMarksPublishedAt() {
        UUID id = insertPending(1).getFirst();

        assertNull(publishedAt(id));

        publisher.publishPendingEvents();

        assertNotNull(publishedAt(id));
    }

    @Test
    void failedSendLeavesRowUnpublished() {
        UUID id = insertPending(1).getFirst();

        // doReturn instead of when(...): re-stubbing with when() would
        // invoke the answer installed in resetState and pollute sentKeys.
        doReturn(CompletableFuture.failedFuture(
                new RuntimeException("broker unavailable")
        )).when(kafkaTemplate).send(anyString(), anyString(), anyString());

        publisher.publishPendingEvents();

        assertNull(
                publishedAt(id),
                "a row must never be marked published unless the broker acknowledged it"
        );

        // And it is still claimable, so the next poll retries it.
        assertEquals(1, inTransaction(() -> claim(10)).size());
    }

    /**
     * Documents the accepted at-least-once duplicate: a send that exceeds the
     * local timeout is not cancelled, so if the broker accepts it anyway the
     * row stays pending and is sent a second time on the next poll.
     */
    @Test
    void sendThatTimesOutIsRepublishedAsAnAcceptedDuplicate() {
        UUID id = insertPending(1).getFirst();

        // A future that never completes: the broker took the record but the
        // acknowledgement did not arrive inside send-timeout-ms.
        doAnswer(invocation -> {
            sentKeys.add(invocation.getArgument(1));
            return new CompletableFuture<String>();
        }).when(kafkaTemplate).send(anyString(), anyString(), anyString());

        publisher.publishPendingEvents();

        assertEquals(1, sentKeys.size());
        assertNull(publishedAt(id), "a timed-out send must not mark the row published");

        // Next poll: the broker responds in time and the row settles. The event
        // has now reached Kafka twice, which consumers must tolerate by
        // deduplicating on eventId.
        doAnswer(invocation -> {
            sentKeys.add(invocation.getArgument(1));
            return CompletableFuture.completedFuture(null);
        }).when(kafkaTemplate).send(anyString(), anyString(), anyString());

        publisher.publishPendingEvents();

        assertEquals(2, sentKeys.size(), "the retry is the accepted duplicate");
        assertEquals(1, Set.copyOf(sentKeys).size(), "both sends are the same event");
        assertNotNull(publishedAt(id));
    }

    // ---------- cross-replica ordering guard ----------

    @Test
    void aggregateWithOlderEventLockedElsewhereIsDeferred() throws Exception {
        UUID aggregateId = UUID.randomUUID();
        Instant base = Instant.parse("2026-01-01T00:00:00Z");

        UUID earlier = insertPending(aggregateId, base);
        UUID later = insertPending(aggregateId, base.plusSeconds(10));

        CountDownLatch claimedEarlier = new CountDownLatch(1);
        CountDownLatch releaseEarlier = new CountDownLatch(1);

        ExecutorService worker = Executors.newSingleThreadExecutor();

        try {
            Future<List<UUID>> otherReplica = worker.submit(() ->
                    inTransaction(() -> {
                        // Oldest pending row first, so this claims `earlier`.
                        List<UUID> claimed = claim(1);
                        claimedEarlier.countDown();
                        awaitQuietly(releaseEarlier);
                        return claimed;
                    }));

            assertTrue(
                    claimedEarlier.await(30, TimeUnit.SECONDS),
                    "the other replica never claimed the earlier event"
            );

            publisher.publishPendingEvents();

            assertTrue(
                    sentKeys.isEmpty(),
                    "the later event must not overtake the earlier one held by another replica"
            );
            assertNull(publishedAt(later));

            releaseEarlier.countDown();
            assertEquals(
                    List.of(earlier),
                    otherReplica.get(30, TimeUnit.SECONDS)
            );
        } finally {
            releaseEarlier.countDown();
            worker.shutdownNow();
        }

        // The other replica released without publishing. This publisher now
        // holds the whole aggregate and must emit it in order.
        publisher.publishPendingEvents();

        List<String> messages = List.copyOf(sentMessages);

        assertEquals(2, messages.size());
        assertTrue(messages.get(0).contains(earlier.toString()));
        assertTrue(messages.get(1).contains(later.toString()));
        assertNotNull(publishedAt(earlier));
        assertNotNull(publishedAt(later));
    }

    // ---------- bounded lock hold ----------

    @Test
    void sendTimeoutAbortsTheRestOfThePoll() {
        insertPending(3);

        // Futures that never complete: the broker is degraded, every send
        // would burn the full local timeout.
        doAnswer(invocation -> {
            sentKeys.add(invocation.getArgument(1));
            return new CompletableFuture<String>();
        }).when(kafkaTemplate).send(anyString(), anyString(), anyString());

        publisher.publishPendingEvents();

        assertEquals(
                1,
                sentKeys.size(),
                "after one local timeout the poll must stop, not spend a timeout per remaining row"
        );

        // Nothing was published and everything is claimable again.
        assertEquals(3, inTransaction(() -> claim(10)).size());
    }

    @Test
    void pollDeadlineStopsSendingButKeepsRowsClaimable() {
        insertPending(3);

        // A deadline of zero expires before the first send; the claimed rows
        // must simply return to the pool at commit.
        OutboxPublisher zeroDeadlinePublisher = new OutboxPublisher(
                outboxEventRepository,
                kafkaTemplate,
                new ObjectMapper(),
                "inventory.events",
                25,
                250,
                0
        );

        // A manual instance has no @Transactional proxy, so supply the
        // transaction the claim query requires.
        inTransaction(() -> {
            zeroDeadlinePublisher.publishPendingEvents();
            return null;
        });

        assertTrue(sentKeys.isEmpty(), "no send may start after the deadline");
        assertEquals(3, inTransaction(() -> claim(10)).size());
    }

    // ---------- ordering ----------

    @Test
    void pendingRowsAreClaimedInOccurredAtOrder() {
        Instant base = Instant.parse("2026-01-01T00:00:00Z");
        UUID aggregateId = UUID.randomUUID();

        UUID third = insertPending(aggregateId, base.plusSeconds(30));
        UUID first = insertPending(aggregateId, base);
        UUID second = insertPending(aggregateId, base.plusSeconds(10));

        List<UUID> claimed = inTransaction(() -> claim(10));

        assertEquals(List.of(first, second, third), claimed);
    }

    @Test
    void claimIsCappedAtTheRequestedBatchSize() {
        insertPending(5);

        assertEquals(2, inTransaction(() -> claim(2)).size());
    }

    // ---------- helpers ----------

    private List<UUID> claim(int batchSize) {
        return outboxEventRepository.lockPendingEvents(batchSize)
                .stream()
                .map(OutboxEvent::getId)
                .toList();
    }

    private <T> T inTransaction(TransactionalWork<T> work) {
        return new TransactionTemplate(transactionManager)
                .execute(status -> work.run());
    }

    @FunctionalInterface
    private interface TransactionalWork<T> {
        T run();
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }

    private List<UUID> insertPending(int count) {
        Instant base = Instant.parse("2026-01-01T00:00:00Z");
        List<UUID> ids = new ArrayList<>();

        for (int index = 0; index < count; index++) {
            ids.add(insertPending(
                    UUID.randomUUID(),
                    base.plusSeconds(index)
            ));
        }

        return ids;
    }

    private UUID insertPending(UUID aggregateId, Instant occurredAt) {
        UUID id = UUID.randomUUID();

        jdbcTemplate.update(
                """
                INSERT INTO outbox_events (
                    id, aggregate_type, aggregate_id,
                    event_type, payload, occurred_at, published_at
                )
                VALUES (?, ?, ?, ?, ?, ?, NULL)
                """,
                id,
                "Order",
                aggregateId,
                "INVENTORY_RESERVED",
                "{\"orderId\":\"" + aggregateId + "\",\"quantity\":3}",
                OffsetDateTime.ofInstant(occurredAt, ZoneOffset.UTC)
        );

        return id;
    }

    private Instant publishedAt(UUID id) {
        OffsetDateTime value = jdbcTemplate.queryForObject(
                "SELECT published_at FROM outbox_events WHERE id = ?",
                OffsetDateTime.class,
                id
        );

        return value == null ? null : value.toInstant();
    }
}
