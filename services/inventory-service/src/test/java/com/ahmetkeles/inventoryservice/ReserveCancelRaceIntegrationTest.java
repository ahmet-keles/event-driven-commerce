package com.ahmetkeles.inventoryservice;

import com.ahmetkeles.inventoryservice.inventory.InventoryItem;
import com.ahmetkeles.inventoryservice.inventory.InventoryItemRepository;
import com.ahmetkeles.inventoryservice.inventory.InventoryReservationRepository;
import com.ahmetkeles.inventoryservice.inventory.InventoryReservationService;
import com.ahmetkeles.inventoryservice.inventory.OrderInventoryStateRepository;
import com.ahmetkeles.inventoryservice.inventory.OrderInventoryStatus;
import com.ahmetkeles.inventoryservice.inventory.ProcessedEventRepository;
import com.ahmetkeles.inventoryservice.inventory.ReservationStatus;
import com.ahmetkeles.inventoryservice.outbox.OutboxEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Forces a real reserve-vs-cancel race on the same order: two threads, two
 * database transactions, released into the service simultaneously by a
 * barrier. The per-order row lock — not Kafka ordering, not sleeps — decides
 * the interleaving, and both orderings must converge to the same end state:
 * the order cancelled, no stock held, no reservation left RESERVED.
 */
class ReserveCancelRaceIntegrationTest extends PostgreSQLIntegrationTest {

    private static final int TIMEOUT_SECONDS = 30;

    @Autowired
    private InventoryReservationService reservationService;

    @Autowired
    private InventoryItemRepository inventoryItemRepository;

    @Autowired
    private InventoryReservationRepository reservationRepository;

    @Autowired
    private OrderInventoryStateRepository orderInventoryStateRepository;

    @Autowired
    private ProcessedEventRepository processedEventRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @BeforeEach
    void cleanDatabase() {
        outboxEventRepository.deleteAll();
        processedEventRepository.deleteAll();
        reservationRepository.deleteAll();
        orderInventoryStateRepository.deleteAll();
        inventoryItemRepository.deleteAll();
    }

    @Test
    void concurrentReserveAndCancelConvergeWithoutLeakingStock()
            throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID orderItemId = UUID.randomUUID();
        UUID productId = UUID.randomUUID();
        inventoryItemRepository.saveAndFlush(
                new InventoryItem(productId, 10));

        CyclicBarrier start = new CyclicBarrier(2);
        ExecutorService executor = Executors.newFixedThreadPool(2);

        try {
            Future<Throwable> reserveResult = executor.submit(() -> {
                try {
                    start.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    reservationService.reserve(
                            UUID.randomUUID(),
                            "ORDER_ITEM_ADDED",
                            orderId,
                            orderItemId,
                            productId,
                            3
                    );
                    return null;
                } catch (Throwable throwable) {
                    return throwable;
                }
            });

            Future<Throwable> cancelResult = executor.submit(() -> {
                try {
                    start.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    reservationService.releaseForCancelledOrder(
                            UUID.randomUUID(),
                            "ORDER_CANCELLED",
                            orderId
                    );
                    return null;
                } catch (Throwable throwable) {
                    return throwable;
                }
            });

            assertNull(reserveResult.get(TIMEOUT_SECONDS, TimeUnit.SECONDS),
                    "reserve must not fail: the lock serializes it");
            assertNull(cancelResult.get(TIMEOUT_SECONDS, TimeUnit.SECONDS),
                    "cancel must not fail: the lock serializes it");
        } finally {
            executor.shutdownNow();
        }

        // Whichever transaction won the lock, the converged state is the
        // same. Reserve first: the reservation was created, then released by
        // the cancellation. Cancel first: the late reserve saw CANCELLED and
        // created nothing.
        InventoryItem item = inventoryItemRepository
                .findById(productId).orElseThrow();
        assertEquals(10, item.getAvailableQuantity(),
                "all stock must be back in the available pool");
        assertEquals(0, item.getReservedQuantity(),
                "no reservation may survive the cancellation");

        assertEquals(OrderInventoryStatus.CANCELLED,
                orderInventoryStateRepository.findById(orderId)
                        .orElseThrow().getState());

        List<ReservationStatus> statuses = reservationRepository
                .findAll().stream()
                .map(reservation -> reservation.getStatus())
                .toList();
        assertTrue(statuses.stream()
                        .noneMatch(status -> status == ReservationStatus.RESERVED),
                "no ledger row may be left RESERVED, found: " + statuses);
    }
}
