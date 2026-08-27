package com.ahmetkeles.orderservice.messaging;

import com.ahmetkeles.orderservice.PostgreSQLIntegrationTest;
import com.ahmetkeles.orderservice.domain.Order;
import com.ahmetkeles.orderservice.domain.OrderItem;
import com.ahmetkeles.orderservice.domain.OrderStatus;
import com.ahmetkeles.orderservice.outbox.OutboxEventRepository;
import com.ahmetkeles.orderservice.repository.OrderRepository;
import com.ahmetkeles.orderservice.service.OrderService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Proves the durable event-level deduplication contract against a real
 * PostgreSQL: the processed_events claim and the order mutation commit or
 * roll back together, duplicates by eventId never mutate twice, and events
 * without a valid identity never claim.
 */
class InventoryEventIdempotencyIntegrationTest
        extends PostgreSQLIntegrationTest {

    @Autowired
    private InventoryEventsConsumer consumer;

    @Autowired
    private InventoryEventProcessor eventProcessor;

    @Autowired
    private ProcessedEventRepository processedEventRepository;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @MockitoSpyBean
    private OrderService orderService;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void clearDatabase() {
        processedEventRepository.deleteAll();
        outboxEventRepository.deleteAll();
        orderRepository.deleteAll();
    }

    @Test
    void sequentialDuplicateMutatesOnceAndKeepsOneClaim() throws Exception {
        UUID orderId = createOrderWithItems(1);
        UUID itemId = itemIds(orderId).getFirst();
        UUID eventId = UUID.randomUUID();
        String message = reservedMessage(eventId, orderId, itemId);

        consumer.consume(message);

        assertEquals(OrderStatus.CONFIRMED, statusOf(orderId));
        assertEquals(1, processedEventRepository.count());
        ProcessedEvent claim =
                processedEventRepository.findById(eventId).orElseThrow();
        assertEquals("INVENTORY_RESERVED", claim.getEventType());
        assertEquals(orderId, claim.getAggregateId());

        Instant updatedAtAfterFirstDelivery = updatedAtOf(orderId);

        // Redelivery of the identical record (restart/replay included) must
        // not touch the order again.
        assertDoesNotThrow(() -> consumer.consume(message));

        assertEquals(1, processedEventRepository.count());
        assertEquals(OrderStatus.CONFIRMED, statusOf(orderId));
        assertEquals(updatedAtAfterFirstDelivery, updatedAtOf(orderId));
        verify(orderService, times(1)).markItemReserved(orderId, itemId);
    }

    @Test
    void concurrentDuplicateAllowsExactlyOneClaimant() throws Exception {
        UUID orderId = createOrderWithItems(1);
        UUID itemId = itemIds(orderId).getFirst();
        InventoryEventEnvelope envelope = reservedEnvelope(
                UUID.randomUUID(), orderId, itemId);
        InventoryReservedEvent event = new InventoryReservedEvent(
                orderId, itemId, UUID.randomUUID(), 1);

        CyclicBarrier barrier = new CyclicBarrier(2);
        CountDownLatch done = new CountDownLatch(2);
        AtomicInteger claims = new AtomicInteger();
        AtomicInteger failures = new AtomicInteger();

        Runnable delivery = () -> {
            try {
                barrier.await();
                if (eventProcessor.processReserved(envelope, event)) {
                    claims.incrementAndGet();
                }
            } catch (Exception exception) {
                failures.incrementAndGet();
            } finally {
                done.countDown();
            }
        };

        Thread first = new Thread(delivery);
        Thread second = new Thread(delivery);
        first.start();
        second.start();
        assertTrue(done.await(30, java.util.concurrent.TimeUnit.SECONDS));

        assertEquals(0, failures.get());
        assertEquals(1, claims.get());
        assertEquals(1, processedEventRepository.count());
        assertEquals(OrderStatus.CONFIRMED, statusOf(orderId));
        verify(orderService, times(1)).markItemReserved(orderId, itemId);
    }

    @Test
    void failedProcessingRollsBackClaimAndRetrySucceeds() throws Exception {
        UUID orderId = createOrderWithItems(1);
        UUID itemId = itemIds(orderId).getFirst();
        String message = reservedMessage(UUID.randomUUID(), orderId, itemId);

        doThrow(new RuntimeException("simulated transient failure"))
                .doCallRealMethod()
                .when(orderService)
                .markItemReserved(orderId, itemId);

        assertThrows(RuntimeException.class, () -> consumer.consume(message));

        // The claim must roll back with the failed mutation, leaving the
        // event claimable by the redelivery the error handler schedules.
        assertEquals(0, processedEventRepository.count());
        assertEquals(OrderStatus.PENDING, statusOf(orderId));

        assertDoesNotThrow(() -> consumer.consume(message));

        assertEquals(1, processedEventRepository.count());
        assertEquals(OrderStatus.CONFIRMED, statusOf(orderId));
    }

    @Test
    void duplicateReservationFailureCancelsOnce() throws Exception {
        UUID orderId = createOrderWithItems(2);
        String message = failedMessage(UUID.randomUUID(), orderId);

        consumer.consume(message);

        assertEquals(OrderStatus.CANCELLED, statusOf(orderId));
        assertEquals(1, processedEventRepository.count());

        Instant updatedAtAfterCancellation = updatedAtOf(orderId);

        assertDoesNotThrow(() -> consumer.consume(message));

        assertEquals(1, processedEventRepository.count());
        assertEquals(OrderStatus.CANCELLED, statusOf(orderId));
        assertEquals(updatedAtAfterCancellation, updatedAtOf(orderId));
        verify(orderService, times(1)).cancelOrder(orderId);
    }

    @Test
    void invalidIdentityNeverClaims() throws Exception {
        UUID orderId = createOrderWithItems(1);
        UUID itemId = itemIds(orderId).getFirst();

        assertThrows(
                InvalidEventException.class,
                () -> consumer.consume(
                        reservedMessage(null, orderId, itemId)
                )
        );

        assertThrows(
                InvalidEventException.class,
                () -> consumer.consume(envelopeMessage(
                        UUID.randomUUID(),
                        UUID.randomUUID(),
                        "INVENTORY_RESERVED",
                        reservedPayload(orderId, itemId)
                ))
        );

        assertEquals(0, processedEventRepository.count());
        assertEquals(OrderStatus.PENDING, statusOf(orderId));
    }

    @Test
    void unsupportedEventTypeNeverClaims() throws Exception {
        assertDoesNotThrow(() -> consumer.consume(envelopeMessage(
                UUID.randomUUID(),
                UUID.randomUUID(),
                "INVENTORY_RESTOCKED",
                "{}"
        )));

        assertEquals(0, processedEventRepository.count());
    }

    @Test
    void multiItemConfirmationSurvivesDuplicateDeliveries() throws Exception {
        UUID orderId = createOrderWithItems(2);
        List<UUID> itemIds = itemIds(orderId);
        String firstItemMessage = reservedMessage(
                UUID.randomUUID(), orderId, itemIds.get(0));

        consumer.consume(firstItemMessage);
        consumer.consume(firstItemMessage);

        // A duplicated per-item event must not advance the order towards
        // confirmation while the other item is unreserved.
        assertEquals(OrderStatus.PENDING, statusOf(orderId));
        assertEquals(1, processedEventRepository.count());

        consumer.consume(reservedMessage(
                UUID.randomUUID(), orderId, itemIds.get(1)));

        assertEquals(OrderStatus.CONFIRMED, statusOf(orderId));
        assertEquals(2, processedEventRepository.count());
        verify(orderService, times(2))
                .markItemReserved(org.mockito.ArgumentMatchers.eq(orderId),
                        org.mockito.ArgumentMatchers.any());
    }

    private UUID createOrderWithItems(int itemCount) {
        Order order = orderService.createOrder(UUID.randomUUID(), "USD");

        for (int i = 0; i < itemCount; i++) {
            orderService.addItem(
                    order.getId(),
                    UUID.randomUUID(),
                    1,
                    new BigDecimal("9.99")
            );
        }

        orderService.submitOrder(order.getId());

        return order.getId();
    }

    private List<UUID> itemIds(UUID orderId) {
        return orderRepository.findWithItemsById(orderId)
                .orElseThrow()
                .getItems()
                .stream()
                .map(OrderItem::getId)
                .toList();
    }

    private OrderStatus statusOf(UUID orderId) {
        return orderRepository.findWithItemsById(orderId)
                .orElseThrow()
                .getStatus();
    }

    private Instant updatedAtOf(UUID orderId) {
        return orderRepository.findWithItemsById(orderId)
                .orElseThrow()
                .getUpdatedAt();
    }

    private InventoryEventEnvelope reservedEnvelope(
            UUID eventId,
            UUID orderId,
            UUID itemId
    ) {
        return new InventoryEventEnvelope(
                eventId,
                "Order",
                orderId,
                "INVENTORY_RESERVED",
                reservedPayload(orderId, itemId),
                Instant.now()
        );
    }

    private String reservedPayload(UUID orderId, UUID itemId) {
        return objectMapper.writeValueAsString(
                new InventoryReservedEvent(
                        orderId, itemId, UUID.randomUUID(), 1)
        );
    }

    private String reservedMessage(UUID eventId, UUID orderId, UUID itemId) {
        return envelopeMessage(
                eventId,
                orderId,
                "INVENTORY_RESERVED",
                reservedPayload(orderId, itemId)
        );
    }

    private String failedMessage(UUID eventId, UUID orderId) {
        return envelopeMessage(
                eventId,
                orderId,
                "INVENTORY_RESERVATION_FAILED",
                objectMapper.writeValueAsString(
                        new InventoryReservationFailedEvent(
                                orderId,
                                UUID.randomUUID(),
                                UUID.randomUUID(),
                                1,
                                "INSUFFICIENT_INVENTORY"
                        )
                )
        );
    }

    private String envelopeMessage(
            UUID eventId,
            UUID aggregateId,
            String eventType,
            String payload
    ) {
        return objectMapper.writeValueAsString(
                new InventoryEventEnvelope(
                        eventId,
                        "Order",
                        aggregateId,
                        eventType,
                        payload,
                        Instant.now()
                )
        );
    }
}
