package com.ahmetkeles.orderservice.service;

import com.ahmetkeles.orderservice.domain.Order;
import com.ahmetkeles.orderservice.domain.OrderItem;
import com.ahmetkeles.orderservice.domain.OrderStatus;
import com.ahmetkeles.orderservice.outbox.OutboxEvent;
import com.ahmetkeles.orderservice.outbox.OutboxEventRepository;
import com.ahmetkeles.orderservice.outbox.event.OrderCancelledEvent;
import com.ahmetkeles.orderservice.outbox.event.OrderConfirmedEvent;
import com.ahmetkeles.orderservice.outbox.event.OrderCreatedEvent;
import com.ahmetkeles.orderservice.outbox.event.OrderItemAddedEvent;
import com.ahmetkeles.orderservice.repository.OrderRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.util.UUID;

@Service
public class OrderService {

    private static final String AGGREGATE_TYPE = "Order";
    private static final String ORDER_CREATED = "ORDER_CREATED";
    private static final String ORDER_ITEM_ADDED = "ORDER_ITEM_ADDED";
    private static final String ORDER_CANCELLED = "ORDER_CANCELLED";
    private static final String ORDER_CONFIRMED = "ORDER_CONFIRMED";

    private final OrderRepository orderRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    public OrderService(
            OrderRepository orderRepository,
            OutboxEventRepository outboxEventRepository,
            ObjectMapper objectMapper
    ) {
        this.orderRepository = orderRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public Order createOrder(UUID customerId, String currency) {
        Order order = orderRepository.save(new Order(customerId, currency));

        OrderCreatedEvent event = new OrderCreatedEvent(
                order.getId(),
                order.getCustomerId(),
                order.getCurrency(),
                order.getStatus()
        );

        outboxEventRepository.save(new OutboxEvent(
                AGGREGATE_TYPE,
                order.getId(),
                ORDER_CREATED,
                serialize(event)
        ));

        return order;
    }

    @Transactional(readOnly = true)
    public Order getOrder(UUID orderId) {
        return findOrderWithItems(orderId);
    }

    /**
     * Marks the order as submitted (assembly finished). If every item is
     * already reserved when the client submits, the same transaction
     * performs PENDING -> CONFIRMED — in which case the ORDER_CONFIRMED
     * outbox row is written here, exactly once: {@code submit() == true}
     * plus a CONFIRMED status identifies the confirming submit, because a
     * previously-confirmed order can never return {@code true} from
     * {@link Order#submit()}. Otherwise the last reservation to arrive
     * confirms later (and emits there). Duplicate submits are no-ops and
     * emit nothing. The submission participates in the aggregate's
     * optimistic locking: a version race with a concurrent addItem or
     * reservation surfaces as an OptimisticLockingFailureException, rolling
     * the outbox row back with it.
     */
    @Transactional
    public Order submitOrder(UUID orderId) {
        Order order = findOrderWithItems(orderId);

        if (order.submit() && order.getStatus() == OrderStatus.CONFIRMED) {
            writeOrderConfirmedEvent(order);
        }

        return order;
    }

    /**
     * The ORDER_CONFIRMED outbox row is written only when this call's item
     * reservation performed the PENDING -> CONFIRMED transition — the
     * explicit result of {@link Order#markItemReserved}, which requires the
     * order to have been explicitly submitted AND every item reserved — in
     * the same transaction as the status change and the payment kickoff.
     * Duplicate, unknown-item, and pre-submission events return
     * {@code false} from the aggregate and emit nothing.
     */
    @Transactional
    public void markItemReserved(UUID orderId, UUID orderItemId) {
        Order order = findOrderWithItems(orderId);

        if (!order.markItemReserved(orderItemId)) {
            return;
        }

        writeOrderConfirmedEvent(order);
    }

    private void writeOrderConfirmedEvent(Order order) {
        OrderConfirmedEvent event = new OrderConfirmedEvent(
                order.getId(),
                order.getCustomerId(),
                order.getTotalAmount(),
                order.getCurrency()
        );

        outboxEventRepository.save(new OutboxEvent(
                AGGREGATE_TYPE,
                order.getId(),
                ORDER_CONFIRMED,
                serialize(event)
        ));
    }

    @Transactional
    public void completePayment(UUID orderId) {
        Order order = findOrderWithItems(orderId);
        order.completePayment();
    }

    /**
     * Applies a failed payment: the payment status moves to FAILED, and when
     * that failure is what kills a CONFIRMED order, the dedicated
     * payment-failure cancellation runs and its ORDER_CANCELLED outbox row is
     * written in the same transaction — the trigger for inventory release.
     * A payment already at a terminal outcome makes the whole call a no-op:
     * COMPLETED never becomes FAILED, and a cancelled order is not
     * re-announced.
     */
    @Transactional
    public void failPayment(UUID orderId) {
        Order order = findOrderWithItems(orderId);

        if (!order.failPayment()) {
            return;
        }

        if (!order.cancelForFailedPayment()) {
            return;
        }

        OrderCancelledEvent event = new OrderCancelledEvent(order.getId());

        outboxEventRepository.save(new OutboxEvent(
                AGGREGATE_TYPE,
                order.getId(),
                ORDER_CANCELLED,
                serialize(event)
        ));
    }

    /**
     * The outbox row is written only when this call performed the
     * PENDING -> CANCELLED transition, in the same transaction as the status
     * change: a duplicate cancellation emits nothing, and a transaction that
     * loses the optimistic-lock race rolls its outbox row back with the
     * order mutation.
     */
    @Transactional
    public void cancelOrder(UUID orderId) {
        Order order = findOrderWithItems(orderId);

        if (!order.cancel()) {
            return;
        }

        OrderCancelledEvent event = new OrderCancelledEvent(order.getId());

        outboxEventRepository.save(new OutboxEvent(
                AGGREGATE_TYPE,
                order.getId(),
                ORDER_CANCELLED,
                serialize(event)
        ));
    }

    @Transactional
    public Order addItem(
            UUID orderId,
            UUID productId,
            int quantity,
            BigDecimal unitPrice
    ) {
        Order order = findOrderWithItems(orderId);
        OrderItem item = order.addItem(productId, quantity, unitPrice);

        OrderItemAddedEvent event = new OrderItemAddedEvent(
                order.getId(),
                item.getId(),
                productId,
                quantity,
                unitPrice,
                order.getTotalAmount()
        );

        outboxEventRepository.save(new OutboxEvent(
                AGGREGATE_TYPE,
                order.getId(),
                ORDER_ITEM_ADDED,
                serialize(event)
        ));

        return order;
    }

    private Order findOrderWithItems(UUID orderId) {
        return orderRepository.findWithItemsById(orderId)
                .orElseThrow(() -> new OrderNotFoundException(orderId));
    }

    private String serialize(Object event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (Exception exception) {
            throw new IllegalStateException(
                    "Failed to serialize outbox event",
                    exception
            );
        }
    }
}
