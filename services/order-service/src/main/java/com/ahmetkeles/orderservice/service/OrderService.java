package com.ahmetkeles.orderservice.service;

import com.ahmetkeles.orderservice.domain.Order;
import com.ahmetkeles.orderservice.outbox.OutboxEvent;
import com.ahmetkeles.orderservice.outbox.OutboxEventRepository;
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

    @Transactional
    public void confirmOrder(UUID orderId) {
        Order order = findOrderWithItems(orderId);
        order.confirm();
    }

    @Transactional
    public Order addItem(
            UUID orderId,
            UUID productId,
            int quantity,
            BigDecimal unitPrice
    ) {
        Order order = findOrderWithItems(orderId);
        order.addItem(productId, quantity, unitPrice);

        OrderItemAddedEvent event = new OrderItemAddedEvent(
                order.getId(),
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
