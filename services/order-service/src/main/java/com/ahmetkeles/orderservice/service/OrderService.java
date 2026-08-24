package com.ahmetkeles.orderservice.service;

import com.ahmetkeles.orderservice.domain.Order;
import com.ahmetkeles.orderservice.repository.OrderRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.UUID;

@Service
public class OrderService {

    private final OrderRepository orderRepository;

    public OrderService(OrderRepository orderRepository) {
        this.orderRepository = orderRepository;
    }

    @Transactional
    public Order createOrder(UUID customerId, String currency) {
        return orderRepository.save(new Order(customerId, currency));
    }

    @Transactional(readOnly = true)
    public Order getOrder(UUID orderId) {
        return findOrderWithItems(orderId);
    }

    @Transactional
    public Order addItem(UUID orderId, UUID productId, int quantity, BigDecimal unitPrice) {
        Order order = findOrderWithItems(orderId);
        order.addItem(productId, quantity, unitPrice);
        return order;
    }

    private Order findOrderWithItems(UUID orderId) {
        return orderRepository.findWithItemsById(orderId)
                .orElseThrow(() -> new OrderNotFoundException(orderId));
    }
}
