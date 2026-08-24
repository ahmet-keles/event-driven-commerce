package com.ahmetkeles.orderservice.repository;

import com.ahmetkeles.orderservice.PostgreSQLIntegrationTest;
import com.ahmetkeles.orderservice.domain.Order;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OrderRepositoryIntegrationTest extends PostgreSQLIntegrationTest {

    @Autowired
    private OrderRepository orderRepository;

    @PersistenceContext
    private EntityManager entityManager;

    @Test
    @Transactional
    void orderAndItemsArePersistedAndLoaded() {
        Order order = new Order(UUID.randomUUID(), "USD");
        order.addItem(UUID.randomUUID(), 2, new BigDecimal("15.00"));
        order.addItem(UUID.randomUUID(), 3, new BigDecimal("4.50"));

        orderRepository.saveAndFlush(order);
        UUID orderId = order.getId();
        entityManager.clear();

        var loadedOrder = orderRepository.findById(orderId);

        assertTrue(loadedOrder.isPresent());
        assertEquals(2, loadedOrder.orElseThrow().getItems().size());
        assertEquals(new BigDecimal("43.50"), loadedOrder.orElseThrow().getTotalAmount());
    }
}
