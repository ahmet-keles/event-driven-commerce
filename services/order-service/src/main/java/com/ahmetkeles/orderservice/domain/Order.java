package com.ahmetkeles.orderservice.domain;

import jakarta.persistence.CascadeType;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Entity
@Table(name = "orders")
public class Order {

    @Id
    private UUID id;

    private UUID customerId;

    @Enumerated(EnumType.STRING)
    private OrderStatus status;

    private BigDecimal totalAmount;

    private String currency;

    private Instant createdAt;

    private Instant updatedAt;

    @OneToMany(mappedBy = "order", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<OrderItem> items = new ArrayList<>();

    protected Order() {
    }

    public Order(UUID customerId, String currency) {
        if (customerId == null) {
            throw new IllegalArgumentException("customerId is required");
        }

        if (currency == null || currency.isBlank()) {
            throw new IllegalArgumentException("currency is required");
        }

        this.id = UUID.randomUUID();
        this.customerId = customerId;
        this.status = OrderStatus.PENDING;
        this.totalAmount = BigDecimal.ZERO;
        this.currency = currency;
        this.createdAt = Instant.now();
        this.updatedAt = Instant.now();
    }
}
