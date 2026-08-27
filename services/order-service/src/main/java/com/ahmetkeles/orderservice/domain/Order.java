package com.ahmetkeles.orderservice.domain;

import jakarta.persistence.CascadeType;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

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

    /**
     * Optimistic-locking version for the aggregate root. Wrapper {@code Long}
     * rather than primitive {@code long}: a new instance carries {@code null},
     * which lets Spring Data treat an unsaved entity with an assigned UUID id
     * as new (persist, no merge round-trip) and lets Hibernate distinguish
     * "never persisted" from "version 0".
     */
    @Version
    private Long version;

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

    public OrderItem addItem(UUID productId, int quantity, BigDecimal unitPrice) {
        OrderItem item = new OrderItem(productId, quantity, unitPrice, this);

        items.add(item);
        totalAmount = totalAmount.add(item.subtotal());
        updatedAt = Instant.now();

        return item;
    }

    /**
     * Records that one item of this order has been reserved, confirming the
     * order only once every item is reserved. Reservation state is tracked per
     * item rather than as a count, so a redelivered event for an item that is
     * already reserved cannot advance the order towards confirmation.
     *
     * <p>The aggregate is only mutated when a previously-unreserved matching
     * item is newly marked: a terminal order, an unknown or null item id, and
     * an already-reserved item all leave the order untouched, including
     * {@code updatedAt}.
     */
    public void markItemReserved(UUID orderItemId) {
        if (status != OrderStatus.PENDING) {
            return;
        }

        OrderItem match = items.stream()
                .filter(item -> item.getId().equals(orderItemId))
                .findFirst()
                .orElse(null);

        if (match == null || match.isReserved()) {
            return;
        }

        match.markReserved();
        updatedAt = Instant.now();

        if (allItemsReserved()) {
            status = OrderStatus.CONFIRMED;
        }
    }

    private boolean allItemsReserved() {
        return !items.isEmpty()
                && items.stream().allMatch(OrderItem::isReserved);
    }

    public void cancel() {
        if (status != OrderStatus.PENDING) {
            return;
        }

        status = OrderStatus.CANCELLED;
        updatedAt = Instant.now();
    }

    public BigDecimal getTotalAmount() {
        return totalAmount;
    }

    public UUID getId() {
        return id;
    }

    public UUID getCustomerId() {
        return customerId;
    }

    public OrderStatus getStatus() {
        return status;
    }

    public String getCurrency() {
        return currency;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public Long getVersion() {
        return version;
    }

    public List<OrderItem> getItems() {
        return List.copyOf(items);
    }
}
