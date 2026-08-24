package com.ahmetkeles.orderservice.domain;

import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;

import java.math.BigDecimal;
import java.util.UUID;

@Entity
@Table(name = "order_items")
public class OrderItem {

    @Id
    private UUID id;

    private UUID productId;

    private int quantity;

    private BigDecimal unitPrice;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "order_id", nullable = false)
    private Order order;

    protected OrderItem() {
    }

    public OrderItem(UUID productId, int quantity, BigDecimal unitPrice, Order order) {
        if (productId == null) {
            throw new IllegalArgumentException("productId is required");
        }

        if (quantity <= 0) {
            throw new IllegalArgumentException("quantity must be greater than 0");
        }

        if (unitPrice == null || unitPrice.signum() < 0) {
            throw new IllegalArgumentException(
                    "unitPrice must be greater than or equal to 0");
        }

        if (order == null) {
            throw new IllegalArgumentException("order is required");
        }

        this.id = UUID.randomUUID();
        this.productId = productId;
        this.quantity = quantity;
        this.unitPrice = unitPrice;
        this.order = order;
    }

    public BigDecimal subtotal() {
        return unitPrice.multiply(BigDecimal.valueOf(quantity));
    }
}
