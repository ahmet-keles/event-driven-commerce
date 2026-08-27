package com.ahmetkeles.inventoryservice.inventory;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

/**
 * One successfully reserved order item. This ledger — not any event payload —
 * is the source of truth for what a cancellation must release: the order item
 * id is the durable correlation key (an order may hold several items for the
 * same product), and the row's status carries whether the held stock has been
 * returned.
 */
@Entity
@Table(name = "inventory_reservations")
public class InventoryReservation {

    @Id
    @Column(name = "order_item_id", nullable = false)
    private UUID orderItemId;

    @Column(name = "order_id", nullable = false)
    private UUID orderId;

    @Column(name = "product_id", nullable = false)
    private UUID productId;

    @Column(nullable = false)
    private int quantity;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private ReservationStatus status;

    @Column(name = "source_event_id", nullable = false)
    private UUID sourceEventId;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected InventoryReservation() {
    }

    public InventoryReservation(
            UUID orderItemId,
            UUID orderId,
            UUID productId,
            int quantity,
            UUID sourceEventId
    ) {
        if (orderItemId == null) {
            throw new IllegalArgumentException("orderItemId is required");
        }

        if (orderId == null) {
            throw new IllegalArgumentException("orderId is required");
        }

        if (productId == null) {
            throw new IllegalArgumentException("productId is required");
        }

        if (quantity <= 0) {
            throw new IllegalArgumentException(
                    "quantity must be greater than zero"
            );
        }

        if (sourceEventId == null) {
            throw new IllegalArgumentException("sourceEventId is required");
        }

        this.orderItemId = orderItemId;
        this.orderId = orderId;
        this.productId = productId;
        this.quantity = quantity;
        this.status = ReservationStatus.RESERVED;
        this.sourceEventId = sourceEventId;
        this.createdAt = Instant.now();
        this.updatedAt = Instant.now();
    }

    /** Idempotent: an already released row is left untouched. */
    public void release() {
        if (status == ReservationStatus.RELEASED) {
            return;
        }

        status = ReservationStatus.RELEASED;
        updatedAt = Instant.now();
    }

    public UUID getOrderItemId() {
        return orderItemId;
    }

    public UUID getOrderId() {
        return orderId;
    }

    public UUID getProductId() {
        return productId;
    }

    public int getQuantity() {
        return quantity;
    }

    public ReservationStatus getStatus() {
        return status;
    }

    public UUID getSourceEventId() {
        return sourceEventId;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }
}
