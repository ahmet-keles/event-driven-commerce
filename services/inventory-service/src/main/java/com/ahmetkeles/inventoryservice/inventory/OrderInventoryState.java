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
 * This service's own record of whether an order may still reserve stock.
 * The row doubles as the per-order lock: reserve and cancel both acquire it
 * with {@code SELECT ... FOR UPDATE} before touching the ledger or the
 * counters, so their decisions are serialized by the database rather than by
 * message ordering.
 */
@Entity
@Table(name = "order_inventory_state")
public class OrderInventoryState {

    @Id
    @Column(name = "order_id", nullable = false)
    private UUID orderId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private OrderInventoryStatus state;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected OrderInventoryState() {
    }

    public boolean isCancelled() {
        return state == OrderInventoryStatus.CANCELLED;
    }

    /**
     * Marks the order cancelled, reporting whether this call performed the
     * transition. An already-cancelled row is left untouched, including its
     * timestamp, so duplicate cancellations cause no churn.
     */
    public boolean markCancelled() {
        if (state == OrderInventoryStatus.CANCELLED) {
            return false;
        }

        state = OrderInventoryStatus.CANCELLED;
        updatedAt = Instant.now();
        return true;
    }

    public UUID getOrderId() {
        return orderId;
    }

    public OrderInventoryStatus getState() {
        return state;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }
}
