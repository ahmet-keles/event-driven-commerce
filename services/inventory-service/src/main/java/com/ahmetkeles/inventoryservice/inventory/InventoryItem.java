package com.ahmetkeles.inventoryservice.inventory;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "inventory_items")
public class InventoryItem {

    @Id
    @Column(name = "product_id", nullable = false)
    private UUID productId;

    @Column(name = "available_quantity", nullable = false)
    private int availableQuantity;

    @Column(name = "reserved_quantity", nullable = false)
    private int reservedQuantity;

    @Version
    @Column(name = "version", nullable = false)
    private long version;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected InventoryItem() {
    }

    public InventoryItem(UUID productId, int availableQuantity) {
        if (productId == null) {
            throw new IllegalArgumentException("productId is required");
        }

        if (availableQuantity < 0) {
            throw new IllegalArgumentException(
                    "availableQuantity cannot be negative"
            );
        }

        this.productId = productId;
        this.availableQuantity = availableQuantity;
        this.reservedQuantity = 0;
        this.updatedAt = Instant.now();
    }

    public void reserve(int quantity) {
        if (quantity <= 0) {
            throw new IllegalArgumentException(
                    "quantity must be greater than zero"
            );
        }

        if (availableQuantity < quantity) {
            throw new InsufficientInventoryException(
                    productId,
                    quantity,
                    availableQuantity
            );
        }

        availableQuantity -= quantity;
        reservedQuantity += quantity;
        updatedAt = Instant.now();
    }

    /**
     * Returns previously reserved stock to the available pool — the inverse
     * of {@link #reserve(int)}. Releasing more than is currently reserved
     * means the reservation ledger and the counters have diverged; that is
     * corruption, not a business outcome, so it throws instead of clamping.
     */
    public void release(int quantity) {
        if (quantity <= 0) {
            throw new IllegalArgumentException(
                    "quantity must be greater than zero"
            );
        }

        if (reservedQuantity < quantity) {
            throw new IllegalStateException(
                    "Cannot release " + quantity + " for product " + productId
                            + ": only " + reservedQuantity + " reserved"
            );
        }

        reservedQuantity -= quantity;
        availableQuantity += quantity;
        updatedAt = Instant.now();
    }

    public UUID getProductId() {
        return productId;
    }

    public int getAvailableQuantity() {
        return availableQuantity;
    }

    public int getReservedQuantity() {
        return reservedQuantity;
    }

    public long getVersion() {
        return version;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }
}
