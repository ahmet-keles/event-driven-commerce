package com.ahmetkeles.paymentservice.payment;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * One payment attempt's terminal outcome for one order.
 *
 * <p>Immutability is structural: the row is created already carrying its
 * terminal {@link PaymentStatus} and this class exposes no method that
 * changes any field afterwards. The {@code order_id} unique constraint
 * guarantees at most one payment row per order, so a duplicate
 * {@code ORDER_CONFIRMED} — same eventId or a re-emitted one — can never
 * produce a second charge.
 *
 * <p>The entity id doubles as the {@code paymentId} in emitted events.
 */
@Entity
@Table(name = "payments")
public class Payment {

    @Id
    private UUID id;

    @Column(name = "order_id", nullable = false, unique = true)
    private UUID orderId;

    @Column(name = "customer_id", nullable = false)
    private UUID customerId;

    @Column(nullable = false)
    private BigDecimal amount;

    @Column(nullable = false, length = 10)
    private String currency;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private PaymentStatus status;

    @Column(name = "failure_reason")
    private String failureReason;

    @Column(name = "gateway_reference", nullable = false, length = 100)
    private String gatewayReference;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    protected Payment() {
    }

    private Payment(
            UUID orderId,
            UUID customerId,
            BigDecimal amount,
            String currency,
            PaymentStatus status,
            String failureReason,
            String gatewayReference
    ) {
        if (orderId == null) {
            throw new IllegalArgumentException("orderId is required");
        }

        if (customerId == null) {
            throw new IllegalArgumentException("customerId is required");
        }

        if (amount == null || amount.signum() <= 0) {
            throw new IllegalArgumentException(
                    "amount must be greater than zero"
            );
        }

        if (currency == null || currency.isBlank()) {
            throw new IllegalArgumentException("currency is required");
        }

        if (gatewayReference == null || gatewayReference.isBlank()) {
            throw new IllegalArgumentException("gatewayReference is required");
        }

        this.id = UUID.randomUUID();
        this.orderId = orderId;
        this.customerId = customerId;
        this.amount = amount;
        this.currency = currency;
        this.status = status;
        this.failureReason = failureReason;
        this.gatewayReference = gatewayReference;
        this.createdAt = Instant.now();
    }

    public static Payment completed(
            UUID orderId,
            UUID customerId,
            BigDecimal amount,
            String currency,
            String gatewayReference
    ) {
        return new Payment(
                orderId,
                customerId,
                amount,
                currency,
                PaymentStatus.COMPLETED,
                null,
                gatewayReference
        );
    }

    public static Payment failed(
            UUID orderId,
            UUID customerId,
            BigDecimal amount,
            String currency,
            String gatewayReference,
            String failureReason
    ) {
        if (failureReason == null || failureReason.isBlank()) {
            throw new IllegalArgumentException(
                    "failureReason is required for a failed payment"
            );
        }

        return new Payment(
                orderId,
                customerId,
                amount,
                currency,
                PaymentStatus.FAILED,
                failureReason,
                gatewayReference
        );
    }

    public UUID getId() {
        return id;
    }

    public UUID getOrderId() {
        return orderId;
    }

    public UUID getCustomerId() {
        return customerId;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public String getCurrency() {
        return currency;
    }

    public PaymentStatus getStatus() {
        return status;
    }

    public String getFailureReason() {
        return failureReason;
    }

    public String getGatewayReference() {
        return gatewayReference;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}
