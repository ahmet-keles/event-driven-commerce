package com.ahmetkeles.orderservice.domain;

import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
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

    @Enumerated(EnumType.STRING)
    @Column(name = "payment_status", nullable = false, length = 20)
    private PaymentStatus paymentStatus;

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

    /**
     * Whether the client has finished assembling this order. Items can only
     * be added while the order is PENDING and unsubmitted, and the order can
     * only confirm once it is submitted AND every item is reserved — so a
     * fast reservation can never confirm an order that is still being
     * assembled.
     */
    private boolean submitted;

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
        this.paymentStatus = PaymentStatus.NOT_STARTED;
        this.totalAmount = BigDecimal.ZERO;
        this.currency = currency;
        this.submitted = false;
        this.createdAt = Instant.now();
        this.updatedAt = Instant.now();
    }

    /**
     * Adds an item to the order. Items can only be added while the order is
     * still {@code PENDING} and the client has not yet submitted it: a
     * terminal or submitted order rejects the call before any state is
     * touched, so items, total, {@code updatedAt} and the version all stay
     * exactly as they were.
     */
    public OrderItem addItem(UUID productId, int quantity, BigDecimal unitPrice) {
        if (status != OrderStatus.PENDING) {
            throw new OrderNotModifiableException(id, status);
        }

        if (submitted) {
            throw new OrderNotModifiableException(id);
        }

        OrderItem item = new OrderItem(productId, quantity, unitPrice, this);

        items.add(item);
        totalAmount = totalAmount.add(item.subtotal());
        updatedAt = Instant.now();

        return item;
    }

    /**
     * Records that one item of this order has been reserved. Reservation
     * state is tracked per item rather than as a count, so a redelivered
     * event for an item that is already reserved cannot advance the order
     * towards confirmation.
     *
     * <p>The aggregate is only mutated when a previously-unreserved matching
     * item is newly marked: a terminal order, an unknown or null item id, and
     * an already-reserved item all leave the order untouched, including
     * {@code updatedAt}.
     *
     * <p>Returns {@code true} only when this call performed the
     * {@code PENDING -> CONFIRMED} transition, which requires the client to
     * have explicitly submitted the order first AND every item to be
     * reserved — the explicit signal the service layer uses to emit
     * {@code ORDER_CONFIRMED} exactly once. A reservation on an order still
     * being assembled records the item and returns {@code false}; the
     * confirming call is then the submit. Confirmation also starts the
     * payment leg by moving {@code paymentStatus} to {@code PENDING} in the
     * same mutation.
     */
    public boolean markItemReserved(UUID orderItemId) {
        if (status != OrderStatus.PENDING) {
            return false;
        }

        OrderItem match = items.stream()
                .filter(item -> item.getId().equals(orderItemId))
                .findFirst()
                .orElse(null);

        if (match == null || match.isReserved()) {
            return false;
        }

        match.markReserved();
        updatedAt = Instant.now();

        return confirmIfSubmittedAndFullyReserved();
    }

    /**
     * Marks the order as submitted: the client has finished assembling it.
     * Reports whether this call performed the transition, mirroring
     * {@link #cancel()}, so exactly one caller ever sees {@code true}.
     *
     * <p>Rules:
     * <ul>
     * <li>a CANCELLED order cannot be submitted (throws
     *     {@link OrderNotModifiableException});</li>
     * <li>an already-submitted order (CONFIRMED orders are always
     *     submitted) is a true no-op — no state touched, {@code false}
     *     returned — so duplicate submits are idempotent;</li>
     * <li>an order without items cannot be submitted (throws
     *     {@link EmptyOrderSubmissionException}) and stays unsubmitted;</li>
     * <li>when every item is already reserved at submission time, the
     *     submit itself performs PENDING -&gt; CONFIRMED (and opens the
     *     payment leg), so reservations that finished before the client
     *     submitted confirm immediately in the submitting transaction. The
     *     service layer detects that case as {@code submit() == true} plus a
     *     CONFIRMED status: a {@code true} submit implies the order was not
     *     previously confirmed, so a CONFIRMED status afterwards can only
     *     have been produced by this call.</li>
     * </ul>
     */
    public boolean submit() {
        if (status == OrderStatus.CANCELLED) {
            throw new OrderNotModifiableException(id, status);
        }

        if (submitted) {
            return false;
        }

        if (items.isEmpty()) {
            throw new EmptyOrderSubmissionException(id);
        }

        submitted = true;
        updatedAt = Instant.now();

        confirmIfSubmittedAndFullyReserved();

        return true;
    }

    /**
     * The single confirmation site: both a final reservation and a submit
     * over fully-reserved items funnel through here, so the
     * PENDING -> CONFIRMED transition and the payment kickoff can never
     * happen separately or twice.
     */
    private boolean confirmIfSubmittedAndFullyReserved() {
        if (submitted && allItemsReserved()) {
            status = OrderStatus.CONFIRMED;
            paymentStatus = PaymentStatus.PENDING;
            return true;
        }

        return false;
    }

    private boolean allItemsReserved() {
        return !items.isEmpty()
                && items.stream().allMatch(OrderItem::isReserved);
    }

    /**
     * Cancels the order, reporting whether this call performed the
     * transition. Exactly one caller ever sees {@code true} for a given
     * order, which is what lets the service layer emit the cancellation
     * outbox event exactly once; a terminal order is left untouched,
     * including {@code updatedAt}.
     */
    public boolean cancel() {
        if (status != OrderStatus.PENDING) {
            return false;
        }

        status = OrderStatus.CANCELLED;
        updatedAt = Instant.now();
        return true;
    }

    /**
     * Records a successful payment, reporting whether this call performed the
     * {@code PENDING -> COMPLETED} transition. The first terminal payment
     * outcome wins: a payment that already completed or failed is left
     * untouched, including {@code updatedAt}.
     */
    public boolean completePayment() {
        if (paymentStatus != PaymentStatus.PENDING) {
            return false;
        }

        paymentStatus = PaymentStatus.COMPLETED;
        updatedAt = Instant.now();
        return true;
    }

    /**
     * Records a failed payment, reporting whether this call performed the
     * {@code PENDING -> FAILED} transition. A payment that already reached a
     * terminal outcome — completed or failed — is left untouched.
     */
    public boolean failPayment() {
        if (paymentStatus != PaymentStatus.PENDING) {
            return false;
        }

        paymentStatus = PaymentStatus.FAILED;
        updatedAt = Instant.now();
        return true;
    }

    /**
     * Payment-failure compensation transition: unlike {@link #cancel()},
     * which only cancels an order that never confirmed, this cancels a
     * CONFIRMED order whose payment failed. Reports whether this call
     * performed the transition; a PENDING or already-CANCELLED order is left
     * untouched, keeping generic cancellation semantics unchanged.
     */
    public boolean cancelForFailedPayment() {
        if (status != OrderStatus.CONFIRMED) {
            return false;
        }

        status = OrderStatus.CANCELLED;
        updatedAt = Instant.now();
        return true;
    }

    public PaymentStatus getPaymentStatus() {
        return paymentStatus;
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

    public boolean isSubmitted() {
        return submitted;
    }

    public List<OrderItem> getItems() {
        return List.copyOf(items);
    }
}
