package com.ahmetkeles.orderservice.outbox.event;

import java.util.UUID;

/**
 * Deliberately minimal: inventory-service releases from its own reservation
 * ledger, so the cancellation must not carry item or quantity details that
 * could compete with that ledger as a source of truth.
 */
public record OrderCancelledEvent(
        UUID orderId
) {
}
