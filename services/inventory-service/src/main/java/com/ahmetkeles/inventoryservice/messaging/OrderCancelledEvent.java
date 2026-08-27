package com.ahmetkeles.inventoryservice.messaging;

import java.util.UUID;

/**
 * Minimal by contract: the cancellation names only the order. What to
 * release is derived from this service's own reservation ledger, never from
 * the event.
 */
public record OrderCancelledEvent(
        UUID orderId
) {
}
