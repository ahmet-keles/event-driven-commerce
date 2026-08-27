package com.ahmetkeles.orderservice.outbox.event;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Announces a confirmed order to the payment side of the saga: everything a
 * payment service needs to charge for the order, nothing more.
 */
public record OrderConfirmedEvent(
        UUID orderId,
        UUID customerId,
        BigDecimal totalAmount,
        String currency
) {
}
