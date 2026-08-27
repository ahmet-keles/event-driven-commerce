package com.ahmetkeles.paymentservice.messaging;

import java.math.BigDecimal;
import java.util.UUID;

/** Frozen contract: ORDER_CONFIRMED payload on order.events. */
public record OrderConfirmedEvent(
        UUID orderId,
        UUID customerId,
        BigDecimal totalAmount,
        String currency
) {
}
