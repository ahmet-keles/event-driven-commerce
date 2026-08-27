package com.ahmetkeles.paymentservice.outbox;

import java.math.BigDecimal;
import java.util.UUID;

/** Frozen contract: PAYMENT_COMPLETED payload on payment.events. */
public record PaymentCompletedEvent(
        UUID orderId,
        UUID paymentId,
        BigDecimal amount,
        String currency
) {
}
