package com.ahmetkeles.paymentservice.outbox;

import java.math.BigDecimal;
import java.util.UUID;

/** Frozen contract: PAYMENT_FAILED payload on payment.events. */
public record PaymentFailedEvent(
        UUID orderId,
        UUID paymentId,
        BigDecimal amount,
        String currency,
        String reason
) {
}
