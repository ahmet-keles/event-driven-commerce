package com.ahmetkeles.paymentservice.gateway;

import java.math.BigDecimal;

/**
 * One charge attempt. The idempotency key identifies the business operation
 * (here: paying one order), not the attempt, so retries reuse the same key.
 */
public record PaymentGatewayRequest(
        String idempotencyKey,
        BigDecimal amount,
        String currency
) {
}
