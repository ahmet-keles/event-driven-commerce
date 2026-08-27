package com.ahmetkeles.orderservice.messaging;

import java.math.BigDecimal;
import java.util.UUID;

public record PaymentCompletedEvent(
        UUID orderId,
        UUID paymentId,
        BigDecimal amount,
        String currency
) {
}
