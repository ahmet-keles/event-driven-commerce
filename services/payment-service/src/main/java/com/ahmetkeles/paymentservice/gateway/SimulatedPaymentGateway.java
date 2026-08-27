package com.ahmetkeles.paymentservice.gateway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * SIMULATED payment provider for this project. No money moves anywhere; the
 * outcome is computed locally and deterministically so integration tests and
 * local development can script both paths:
 *
 * <ul>
 *   <li>amount strictly below {@code app.payment.gateway.decline-threshold}
 *       (default 1000.00) → approved;</li>
 *   <li>amount at or above the threshold → declined with reason
 *       {@code "simulated decline: amount at or above threshold"}.</li>
 * </ul>
 *
 * <p>The gateway reference is derived from the idempotency key alone
 * ({@link UUID#nameUUIDFromBytes}), so replaying a charge with the same key
 * yields the identical reference and outcome — the behaviour a real provider's
 * idempotency-key support gives, exercised here without network calls.
 *
 * <p>Swap this bean for a real provider integration to go to production;
 * nothing else in the service depends on how the outcome is produced.
 */
@Component
public class SimulatedPaymentGateway implements PaymentGateway {

    private static final Logger log =
            LoggerFactory.getLogger(SimulatedPaymentGateway.class);

    static final String DECLINE_REASON =
            "simulated decline: amount at or above threshold";

    private final BigDecimal declineThreshold;

    public SimulatedPaymentGateway(
            @Value("${app.payment.gateway.decline-threshold:1000.00}")
            BigDecimal declineThreshold
    ) {
        this.declineThreshold = declineThreshold;
    }

    @Override
    public PaymentGatewayResult charge(PaymentGatewayRequest request) {
        String reference = "sim-" + UUID.nameUUIDFromBytes(
                request.idempotencyKey().getBytes(StandardCharsets.UTF_8)
        );

        if (request.amount().compareTo(declineThreshold) >= 0) {
            log.info(
                    "[SIMULATED] Declined charge {} for {} {} (threshold {})",
                    reference,
                    request.amount(),
                    request.currency(),
                    declineThreshold
            );

            return PaymentGatewayResult.declined(reference, DECLINE_REASON);
        }

        log.info(
                "[SIMULATED] Approved charge {} for {} {}",
                reference,
                request.amount(),
                request.currency()
        );

        return PaymentGatewayResult.approved(reference);
    }
}
