package com.ahmetkeles.paymentservice.gateway;

/**
 * Boundary to the payment provider.
 *
 * <p>Implementations must be idempotent on
 * {@link PaymentGatewayRequest#idempotencyKey()}: charging the same key twice
 * must not charge the customer twice and must return the same outcome, because
 * the caller invokes this inside a database transaction that can commit after
 * the charge succeeded — a crash in that window redelivers the triggering
 * event and replays the charge under the same key.
 */
public interface PaymentGateway {

    PaymentGatewayResult charge(PaymentGatewayRequest request);
}
