package com.ahmetkeles.paymentservice.gateway;

/**
 * Terminal outcome of a charge. {@code declineReason} is set exactly when the
 * charge was not approved; {@code gatewayReference} identifies the operation
 * at the provider in both cases.
 */
public record PaymentGatewayResult(
        boolean approved,
        String gatewayReference,
        String declineReason
) {

    public static PaymentGatewayResult approved(String gatewayReference) {
        return new PaymentGatewayResult(true, gatewayReference, null);
    }

    public static PaymentGatewayResult declined(
            String gatewayReference,
            String declineReason
    ) {
        return new PaymentGatewayResult(false, gatewayReference, declineReason);
    }
}
