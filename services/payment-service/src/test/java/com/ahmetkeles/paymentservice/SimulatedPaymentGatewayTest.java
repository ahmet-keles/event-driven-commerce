package com.ahmetkeles.paymentservice;

import com.ahmetkeles.paymentservice.gateway.PaymentGatewayRequest;
import com.ahmetkeles.paymentservice.gateway.PaymentGatewayResult;
import com.ahmetkeles.paymentservice.gateway.SimulatedPaymentGateway;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SimulatedPaymentGatewayTest {

    private final SimulatedPaymentGateway gateway =
            new SimulatedPaymentGateway(new BigDecimal("1000.00"));

    private PaymentGatewayRequest request(String amount) {
        return new PaymentGatewayRequest(
                UUID.randomUUID().toString(),
                new BigDecimal(amount),
                "USD"
        );
    }

    @Test
    void amountBelowThresholdIsApproved() {
        PaymentGatewayResult result = gateway.charge(request("999.99"));

        assertTrue(result.approved());
        assertNotNull(result.gatewayReference());
        assertNull(result.declineReason());
    }

    @Test
    void amountAtThresholdIsDeclined() {
        PaymentGatewayResult result = gateway.charge(request("1000.00"));

        assertFalse(result.approved());
        assertNotNull(result.declineReason());
    }

    @Test
    void amountAboveThresholdIsDeclined() {
        PaymentGatewayResult result = gateway.charge(request("2500.00"));

        assertFalse(result.approved());
        assertNotNull(result.gatewayReference());
    }

    @Test
    void sameIdempotencyKeyYieldsSameReference() {
        String key = UUID.randomUUID().toString();

        PaymentGatewayResult first = gateway.charge(
                new PaymentGatewayRequest(key, new BigDecimal("10.00"), "USD")
        );
        PaymentGatewayResult replay = gateway.charge(
                new PaymentGatewayRequest(key, new BigDecimal("10.00"), "USD")
        );

        assertEquals(first.gatewayReference(), replay.gatewayReference());
        assertEquals(first.approved(), replay.approved());
    }

    @Test
    void thresholdIsConfigurable() {
        SimulatedPaymentGateway strict =
                new SimulatedPaymentGateway(new BigDecimal("5.00"));

        assertFalse(strict.charge(request("5.00")).approved());
        assertTrue(strict.charge(request("4.99")).approved());
    }
}
