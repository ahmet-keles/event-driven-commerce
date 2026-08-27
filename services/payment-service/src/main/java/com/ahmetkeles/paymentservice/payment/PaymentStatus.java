package com.ahmetkeles.paymentservice.payment;

/**
 * Terminal payment outcomes. There is no in-between state on purpose: a
 * payment row is written once with its final status and never transitions.
 */
public enum PaymentStatus {
    COMPLETED,
    FAILED
}
