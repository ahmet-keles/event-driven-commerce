package com.ahmetkeles.orderservice.domain;

import java.util.UUID;

public class EmptyOrderSubmissionException extends RuntimeException {

    public EmptyOrderSubmissionException(UUID orderId) {
        super("Order " + orderId + " cannot be submitted without items");
    }
}
