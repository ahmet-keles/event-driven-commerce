package com.ahmetkeles.inventoryservice.messaging;

/**
 * A record whose content violates the event contract: unparseable JSON or a
 * payload that does not match the announced event type. Such records can never
 * succeed on redelivery, so the error handling policy classifies this
 * exception as non-retryable and dead-letters the record immediately.
 */
public class InvalidEventException extends RuntimeException {

    public InvalidEventException(String message, Throwable cause) {
        super(message, cause);
    }
}
