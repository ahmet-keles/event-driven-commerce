package com.ahmetkeles.inventoryservice.inventory;

import java.util.UUID;

public class InsufficientInventoryException extends RuntimeException {

    public InsufficientInventoryException(
            UUID productId,
            int requested,
            int available
    ) {
        super(
                "Insufficient inventory for product " + productId
                        + ": requested=" + requested
                        + ", available=" + available
        );
    }
}
