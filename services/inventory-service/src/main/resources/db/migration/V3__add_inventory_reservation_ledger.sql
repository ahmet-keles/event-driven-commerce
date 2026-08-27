-- Durable compensation state. Two structures, both owned by this service:
--
-- inventory_reservations is the per-item reservation ledger: one row per
-- successfully reserved order item, keyed by the order item's id (the durable
-- business correlation key carried on every event since the multi-item work;
-- product_id cannot key a reservation because one order may legally contain
-- several items for the same product). Cancellation releases stock from these
-- rows, never from event payloads.
--
-- order_inventory_state is this service's per-order cancellation marker and
-- the serialization point for reserve/cancel decisions: both paths lock the
-- order's row before touching stock, which closes the race where a late
-- reservation lands while a cancellation is releasing. Kafka's per-key
-- ordering cannot be relied on for this (retries and dead-letter redelivery
-- break it).

CREATE TABLE inventory_reservations (
    order_item_id UUID NOT NULL,
    order_id UUID NOT NULL,
    product_id UUID NOT NULL,
    quantity INTEGER NOT NULL,
    status VARCHAR(20) NOT NULL,
    source_event_id UUID NOT NULL,
    created_at TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP(6) WITH TIME ZONE NOT NULL,

    CONSTRAINT inventory_reservations_pkey PRIMARY KEY (order_item_id),
    CONSTRAINT inventory_reservations_source_event_unique
        UNIQUE (source_event_id),
    CONSTRAINT inventory_reservations_quantity_positive
        CHECK (quantity > 0),
    CONSTRAINT inventory_reservations_status_check
        CHECK (status IN ('RESERVED', 'RELEASED'))
);

CREATE INDEX idx_inventory_reservations_order_reserved
    ON inventory_reservations (order_id)
    WHERE status = 'RESERVED';

CREATE TABLE order_inventory_state (
    order_id UUID NOT NULL,
    state VARCHAR(20) NOT NULL,
    updated_at TIMESTAMP(6) WITH TIME ZONE NOT NULL,

    CONSTRAINT order_inventory_state_pkey PRIMARY KEY (order_id),
    CONSTRAINT order_inventory_state_state_check
        CHECK (state IN ('ACTIVE', 'CANCELLED'))
);
