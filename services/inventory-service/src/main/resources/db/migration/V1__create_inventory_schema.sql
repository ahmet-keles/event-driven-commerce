CREATE TABLE inventory_items (
    product_id UUID NOT NULL,
    available_quantity INTEGER NOT NULL,
    reserved_quantity INTEGER NOT NULL DEFAULT 0,
    version BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMP(6) WITH TIME ZONE NOT NULL,

    CONSTRAINT inventory_items_pkey PRIMARY KEY (product_id),
    CONSTRAINT inventory_available_quantity_nonnegative
        CHECK (available_quantity >= 0),
    CONSTRAINT inventory_reserved_quantity_nonnegative
        CHECK (reserved_quantity >= 0)
);

CREATE TABLE processed_events (
    event_id UUID NOT NULL,
    event_type VARCHAR(150) NOT NULL,
    processed_at TIMESTAMP(6) WITH TIME ZONE NOT NULL,

    CONSTRAINT processed_events_pkey PRIMARY KEY (event_id)
);
