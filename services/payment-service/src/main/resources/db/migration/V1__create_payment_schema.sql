CREATE TABLE payments (
    id UUID NOT NULL,
    order_id UUID NOT NULL,
    customer_id UUID NOT NULL,
    amount NUMERIC(38,2) NOT NULL,
    currency VARCHAR(10) NOT NULL,
    status VARCHAR(20) NOT NULL,
    failure_reason VARCHAR(255),
    gateway_reference VARCHAR(100) NOT NULL,
    created_at TIMESTAMP(6) WITH TIME ZONE NOT NULL,

    CONSTRAINT payments_pkey PRIMARY KEY (id),
    -- At most one payment per order: the database-level backstop for the
    -- "duplicate ORDER_CONFIRMED never charges twice" guarantee.
    CONSTRAINT payments_order_id_unique UNIQUE (order_id),
    CONSTRAINT payments_status_check
        CHECK (status IN ('COMPLETED', 'FAILED')),
    CONSTRAINT payments_amount_positive CHECK (amount > 0),
    -- A failed payment carries its reason; a completed one never does.
    CONSTRAINT payments_failure_reason_matches_status CHECK (
        (status = 'FAILED' AND failure_reason IS NOT NULL)
        OR (status = 'COMPLETED' AND failure_reason IS NULL)
    )
);

CREATE TABLE processed_events (
    event_id UUID NOT NULL,
    event_type VARCHAR(150) NOT NULL,
    processed_at TIMESTAMP(6) WITH TIME ZONE NOT NULL,

    CONSTRAINT processed_events_pkey PRIMARY KEY (event_id)
);

CREATE TABLE outbox_events (
    id UUID NOT NULL,
    aggregate_type VARCHAR(100) NOT NULL,
    aggregate_id UUID NOT NULL,
    event_type VARCHAR(150) NOT NULL,
    payload TEXT NOT NULL,
    occurred_at TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    published_at TIMESTAMP(6) WITH TIME ZONE,

    CONSTRAINT payment_outbox_events_pkey PRIMARY KEY (id)
);

CREATE INDEX idx_payment_outbox_unpublished
    ON outbox_events (occurred_at)
    WHERE published_at IS NULL;

CREATE INDEX idx_payment_outbox_aggregate
    ON outbox_events (aggregate_id);
