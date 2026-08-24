CREATE TABLE outbox_events (
    id UUID NOT NULL,
    aggregate_type VARCHAR(100) NOT NULL,
    aggregate_id UUID NOT NULL,
    event_type VARCHAR(150) NOT NULL,
    payload TEXT NOT NULL,
    occurred_at TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    published_at TIMESTAMP(6) WITH TIME ZONE,

    CONSTRAINT inventory_outbox_events_pkey PRIMARY KEY (id)
);

CREATE INDEX idx_inventory_outbox_unpublished
    ON outbox_events (occurred_at)
    WHERE published_at IS NULL;

CREATE INDEX idx_inventory_outbox_aggregate
    ON outbox_events (aggregate_id);
