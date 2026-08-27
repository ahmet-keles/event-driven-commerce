-- Consumer-side idempotency ledger for inventory.events. A row means the
-- event's effects are fully committed: the claim insert and the order
-- mutation share one transaction, so a rolled-back attempt leaves no row
-- and the same event can be claimed again on redelivery.
CREATE TABLE processed_events (
    event_id UUID NOT NULL,
    event_type VARCHAR(150) NOT NULL,
    aggregate_id UUID NOT NULL,
    processed_at TIMESTAMP(6) WITH TIME ZONE NOT NULL,

    CONSTRAINT processed_events_pkey PRIMARY KEY (event_id)
);
