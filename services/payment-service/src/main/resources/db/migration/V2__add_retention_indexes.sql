-- Retention scans delete oldest-first below an age cutoff, ordering by
-- (timestamp, id); each index carries the id tiebreak so it matches its
-- query's ORDER BY exactly (same shapes as the order and inventory schemas).
--
-- The published-outbox index is partial: retention only ever touches
-- published rows, and keeping unpublished rows out of it keeps the index
-- disjoint from the publisher's working set, which V1's partial
-- idx_payment_outbox_unpublished already serves.
CREATE INDEX idx_payment_outbox_published
    ON outbox_events (published_at, id)
    WHERE published_at IS NOT NULL;

CREATE INDEX idx_payment_processed_events_processed_at
    ON processed_events (processed_at, event_id);
