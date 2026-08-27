-- Hardening audit of the inventory schema (V1–V3), plus the indexes the
-- outbox publisher's claim queries and the retention job actually need.
--
-- Audited with no change required:
--   inventory_items      — every column NOT NULL, quantities guarded by
--                          non-negative CHECKs, optimistic-lock version
--                          present (V1).
--   order_inventory_state — PRIMARY KEY on order_id, state constrained to
--                          ACTIVE/CANCELLED, updated_at NOT NULL (V3).
--   No CHECK relating published_at to occurred_at is added on purpose: a
--   backwards clock step would otherwise make markPublished() violate the
--   constraint and wedge the publisher in a retry loop.

-- inventory_reservations: every reservation is created in the same
-- transaction that loaded and reserved its inventory item, so the reference
-- is always satisfiable; the FK turns "release found no inventory item"
-- (today an IllegalStateException at release time) into an impossible state.
-- RESTRICT (the default) also blocks deleting a product that still has
-- ledger rows.
ALTER TABLE inventory_reservations
    ADD CONSTRAINT fk_inventory_reservations_product
        FOREIGN KEY (product_id) REFERENCES inventory_items (product_id);

-- Outbox claim query (lockPendingEvents) orders by (occurred_at, id); the V2
-- index covered only occurred_at, leaving the tiebreak to a sort node.
DROP INDEX idx_inventory_outbox_unpublished;
CREATE INDEX idx_inventory_outbox_unpublished
    ON outbox_events (occurred_at, id)
    WHERE published_at IS NULL;

-- Cross-replica ordering guard (findOldestPendingEventIds) filters
-- published_at IS NULL and orders by (aggregate_id, occurred_at, id). The V2
-- index was a full index on aggregate_id alone; no query reads published rows
-- by aggregate, so the full index only taxed writes.
DROP INDEX idx_inventory_outbox_aggregate;
CREATE INDEX idx_inventory_outbox_aggregate_pending
    ON outbox_events (aggregate_id, occurred_at, id)
    WHERE published_at IS NULL;

-- Retention scans: both jobs delete oldest-first below an age cutoff, so each
-- needs a range scan on its timestamp. The outbox index is partial — retention
-- only ever touches published rows, and keeping unpublished rows out of the
-- index keeps it disjoint from the publisher's working set.
CREATE INDEX idx_inventory_outbox_published
    ON outbox_events (published_at)
    WHERE published_at IS NOT NULL;

CREATE INDEX idx_processed_events_processed_at
    ON processed_events (processed_at);
