-- Hardens the order schema so the invariants the domain model enforces in
-- Java are also enforced by PostgreSQL: any write path that bypasses the
-- aggregate (a backfill, a repair script, a future mapping bug) fails at the
-- database instead of persisting a row the domain cannot represent.
--
-- Everything here runs in one Flyway transaction. On this schema's data
-- volumes the validating scans are trivial; at production scale each
-- SET NOT NULL / ADD CONSTRAINT would instead be split into
-- ADD CONSTRAINT ... NOT VALID + VALIDATE CONSTRAINT across separate
-- releases so the exclusive lock stays brief.
--
-- V6 is reserved by concurrent work and intentionally absent here; this
-- migration must merge after it.

-- === orders: business-required columns become NOT NULL ======================
-- The Java constructor already rejects all of these; the schema now agrees.
-- (orders.version is NOT NULL since V5.)
ALTER TABLE orders
    ALTER COLUMN customer_id SET NOT NULL,
    ALTER COLUMN status SET NOT NULL,
    ALTER COLUMN total_amount SET NOT NULL,
    ALTER COLUMN currency SET NOT NULL,
    ALTER COLUMN created_at SET NOT NULL,
    ALTER COLUMN updated_at SET NOT NULL;

-- status NOT NULL also closes the gap in orders_status_check from V1: a SQL
-- CHECK passes on NULL, so the IN-list alone never rejected a NULL status.

ALTER TABLE orders
    ADD CONSTRAINT orders_total_amount_nonnegative
        CHECK (total_amount >= 0),
    -- Order totals are sums of non-negative item subtotals.
    ADD CONSTRAINT orders_currency_iso4217
        CHECK (currency ~ '^[A-Z]{3}$'),
    -- Three uppercase letters, the ISO-4217 alphabetic form ("USD").
    ADD CONSTRAINT orders_version_nonnegative
        CHECK (version >= 0);
    -- Hibernate's @Version only ever increments from 0; a negative value
    -- means a corrupted write, not a concurrency outcome.

-- === order_items ============================================================
ALTER TABLE order_items
    ALTER COLUMN product_id SET NOT NULL,
    ALTER COLUMN unit_price SET NOT NULL;

ALTER TABLE order_items
    ADD CONSTRAINT order_items_quantity_positive
        CHECK (quantity > 0),
    ADD CONSTRAINT order_items_unit_price_nonnegative
        CHECK (unit_price >= 0);

-- Align the foreign key with the JPA mapping (cascade + orphanRemoval on
-- Order.items): a delete that does not go through the persistence context —
-- a cleanup script, an erasure job — now cascades instead of failing.
ALTER TABLE order_items
    DROP CONSTRAINT fk_order_items_order;
ALTER TABLE order_items
    ADD CONSTRAINT fk_order_items_order
        FOREIGN KEY (order_id) REFERENCES orders (id) ON DELETE CASCADE;

-- === outbox_events: indexes for the multi-replica publisher and retention ==
-- The publisher claims with
--   WHERE published_at IS NULL ORDER BY occurred_at, id FOR UPDATE SKIP LOCKED
-- and its cross-replica ordering guard scans
--   DISTINCT ON (aggregate_id) ... WHERE published_at IS NULL
--   ORDER BY aggregate_id, occurred_at, id.
-- Two partial indexes match those shapes exactly; both stay small because
-- they only ever hold the pending backlog.
DROP INDEX idx_outbox_events_unpublished; -- superseded by the composite below
CREATE INDEX idx_outbox_events_pending_claim
    ON outbox_events (occurred_at, id)
    WHERE published_at IS NULL;
CREATE INDEX idx_outbox_events_pending_aggregate
    ON outbox_events (aggregate_id, occurred_at, id)
    WHERE published_at IS NULL;
-- idx_outbox_events_aggregate (all rows) is kept: it serves operational
-- lookups of an aggregate's full history, published rows included.

-- Retention deletes published rows oldest-first; this index is the mirror
-- image of the pending ones and holds only the published side.
CREATE INDEX idx_outbox_events_published_at
    ON outbox_events (published_at, id)
    WHERE published_at IS NOT NULL;

-- === processed_events: index for retention ==================================
CREATE INDEX idx_processed_events_processed_at
    ON processed_events (processed_at, event_id);
