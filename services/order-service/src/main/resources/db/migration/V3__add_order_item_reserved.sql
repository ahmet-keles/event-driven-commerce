-- Per-item reservation state. An order is confirmed only once every one of its
-- items is reserved, so the order's progress is derived from these flags rather
-- than from a count of reservation events.
ALTER TABLE order_items
    ADD COLUMN reserved BOOLEAN NOT NULL DEFAULT FALSE;
