-- Explicit assembly/finalization state for the Order aggregate. An order is
-- assembled by the client (items added over multiple requests) and then
-- explicitly submitted; confirmation additionally requires every item to be
-- reserved, so a fast reservation can no longer confirm an order the client
-- is still assembling.
--
-- This lands as V6 (submission precedes the payment and schema-hardening
-- work, which renumber to V7/V8). NOT NULL DEFAULT FALSE keeps any non-JPA
-- insert path valid; Hibernate always writes the column explicitly.
ALTER TABLE orders
    ADD COLUMN submitted BOOLEAN NOT NULL DEFAULT FALSE;

-- Under the new rule CONFIRMED implies submitted. Backfill existing
-- confirmed orders so the invariant holds for rows that predate this
-- migration; CANCELLED rows stay unsubmitted (terminal either way).
UPDATE orders
SET submitted = TRUE
WHERE status = 'CONFIRMED';
