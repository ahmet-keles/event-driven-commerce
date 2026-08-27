-- Optimistic-locking version for the Order aggregate root. Concurrent
-- transactions that load the same order and both write it are serialized by
-- Hibernate's versioned UPDATE (WHERE id = ? AND version = ?): the second
-- writer matches zero rows and fails with an optimistic-lock conflict instead
-- of silently overwriting the first.
--
-- ADD COLUMN with a non-volatile DEFAULT is metadata-only on PostgreSQL 11+:
-- no table rewrite, no long lock, and every existing order becomes version 0
-- in the same statement that makes the column NOT NULL. The default also
-- keeps any non-JPA insert path valid; Hibernate always writes the version
-- explicitly.
ALTER TABLE orders
    ADD COLUMN version BIGINT NOT NULL DEFAULT 0;
