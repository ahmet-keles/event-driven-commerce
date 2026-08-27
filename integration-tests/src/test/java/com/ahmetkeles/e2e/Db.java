package com.ahmetkeles.e2e;

import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * White-box assertions over each service's database, always scoped by the
 * test's own UUIDs — never global counts, never cleanup.
 */
final class Db {

    record OutboxRow(UUID id, String eventType, String payload, Instant publishedAt) {
    }

    record InventoryRow(int available, int reserved) {
    }

    private final PostgreSQLContainer<?> container;

    Db(PostgreSQLContainer<?> container) {
        this.container = container;
    }

    void seedInventory(UUID productId, int availableQuantity) {
        execute(
                "INSERT INTO inventory_items"
                        + " (product_id, available_quantity, reserved_quantity,"
                        + " version, updated_at)"
                        + " VALUES (?, ?, 0, 0, now())",
                statement -> {
                    statement.setObject(1, productId);
                    statement.setInt(2, availableQuantity);
                    statement.executeUpdate();
                    return null;
                });
    }

    Optional<InventoryRow> inventoryRow(UUID productId) {
        return execute(
                "SELECT available_quantity, reserved_quantity"
                        + " FROM inventory_items WHERE product_id = ?",
                statement -> {
                    statement.setObject(1, productId);
                    try (ResultSet result = statement.executeQuery()) {
                        if (!result.next()) {
                            return Optional.empty();
                        }
                        return Optional.of(new InventoryRow(
                                result.getInt(1), result.getInt(2)));
                    }
                });
    }

    boolean processedEventExists(UUID eventId) {
        return execute(
                "SELECT 1 FROM processed_events WHERE event_id = ?",
                statement -> {
                    statement.setObject(1, eventId);
                    try (ResultSet result = statement.executeQuery()) {
                        return result.next();
                    }
                });
    }

    long processedEventCount(UUID eventId) {
        return execute(
                "SELECT COUNT(*) FROM processed_events WHERE event_id = ?",
                statement -> {
                    statement.setObject(1, eventId);
                    try (ResultSet result = statement.executeQuery()) {
                        result.next();
                        return result.getLong(1);
                    }
                });
    }

    Optional<Instant> orderUpdatedAt(UUID orderId) {
        return execute(
                "SELECT updated_at FROM orders WHERE id = ?",
                statement -> {
                    statement.setObject(1, orderId);
                    try (ResultSet result = statement.executeQuery()) {
                        return result.next()
                                ? Optional.of(result.getTimestamp(1).toInstant())
                                : Optional.empty();
                    }
                });
    }

    /**
     * Reopens the accepted at-least-once window on purpose: a NULL
     * published_at makes the row pending again, so the publisher re-sends the
     * same eventId and the consumer's ledger must absorb the duplicate.
     */
    void markOutboxUnpublished(UUID outboxEventId) {
        execute(
                "UPDATE outbox_events SET published_at = NULL WHERE id = ?",
                statement -> {
                    statement.setObject(1, outboxEventId);
                    statement.executeUpdate();
                    return null;
                });
    }

    Optional<String> orderStatus(UUID orderId) {
        return execute(
                "SELECT status FROM orders WHERE id = ?",
                statement -> {
                    statement.setObject(1, orderId);
                    try (ResultSet result = statement.executeQuery()) {
                        return result.next()
                                ? Optional.of(result.getString(1))
                                : Optional.empty();
                    }
                });
    }

    Optional<Boolean> orderItemReserved(UUID orderItemId) {
        return execute(
                "SELECT reserved FROM order_items WHERE id = ?",
                statement -> {
                    statement.setObject(1, orderItemId);
                    try (ResultSet result = statement.executeQuery()) {
                        return result.next()
                                ? Optional.of(result.getBoolean(1))
                                : Optional.empty();
                    }
                });
    }

    List<OutboxRow> outboxRows(UUID aggregateId) {
        return execute(
                "SELECT id, event_type, payload, published_at"
                        + " FROM outbox_events WHERE aggregate_id = ?"
                        + " ORDER BY occurred_at",
                statement -> {
                    statement.setObject(1, aggregateId);
                    try (ResultSet result = statement.executeQuery()) {
                        List<OutboxRow> rows = new ArrayList<>();
                        while (result.next()) {
                            Timestamp publishedAt = result.getTimestamp(4);
                            rows.add(new OutboxRow(
                                    result.getObject(1, UUID.class),
                                    result.getString(2),
                                    result.getString(3),
                                    publishedAt == null
                                            ? null
                                            : publishedAt.toInstant()));
                        }
                        return rows;
                    }
                });
    }

    Optional<OutboxRow> outboxRow(UUID aggregateId, String eventType) {
        return outboxRows(aggregateId).stream()
                .filter(row -> row.eventType().equals(eventType))
                .findFirst();
    }

    private interface Query<T> {
        T run(PreparedStatement statement) throws SQLException;
    }

    private <T> T execute(String sql, Query<T> query) {
        try (Connection connection = DriverManager.getConnection(
                container.getJdbcUrl(),
                container.getUsername(),
                container.getPassword());
             PreparedStatement statement = connection.prepareStatement(sql)) {
            return query.run(statement);
        } catch (SQLException exception) {
            throw new IllegalStateException("Query failed: " + sql, exception);
        }
    }
}
