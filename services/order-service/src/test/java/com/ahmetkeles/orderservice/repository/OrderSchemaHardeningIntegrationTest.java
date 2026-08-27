package com.ahmetkeles.orderservice.repository;

import com.ahmetkeles.orderservice.PostgreSQLIntegrationTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Proves the V7 hardening at the SQL level, deliberately bypassing the JPA
 * entities: every invariant the domain constructors enforce in Java must now
 * also be rejected by PostgreSQL itself, so no write path that skips the
 * aggregate — a backfill, a repair script, a mapping bug — can persist a row
 * the domain cannot represent.
 */
class OrderSchemaHardeningIntegrationTest extends PostgreSQLIntegrationTest {

    private static final String INSERT_ORDER = """
            INSERT INTO orders
                (id, customer_id, status, total_amount, currency,
                 created_at, updated_at, version)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """;

    private static final String INSERT_ITEM = """
            INSERT INTO order_items
                (id, product_id, quantity, unit_price, order_id, reserved)
            VALUES (?, ?, ?, ?, ?, FALSE)
            """;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void clearDatabase() {
        jdbcTemplate.update("DELETE FROM order_items");
        jdbcTemplate.update("DELETE FROM orders");
    }

    // -- orders ---------------------------------------------------------------

    @Test
    void wellFormedOrderRowIsAccepted() {
        UUID orderId = UUID.randomUUID();
        insertOrder(Map.of("id", orderId));

        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM orders", Integer.class);
        assertEquals(1, count);

        // A raw insert that names only the V1/V5 columns must still satisfy
        // the V6/V7 additions through their defaults.
        Boolean submitted = jdbcTemplate.queryForObject(
                "SELECT submitted FROM orders WHERE id = ?",
                Boolean.class, orderId);
        String paymentStatus = jdbcTemplate.queryForObject(
                "SELECT payment_status FROM orders WHERE id = ?",
                String.class, orderId);
        assertEquals(Boolean.FALSE, submitted);
        assertEquals("NOT_STARTED", paymentStatus);
    }

    @Test
    void businessRequiredOrderColumnsRejectNull() {
        for (String column : List.of(
                "customer_id", "status", "total_amount", "currency",
                "created_at", "updated_at", "version")) {
            Map<String, Object> overrides = new HashMap<>();
            overrides.put(column, null);

            assertThrows(DataIntegrityViolationException.class,
                    () -> insertOrder(overrides),
                    "NULL " + column + " must be rejected");
        }
    }

    @Test
    void negativeTotalAmountIsRejected() {
        assertThrows(DataIntegrityViolationException.class,
                () -> insertOrder(Map.of(
                        "total_amount", new BigDecimal("-0.01"))));
    }

    @Test
    void nonIso4217CurrencyIsRejected() {
        for (String currency : List.of("usd", "DOLLARS", "US", "U5D", "   ")) {
            assertThrows(DataIntegrityViolationException.class,
                    () -> insertOrder(Map.of("currency", currency)),
                    "currency '" + currency + "' must be rejected");
        }
    }

    @Test
    void negativeVersionIsRejected() {
        assertThrows(DataIntegrityViolationException.class,
                () -> insertOrder(Map.of("version", -1L)));
    }

    @Test
    void nullStatusIsNoLongerLetThroughTheStatusCheck() {
        // V1's CHECK (status IN (...)) passes on NULL by SQL semantics; the
        // V7 NOT NULL is what actually closes that gap.
        Map<String, Object> overrides = new HashMap<>();
        overrides.put("status", null);

        assertThrows(DataIntegrityViolationException.class,
                () -> insertOrder(overrides));
    }

    // -- order_items ----------------------------------------------------------

    @Test
    void nonPositiveQuantityIsRejected() {
        UUID orderId = UUID.randomUUID();
        insertOrder(Map.of("id", orderId));

        for (int quantity : new int[] {0, -1}) {
            assertThrows(DataIntegrityViolationException.class,
                    () -> insertItem(orderId, quantity,
                            new BigDecimal("1.00")),
                    "quantity " + quantity + " must be rejected");
        }
    }

    @Test
    void negativeUnitPriceAndNullItemColumnsAreRejected() {
        UUID orderId = UUID.randomUUID();
        insertOrder(Map.of("id", orderId));

        assertThrows(DataIntegrityViolationException.class,
                () -> insertItem(orderId, 1, new BigDecimal("-0.01")));
        assertThrows(DataIntegrityViolationException.class,
                () -> jdbcTemplate.update(INSERT_ITEM, UUID.randomUUID(),
                        null, 1, new BigDecimal("1.00"), orderId));
        assertThrows(DataIntegrityViolationException.class,
                () -> jdbcTemplate.update(INSERT_ITEM, UUID.randomUUID(),
                        UUID.randomUUID(), 1, null, orderId));
    }

    @Test
    void deletingAnOrderOutsideJpaCascadesToItsItems() {
        UUID orderId = UUID.randomUUID();
        insertOrder(Map.of("id", orderId));
        insertItem(orderId, 2, new BigDecimal("1.00"));

        jdbcTemplate.update("DELETE FROM orders WHERE id = ?", orderId);

        Integer orphans = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM order_items WHERE order_id = ?",
                Integer.class, orderId);
        assertEquals(0, orphans,
                "ON DELETE CASCADE must remove the order's items");
    }

    // -- indexes --------------------------------------------------------------

    @Test
    void publisherAndRetentionIndexesExist() {
        List<String> indexes = jdbcTemplate.queryForList(
                "SELECT indexname FROM pg_indexes WHERE schemaname = 'public'",
                String.class);

        for (String expected : List.of(
                "idx_outbox_events_pending_claim",
                "idx_outbox_events_pending_aggregate",
                "idx_outbox_events_published_at",
                "idx_processed_events_processed_at")) {
            assertTrue(indexes.contains(expected),
                    "missing index " + expected + "; present: " + indexes);
        }

        assertFalse(indexes.contains("idx_outbox_events_unpublished"),
                "superseded index should have been dropped by V7");
    }

    // -- helpers --------------------------------------------------------------

    /** Inserts an orders row that is valid except for the given overrides. */
    private void insertOrder(Map<String, Object> overrides) {
        Map<String, Object> row = new HashMap<>();
        row.put("id", UUID.randomUUID());
        row.put("customer_id", UUID.randomUUID());
        row.put("status", "PENDING");
        row.put("total_amount", BigDecimal.ZERO);
        row.put("currency", "USD");
        row.put("created_at", OffsetDateTime.now());
        row.put("updated_at", OffsetDateTime.now());
        row.put("version", 0L);
        row.putAll(overrides);

        jdbcTemplate.update(INSERT_ORDER,
                row.get("id"), row.get("customer_id"), row.get("status"),
                row.get("total_amount"), row.get("currency"),
                row.get("created_at"), row.get("updated_at"),
                row.get("version"));
    }

    private void insertItem(UUID orderId, int quantity, BigDecimal unitPrice) {
        jdbcTemplate.update(INSERT_ITEM, UUID.randomUUID(), UUID.randomUUID(),
                quantity, unitPrice, orderId);
    }
}
