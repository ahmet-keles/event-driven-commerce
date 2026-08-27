package com.ahmetkeles.inventoryservice;

import com.ahmetkeles.inventoryservice.inventory.InventoryItem;
import com.ahmetkeles.inventoryservice.inventory.InventoryItemRepository;
import com.ahmetkeles.inventoryservice.inventory.InventoryReservation;
import com.ahmetkeles.inventoryservice.inventory.InventoryReservationRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Proofs for the V4 hardening migration: the reservation-to-item foreign key
 * holds, and the indexes the outbox publisher's claim queries and the
 * retention job rely on actually exist after migration.
 */
class SchemaHardeningIntegrationTest extends PostgreSQLIntegrationTest {

    @Autowired
    private InventoryItemRepository inventoryItemRepository;

    @Autowired
    private InventoryReservationRepository inventoryReservationRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void cleanDatabase() {
        inventoryReservationRepository.deleteAll();
        inventoryItemRepository.deleteAll();
    }

    @Test
    void reservationForUnknownProductIsRejectedByForeignKey() {
        assertThrows(
                DataIntegrityViolationException.class,
                () -> inventoryReservationRepository.saveAndFlush(
                        new InventoryReservation(
                                UUID.randomUUID(),
                                UUID.randomUUID(),
                                UUID.randomUUID(),
                                1,
                                UUID.randomUUID()
                        )
                )
        );
    }

    @Test
    void reservationForExistingProductIsAccepted() {
        UUID productId = UUID.randomUUID();
        inventoryItemRepository.saveAndFlush(
                new InventoryItem(productId, 10)
        );

        assertDoesNotThrow(() -> inventoryReservationRepository.saveAndFlush(
                new InventoryReservation(
                        UUID.randomUUID(),
                        UUID.randomUUID(),
                        productId,
                        1,
                        UUID.randomUUID()
                )
        ));
    }

    @Test
    void productWithLiveReservationsCannotBeDeleted() {
        UUID productId = UUID.randomUUID();
        inventoryItemRepository.saveAndFlush(
                new InventoryItem(productId, 10)
        );
        inventoryReservationRepository.saveAndFlush(
                new InventoryReservation(
                        UUID.randomUUID(),
                        UUID.randomUUID(),
                        productId,
                        1,
                        UUID.randomUUID()
                )
        );

        assertThrows(
                DataIntegrityViolationException.class,
                () -> {
                    inventoryItemRepository.deleteById(productId);
                    inventoryItemRepository.flush();
                }
        );
    }

    @Test
    void hardeningIndexesExist() {
        List<String> indexNames = jdbcTemplate.queryForList(
                """
                SELECT indexname
                FROM pg_indexes
                WHERE tablename IN ('outbox_events', 'processed_events')
                """,
                String.class
        );

        assertTrue(indexNames.contains("idx_inventory_outbox_unpublished"));
        assertTrue(indexNames.contains(
                "idx_inventory_outbox_aggregate_pending"));
        assertTrue(indexNames.contains("idx_inventory_outbox_published"));
        assertTrue(indexNames.contains("idx_processed_events_processed_at"));
    }
}
