package com.ahmetkeles.inventoryservice.inventory;

import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

public interface OrderInventoryStateRepository
        extends JpaRepository<OrderInventoryState, UUID> {

    /**
     * Creates the order's state row if it does not exist yet, without failing
     * or lifting an existing CANCELLED row back to ACTIVE. When two
     * transactions race on the same new order id, PostgreSQL blocks the
     * second insert until the first resolves, so the follow-up
     * {@link #lockByOrderId} always finds a committed row to lock.
     */
    @Modifying
    @Query(
            value = """
                    INSERT INTO order_inventory_state
                        (order_id, state, updated_at)
                    VALUES
                        (:orderId, 'ACTIVE', :now)
                    ON CONFLICT (order_id) DO NOTHING
                    """,
            nativeQuery = true
    )
    int insertIfAbsent(
            @Param("orderId") UUID orderId,
            @Param("now") Instant now
    );

    /**
     * Acquires the per-order lock ({@code SELECT ... FOR UPDATE}). Blocks
     * until any concurrent reserve or cancel transaction for the same order
     * commits or rolls back.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select s from OrderInventoryState s where s.orderId = :orderId")
    Optional<OrderInventoryState> lockByOrderId(@Param("orderId") UUID orderId);
}
