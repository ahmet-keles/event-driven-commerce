package com.ahmetkeles.inventoryservice.inventory;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface InventoryReservationRepository
        extends JpaRepository<InventoryReservation, UUID> {

    List<InventoryReservation> findByOrderIdAndStatus(
            UUID orderId,
            ReservationStatus status
    );
}
