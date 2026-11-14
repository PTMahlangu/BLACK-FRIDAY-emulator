package com.example.waiting_room.store;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

@Entity
@Getter
@NoArgsConstructor
public class PurchaseOrder {

    @Id
    @GeneratedValue
    private Long id;

    private Long productId;

    private UUID ticketId;

    private long priceCents;

    private Instant createdAt;

    public PurchaseOrder(Long productId, UUID ticketId, long priceCents) {
        this.productId = productId;
        this.ticketId = ticketId;
        this.priceCents = priceCents;
        this.createdAt = Instant.now();
    }
}
