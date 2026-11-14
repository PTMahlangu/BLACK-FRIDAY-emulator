package com.example.waiting_room.events;

import java.time.Instant;
import java.util.UUID;

public record ShopEvent(EventType type, UUID ticketId, Long productId, Instant at) {

    public static ShopEvent of(EventType type, UUID ticketId) {
        return new ShopEvent(type, ticketId, null, Instant.now());
    }

    public static ShopEvent of(EventType type, UUID ticketId, Long productId) {
        return new ShopEvent(type, ticketId, productId, Instant.now());
    }
}
