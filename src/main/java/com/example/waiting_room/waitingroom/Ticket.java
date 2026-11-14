package com.example.waiting_room.waitingroom;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

/** A shopper's place in the waiting room. */
public final class Ticket {

    private final UUID id = UUID.randomUUID();
    private final Instant joinedAt = Instant.now();
    private final AtomicReference<TicketStatus> status = new AtomicReference<>(TicketStatus.WAITING);
    private volatile Instant admittedAt;
    /** Offset of this ticket's record in the Kafka queue topic, -1 until Kafka confirms it. */
    private volatile long offset = -1;

    boolean transition(TicketStatus from, TicketStatus to) {
        boolean changed = status.compareAndSet(from, to);
        if (changed && to == TicketStatus.ADMITTED) {
            admittedAt = Instant.now();
        }
        return changed;
    }

    void assignOffset(long offset) {
        this.offset = offset;
    }

    public UUID id() {
        return id;
    }

    public long offset() {
        return offset;
    }

    public Instant joinedAt() {
        return joinedAt;
    }

    public Instant admittedAt() {
        return admittedAt;
    }

    public TicketStatus status() {
        return status.get();
    }
}
