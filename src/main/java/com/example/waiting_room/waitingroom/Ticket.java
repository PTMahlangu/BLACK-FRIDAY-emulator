package com.example.waiting_room.waitingroom;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

/** A shopper's place in the waiting room. */
public final class Ticket {

    private final UUID id = UUID.randomUUID();
    private final long sequence;
    private final Instant joinedAt = Instant.now();
    private final AtomicReference<TicketStatus> status = new AtomicReference<>(TicketStatus.WAITING);
    private volatile Instant admittedAt;

    Ticket(long sequence) {
        this.sequence = sequence;
    }

    boolean transition(TicketStatus from, TicketStatus to) {
        boolean changed = status.compareAndSet(from, to);
        if (changed && to == TicketStatus.ADMITTED) {
            admittedAt = Instant.now();
        }
        return changed;
    }

    public UUID id() {
        return id;
    }

    public long sequence() {
        return sequence;
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
