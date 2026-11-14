package com.example.waiting_room.waitingroom;

import com.example.waiting_room.events.EventPublisher;
import com.example.waiting_room.events.EventType;
import com.example.waiting_room.events.ShopEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * FIFO virtual waiting room. A scheduler admits shoppers at a fixed rate,
 * as long as the store is below capacity.
 */
@Service
public class WaitingRoomService {

    private static final long TICK_MS = 100;

    private final EventPublisher events;

    private final Map<UUID, Ticket> tickets = new ConcurrentHashMap<>();
    private final Queue<Ticket> queue = new ConcurrentLinkedQueue<>();
    private final Map<UUID, Ticket> inStore = new ConcurrentHashMap<>();

    private final AtomicLong nextSequence = new AtomicLong();
    private volatile long lastAdmittedSequence = 0;
    private final AtomicInteger waiting = new AtomicInteger();

    private final LongAdder joined = new LongAdder();
    private final LongAdder admitted = new LongAdder();
    private final LongAdder abandoned = new LongAdder();
    private final LongAdder expired = new LongAdder();
    private final LongAdder done = new LongAdder();

    private volatile WaitingRoomSettings settings;
    private double admissionBudget = 0; // guarded by this (tick and reset are synchronized)

    public WaitingRoomService(EventPublisher events,
                              @Value("${blackfriday.admission-rate-per-second:20}") double rate,
                              @Value("${blackfriday.store-capacity:200}") int capacity,
                              @Value("${blackfriday.session-seconds:30}") int sessionSeconds) {
        this.events = events;
        this.settings = new WaitingRoomSettings(rate, capacity, sessionSeconds);
    }

    public Ticket join() {
        Ticket ticket = new Ticket(nextSequence.incrementAndGet());
        tickets.put(ticket.id(), ticket);
        waiting.incrementAndGet();
        queue.add(ticket);
        joined.increment();
        events.publish(ShopEvent.of(EventType.JOINED, ticket.id()));
        return ticket;
    }

    public Optional<Ticket> find(UUID id) {
        return Optional.ofNullable(tickets.get(id));
    }

    /** Shopper gives up while waiting. The ticket stays in the queue and is skipped on admission. */
    public void abandon(UUID id) {
        find(id).filter(t -> t.transition(TicketStatus.WAITING, TicketStatus.ABANDONED)).ifPresent(t -> {
            waiting.decrementAndGet();
            abandoned.increment();
            events.publish(ShopEvent.of(EventType.ABANDONED, t.id()));
        });
    }

    /** Shopper is finished and leaves the store, freeing a slot. */
    public void leaveStore(UUID id) {
        find(id).filter(t -> t.transition(TicketStatus.ADMITTED, TicketStatus.DONE)).ifPresent(t -> {
            inStore.remove(t.id());
            done.increment();
            events.publish(ShopEvent.of(EventType.LEFT_STORE, t.id()));
        });
    }

    public boolean isShopping(UUID id) {
        return find(id).map(t -> t.status() == TicketStatus.ADMITTED).orElse(false);
    }

    /** Approximate number of people ahead of this ticket. */
    public long position(Ticket ticket) {
        if (ticket.status() != TicketStatus.WAITING) {
            return 0;
        }
        return Math.max(1, ticket.sequence() - lastAdmittedSequence);
    }

    public long estimatedWaitSeconds(Ticket ticket) {
        return (long) Math.ceil(position(ticket) / settings.admissionRatePerSecond());
    }

    @Scheduled(fixedRate = TICK_MS)
    synchronized void tick() {
        expireSessions();
        admit();
    }

    private void admit() {
        WaitingRoomSettings s = settings;
        admissionBudget = Math.min(admissionBudget + s.admissionRatePerSecond() * TICK_MS / 1000.0,
                Math.max(1, s.admissionRatePerSecond()));
        while (admissionBudget >= 1 && inStore.size() < s.storeCapacity()) {
            Ticket next = queue.poll();
            if (next == null) {
                admissionBudget = Math.min(admissionBudget, 1);
                return;
            }
            if (next.transition(TicketStatus.WAITING, TicketStatus.ADMITTED)) {
                waiting.decrementAndGet();
                inStore.put(next.id(), next);
                lastAdmittedSequence = next.sequence();
                admitted.increment();
                admissionBudget -= 1;
                events.publish(ShopEvent.of(EventType.ADMITTED, next.id()));
            }
            // abandoned tickets are dropped without using budget
        }
    }

    private void expireSessions() {
        Instant cutoff = Instant.now().minus(Duration.ofSeconds(settings.sessionSeconds()));
        for (Ticket t : inStore.values()) {
            if (t.admittedAt() != null && t.admittedAt().isBefore(cutoff)
                    && t.transition(TicketStatus.ADMITTED, TicketStatus.EXPIRED)) {
                inStore.remove(t.id());
                expired.increment();
                events.publish(ShopEvent.of(EventType.EXPIRED, t.id()));
            }
        }
    }

    public WaitingRoomSettings settings() {
        return settings;
    }

    public void updateSettings(WaitingRoomSettings newSettings) {
        this.settings = newSettings;
    }

    public synchronized void reset() {
        queue.clear();
        tickets.clear();
        inStore.clear();
        waiting.set(0);
        nextSequence.set(0);
        lastAdmittedSequence = 0;
        admissionBudget = 0;
        joined.reset();
        admitted.reset();
        abandoned.reset();
        expired.reset();
        done.reset();
    }

    public Stats stats() {
        return new Stats(waiting.get(), inStore.size(), joined.sum(), admitted.sum(),
                abandoned.sum(), expired.sum(), done.sum());
    }

    public record Stats(int waiting, int inStore, long joined, long admitted,
                        long abandoned, long expired, long leftStore) {
    }
}
