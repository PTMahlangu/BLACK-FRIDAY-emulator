package com.example.waiting_room.waitingroom;

import com.example.waiting_room.config.BlackFridayProperties;
import com.example.waiting_room.events.EventPublisher;
import com.example.waiting_room.events.EventType;
import com.example.waiting_room.events.ShopEvent;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/**
 * Virtual waiting room. The line itself lives in Kafka ({@link KafkaWaitingQueue});
 * this service keeps the ticket registry (status lookups), the store occupancy
 * and the admission rate budget that the Kafka gatekeeper asks for on every tick.
 */
@Service
public class WaitingRoomService implements KafkaWaitingQueue.Gate {

    private final EventPublisher events;
    private final KafkaWaitingQueue queue;

    private final Map<UUID, Ticket> tickets = new ConcurrentHashMap<>();
    private final Map<UUID, Ticket> inStore = new ConcurrentHashMap<>();
    private final AtomicInteger waiting = new AtomicInteger();

    private final LongAdder joined = new LongAdder();
    private final LongAdder admitted = new LongAdder();
    private final LongAdder abandoned = new LongAdder();
    private final LongAdder expired = new LongAdder();
    private final LongAdder done = new LongAdder();

    private volatile WaitingRoomSettings settings;
    private volatile double admissionBudget = 0; // written by the gatekeeper thread (and reset)

    public WaitingRoomService(EventPublisher events, KafkaWaitingQueue queue, BlackFridayProperties defaults) {
        this.events = events;
        this.queue = queue;
        this.settings = new WaitingRoomSettings(
                defaults.admissionRatePerSecond(), defaults.storeCapacity(), defaults.sessionSeconds());
    }

    @EventListener(ApplicationReadyEvent.class)
    void openWaitingRoom() {
        queue.start(this);
    }

    /** Joins the back of the line. Blocks until Kafka has stored the ticket. */
    public Ticket join() {
        Ticket ticket = new Ticket();
        // Register before producing so the gatekeeper always recognises the record.
        tickets.put(ticket.id(), ticket);
        waiting.incrementAndGet();
        try {
            ticket.assignOffset(queue.enqueue(ticket.id()));
        } catch (RuntimeException e) {
            tickets.remove(ticket.id());
            waiting.decrementAndGet();
            throw e;
        }
        joined.increment();
        events.publish(ShopEvent.of(EventType.JOINED, ticket.id()));
        return ticket;
    }

    public Optional<Ticket> find(UUID id) {
        return Optional.ofNullable(tickets.get(id));
    }

    /** Shopper gives up while waiting. The gatekeeper skips their record when it reaches it. */
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

    /** Places in line counted by Kafka offsets: 1 means next to be admitted. Includes people who gave up but are not yet skipped. */
    public long position(Ticket ticket) {
        if (ticket.status() != TicketStatus.WAITING) {
            return 0;
        }
        long head = queue.headOffset();
        if (ticket.offset() < 0 || head < 0) {
            return waiting.get();
        }
        return Math.max(1, ticket.offset() - head + 1);
    }

    public long estimatedWaitSeconds(Ticket ticket) {
        return (long) Math.ceil(position(ticket) / settings.admissionRatePerSecond());
    }

    // ---- KafkaWaitingQueue.Gate, called by the gatekeeper thread every tick ----

    @Override
    public int allowance() {
        expireSessions();
        WaitingRoomSettings s = settings;
        admissionBudget = Math.min(admissionBudget + s.admissionRatePerSecond() * KafkaWaitingQueue.TICK_MS / 1000.0,
                Math.max(1, s.admissionRatePerSecond()));
        int freeSlots = s.storeCapacity() - inStore.size();
        return (int) Math.max(0, Math.min(Math.floor(admissionBudget), freeSlots));
    }

    @Override
    public boolean isWaiting(UUID ticketId) {
        Ticket t = tickets.get(ticketId);
        return t != null && t.status() == TicketStatus.WAITING;
    }

    @Override
    public boolean admit(UUID ticketId) {
        Ticket t = tickets.get(ticketId);
        if (t == null || !t.transition(TicketStatus.WAITING, TicketStatus.ADMITTED)) {
            return false;
        }
        waiting.decrementAndGet();
        inStore.put(t.id(), t);
        admitted.increment();
        admissionBudget -= 1;
        events.publish(ShopEvent.of(EventType.ADMITTED, t.id()));
        return true;
    }

    @Override
    public void queueIdle() {
        // don't let unused budget pile up into a burst when the next crowd arrives
        admissionBudget = Math.min(admissionBudget, 1);
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

    // ---- settings, reset, stats ----

    public WaitingRoomSettings settings() {
        return settings;
    }

    public void updateSettings(WaitingRoomSettings newSettings) {
        this.settings = newSettings;
    }

    /** Clears all tickets and moves the Kafka line's head to its tail. */
    public void reset() {
        queue.requestReset();
        tickets.clear();
        inStore.clear();
        waiting.set(0);
        admissionBudget = 0;
        joined.reset();
        admitted.reset();
        abandoned.reset();
        expired.reset();
        done.reset();
    }

    public Stats stats() {
        long head = queue.headOffset();
        long tail = queue.tailOffset();
        return new Stats(waiting.get(), inStore.size(), joined.sum(), admitted.sum(),
                abandoned.sum(), expired.sum(), done.sum(),
                new QueueOffsets(head, tail, head < 0 ? 0 : Math.max(0, tail - head)));
    }

    public record Stats(int waiting, int inStore, long joined, long admitted,
                        long abandoned, long expired, long leftStore, QueueOffsets kafkaQueue) {
    }

    /** head = next offset to admit, tail = next offset to be written, lag = records not yet handled. */
    public record QueueOffsets(long head, long tail, long lag) {
    }
}
