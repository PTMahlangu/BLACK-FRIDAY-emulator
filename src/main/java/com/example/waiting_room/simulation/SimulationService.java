package com.example.waiting_room.simulation;

import com.example.waiting_room.store.Product;
import com.example.waiting_room.store.PurchaseResult;
import com.example.waiting_room.store.StoreService;
import com.example.waiting_room.waitingroom.Ticket;
import com.example.waiting_room.waitingroom.TicketStatus;
import com.example.waiting_room.waitingroom.WaitingRoomService;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Sends a crowd of virtual shoppers (one virtual thread each) at the waiting room.
 * Each shopper: arrives during the ramp-up, waits in the queue (or gives up),
 * browses once admitted, maybe buys, then leaves.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class SimulationService {

    private static final int MAX_PURCHASE_ATTEMPTS = 3;

    private final WaitingRoomService waitingRoom;
    private final StoreService store;

    private volatile AtomicInteger active = new AtomicInteger();
    private volatile ExecutorService executor;
    private volatile boolean running;
    private volatile SimulationRequest current;

    public synchronized void start(SimulationRequest request) {
        if (running) {
            throw new IllegalStateException("A simulation is already running");
        }
        List<Product> catalogue = store.catalogue();
        if (catalogue.isEmpty()) {
            throw new IllegalStateException("No products to sell");
        }
        running = true;
        current = request;
        executor = Executors.newVirtualThreadPerTaskExecutor();
        AtomicInteger remaining = new AtomicInteger(request.shoppers());
        active = remaining;
        for (int i = 0; i < request.shoppers(); i++) {
            executor.submit(() -> {
                try {
                    shop(request, catalogue);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    log.debug("Shopper failed", e);
                } finally {
                    if (remaining.decrementAndGet() == 0 && active == remaining) {
                        running = false;
                    }
                }
            });
        }
        executor.shutdown();
        log.info("Black Friday started: {}", request);
    }

    public synchronized void stop() {
        running = false;
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @PreDestroy
    void shutdown() {
        stop();
    }

    private void shop(SimulationRequest r, List<Product> catalogue) throws InterruptedException {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        if (r.rampUpSeconds() > 0) {
            Thread.sleep(rnd.nextLong(r.rampUpSeconds() * 1000L));
        }
        if (!running) {
            return;
        }

        Ticket ticket = waitingRoom.join();
        long patienceMs = (long) (r.patienceSeconds() * 1000L * rnd.nextDouble(0.5, 1.5));
        long giveUpAt = System.currentTimeMillis() + patienceMs;
        while (ticket.status() == TicketStatus.WAITING) {
            if (!running || System.currentTimeMillis() > giveUpAt) {
                waitingRoom.abandon(ticket.id());
                return;
            }
            Thread.sleep(250);
        }
        if (ticket.status() != TicketStatus.ADMITTED) {
            return;
        }

        try {
            Thread.sleep(rnd.nextLong(300, 2_000)); // browsing
            if (rnd.nextDouble() < r.buyProbability()) {
                for (int attempt = 0; attempt < MAX_PURCHASE_ATTEMPTS; attempt++) {
                    Product wanted = pickProduct(catalogue, rnd);
                    PurchaseResult result = store.purchase(ticket.id(), wanted.getId());
                    if (result != PurchaseResult.SOLD_OUT) {
                        break;
                    }
                    Thread.sleep(rnd.nextLong(200, 800)); // look for something else
                }
            }
        } finally {
            waitingRoom.leaveStore(ticket.id());
        }
    }

    /** Earlier (bigger) deals are more popular: weight = n - index. */
    private static Product pickProduct(List<Product> catalogue, ThreadLocalRandom rnd) {
        int n = catalogue.size();
        int totalWeight = n * (n + 1) / 2;
        int pick = rnd.nextInt(totalWeight);
        for (int i = 0; i < n; i++) {
            pick -= n - i;
            if (pick < 0) {
                return catalogue.get(i);
            }
        }
        return catalogue.getLast();
    }

    public Status status() {
        return new Status(running, current, running ? active.get() : 0);
    }

    public record Status(boolean running, SimulationRequest lastRequest, int shoppersActive) {
    }
}
