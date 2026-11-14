package com.example.waiting_room.web;

import com.example.waiting_room.events.EventStatsListener;
import com.example.waiting_room.events.EventType;
import com.example.waiting_room.simulation.SimulationRequest;
import com.example.waiting_room.simulation.SimulationService;
import com.example.waiting_room.store.Product;
import com.example.waiting_room.store.StoreService;
import com.example.waiting_room.store.StoreService.PurchaseResult;
import com.example.waiting_room.waitingroom.Ticket;
import com.example.waiting_room.waitingroom.TicketStatus;
import com.example.waiting_room.waitingroom.WaitingRoomService;
import com.example.waiting_room.waitingroom.WaitingRoomSettings;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class BlackFridayController {

    private final WaitingRoomService waitingRoom;
    private final StoreService store;
    private final SimulationService simulation;
    private final EventStatsListener kafkaStats;
    private final StatsHistory history;

    // ---- shopper API (a real client could use this) ----

    @PostMapping("/queue")
    public TicketView join() {
        return view(waitingRoom.join());
    }

    @GetMapping("/queue/{ticketId}")
    public ResponseEntity<TicketView> ticket(@PathVariable UUID ticketId) {
        return ResponseEntity.of(waitingRoom.find(ticketId).map(this::view));
    }

    @DeleteMapping("/queue/{ticketId}")
    public void leave(@PathVariable UUID ticketId) {
        waitingRoom.abandon(ticketId);
        waitingRoom.leaveStore(ticketId);
    }

    @GetMapping("/products")
    public List<Product> products() {
        return store.catalogue();
    }

    @PostMapping("/store/{ticketId}/buy/{productId}")
    public ResponseEntity<Map<String, PurchaseResult>> buy(@PathVariable UUID ticketId, @PathVariable Long productId) {
        PurchaseResult result = store.purchase(ticketId, productId);
        HttpStatus status = switch (result) {
            case PURCHASED -> HttpStatus.OK;
            case SOLD_OUT -> HttpStatus.CONFLICT;
            case NOT_ADMITTED -> HttpStatus.FORBIDDEN;
            case UNKNOWN_PRODUCT -> HttpStatus.NOT_FOUND;
        };
        return ResponseEntity.status(status).body(Map.of("result", result));
    }

    // ---- emulator controls ----

    @PostMapping("/simulation")
    public SimulationService.Status start(@RequestBody SimulationRequest request) {
        simulation.start(request);
        return simulation.status();
    }

    @DeleteMapping("/simulation")
    public SimulationService.Status stop() {
        simulation.stop();
        return simulation.status();
    }

    @PutMapping("/settings")
    public WaitingRoomSettings updateSettings(@RequestBody WaitingRoomSettings settings) {
        waitingRoom.updateSettings(settings);
        return waitingRoom.settings();
    }

    /** Stops the simulation, empties the queue, deletes orders and restocks every product. */
    @PostMapping("/reset")
    public void reset() {
        simulation.stop();
        waitingRoom.reset();
        store.reset();
        kafkaStats.reset();
        history.reset();
    }

    @GetMapping("/stats")
    public Dashboard stats() {
        return new Dashboard(waitingRoom.stats(), store.stats(), simulation.status(), waitingRoom.settings(),
                new KafkaStats(kafkaStats.total(), kafkaStats.counts()), store.catalogue(), history.points());
    }

    @ExceptionHandler(IllegalArgumentException.class)
    ProblemDetail badRequest(IllegalArgumentException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, e.getMessage());
    }

    @ExceptionHandler(IllegalStateException.class)
    ProblemDetail conflict(IllegalStateException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, e.getMessage());
    }

    private TicketView view(Ticket t) {
        return new TicketView(t.id(), t.status(), waitingRoom.position(t), waitingRoom.estimatedWaitSeconds(t));
    }

    public record TicketView(UUID ticketId, TicketStatus status, long position, long estimatedWaitSeconds) {
    }

    public record KafkaStats(long consumed, Map<EventType, Long> byType) {
    }

    public record Dashboard(WaitingRoomService.Stats waitingRoom, StoreService.Stats store,
                            SimulationService.Status simulation, WaitingRoomSettings settings,
                            KafkaStats kafka, List<Product> products, List<StatsHistory.Point> history) {
    }
}
