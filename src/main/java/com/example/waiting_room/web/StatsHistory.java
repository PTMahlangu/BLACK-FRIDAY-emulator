package com.example.waiting_room.web;

import com.example.waiting_room.store.StoreService;
import com.example.waiting_room.waitingroom.WaitingRoomService;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

/** One-second samples for the dashboard chart (last 5 minutes). */
@Component
@RequiredArgsConstructor
public class StatsHistory {

    private static final int MAX_POINTS = 300;

    private final WaitingRoomService waitingRoom;
    private final StoreService store;

    private final Deque<Point> points = new ArrayDeque<>();
    private long lastPurchased;
    private long lastAdmitted;

    @Scheduled(fixedRate = 1000)
    synchronized void sample() {
        WaitingRoomService.Stats room = waitingRoom.stats();
        long purchased = store.stats().purchased();
        points.addLast(new Point(System.currentTimeMillis(), room.waiting(), room.inStore(),
                Math.max(0, room.admitted() - lastAdmitted), Math.max(0, purchased - lastPurchased)));
        lastAdmitted = room.admitted();
        lastPurchased = purchased;
        while (points.size() > MAX_POINTS) {
            points.removeFirst();
        }
    }

    public synchronized List<Point> points() {
        return List.copyOf(points);
    }

    public synchronized void reset() {
        points.clear();
        lastPurchased = 0;
        lastAdmitted = 0;
    }

    public record Point(long t, int waiting, int inStore, long admittedPerSec, long ordersPerSec) {
    }
}
