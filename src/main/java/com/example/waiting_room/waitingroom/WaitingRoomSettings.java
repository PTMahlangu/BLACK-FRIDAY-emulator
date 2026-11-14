package com.example.waiting_room.waitingroom;

/**
 * @param admissionRatePerSecond how many shoppers are let in per second
 * @param storeCapacity          max shoppers inside the store at once
 * @param sessionSeconds         how long an admitted shopper may stay
 */
public record WaitingRoomSettings(double admissionRatePerSecond, int storeCapacity, int sessionSeconds) {

    public WaitingRoomSettings {
        if (admissionRatePerSecond <= 0 || admissionRatePerSecond > 10_000) {
            throw new IllegalArgumentException("admissionRatePerSecond must be in (0, 10000]");
        }
        if (storeCapacity < 1 || storeCapacity > 100_000) {
            throw new IllegalArgumentException("storeCapacity must be in [1, 100000]");
        }
        if (sessionSeconds < 1 || sessionSeconds > 3_600) {
            throw new IllegalArgumentException("sessionSeconds must be in [1, 3600]");
        }
    }
}
