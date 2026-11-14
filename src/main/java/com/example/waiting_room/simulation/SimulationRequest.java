package com.example.waiting_room.simulation;

/**
 * @param shoppers        number of virtual shoppers to send
 * @param rampUpSeconds   shoppers arrive spread over this many seconds
 * @param buyProbability  chance (0..1) that an admitted shopper tries to buy
 * @param patienceSeconds average time a shopper waits in the queue before giving up
 */
public record SimulationRequest(int shoppers, int rampUpSeconds, double buyProbability, int patienceSeconds) {

    public SimulationRequest {
        if (shoppers < 1 || shoppers > 100_000) {
            throw new IllegalArgumentException("shoppers must be in [1, 100000]");
        }
        if (rampUpSeconds < 0 || rampUpSeconds > 3_600) {
            throw new IllegalArgumentException("rampUpSeconds must be in [0, 3600]");
        }
        if (buyProbability < 0 || buyProbability > 1) {
            throw new IllegalArgumentException("buyProbability must be in [0, 1]");
        }
        if (patienceSeconds < 1 || patienceSeconds > 3_600) {
            throw new IllegalArgumentException("patienceSeconds must be in [1, 3600]");
        }
    }
}
