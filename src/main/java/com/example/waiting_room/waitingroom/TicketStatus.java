package com.example.waiting_room.waitingroom;

public enum TicketStatus {
    /** In the queue. */
    WAITING,
    /** Let into the store, may shop until the session expires. */
    ADMITTED,
    /** Gave up while waiting. */
    ABANDONED,
    /** Admitted but the session timed out. */
    EXPIRED,
    /** Finished shopping and left the store. */
    DONE
}
