package com.example.waiting_room.events;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/** Consumes the event stream back from Kafka and counts events by type. */
@Component
public class EventStatsListener {

    private final Map<EventType, LongAdder> counts = new EnumMap<>(EventType.class);
    private final AtomicLong total = new AtomicLong();

    public EventStatsListener() {
        for (EventType type : EventType.values()) {
            counts.put(type, new LongAdder());
        }
    }

    @KafkaListener(topics = EventPublisher.TOPIC)
    public void onEvent(ConsumerRecord<String, String> record) {
        try {
            counts.get(EventType.valueOf(record.key())).increment();
        } catch (IllegalArgumentException | NullPointerException ignored) {
            // unknown key, still counted in the total
        }
        total.incrementAndGet();
    }

    public Map<EventType, Long> counts() {
        Map<EventType, Long> snapshot = new EnumMap<>(EventType.class);
        counts.forEach((type, adder) -> snapshot.put(type, adder.sum()));
        return snapshot;
    }

    public long total() {
        return total.get();
    }

    public void reset() {
        counts.values().forEach(LongAdder::reset);
        total.set(0);
    }
}
