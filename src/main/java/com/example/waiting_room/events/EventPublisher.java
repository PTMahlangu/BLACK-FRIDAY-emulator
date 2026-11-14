package com.example.waiting_room.events;

import com.example.waiting_room.config.KafkaTopics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/**
 * Publishes every shop event to Kafka, fire-and-forget.
 * Record key = event type, value = the event as JSON.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class EventPublisher {

    private final KafkaTemplate<String, String> kafka;
    private final JsonMapper json;

    public void publish(ShopEvent event) {
        kafka.send(KafkaTopics.SHOP_EVENTS, event.type().name(), json.writeValueAsString(event))
                .exceptionally(ex -> {
                    log.warn("Failed to publish {}: {}", event.type(), ex.getMessage());
                    return null;
                });
    }
}
