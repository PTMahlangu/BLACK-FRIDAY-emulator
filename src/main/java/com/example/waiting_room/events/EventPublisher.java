package com.example.waiting_room.events;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/**
 * Publishes every shop event to Kafka. The record key is the event type,
 * the value is the event as JSON.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class EventPublisher {

    public static final String TOPIC = "black-friday-events";

    private final KafkaTemplate<String, String> kafka;
    private final JsonMapper json;

    public void publish(ShopEvent event) {
        kafka.send(TOPIC, event.type().name(), json.writeValueAsString(event))
                .exceptionally(ex -> {
                    log.warn("Failed to publish {}: {}", event.type(), ex.getMessage());
                    return null;
                });
    }

    @Configuration
    static class TopicConfig {
        @Bean
        NewTopic blackFridayEvents() {
            return TopicBuilder.name(TOPIC).partitions(3).replicas(1).build();
        }
    }
}
