package com.ahmetkeles.inventoryservice.outbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Component
@ConditionalOnProperty(
        name = "app.outbox.publisher-enabled",
        havingValue = "true",
        matchIfMissing = true
)
public class OutboxPublisher {

    private static final Logger log =
            LoggerFactory.getLogger(OutboxPublisher.class);

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String topicName;

    public OutboxPublisher(
            OutboxEventRepository outboxEventRepository,
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper,
            @Value("${app.kafka.inventory-events-topic}") String topicName
    ) {
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.topicName = topicName;
    }

    @Scheduled(
            fixedDelayString = "${app.outbox.publish-interval-ms:1000}"
    )
    public void publishPendingEvents() {
        List<OutboxEvent> events =
                outboxEventRepository
                        .findTop100ByPublishedAtIsNullOrderByOccurredAtAsc();

        for (OutboxEvent event : events) {
            publish(event);
        }
    }

    private void publish(OutboxEvent event) {
        try {
            String message = objectMapper.writeValueAsString(
                    PublishedOutboxEvent.from(event)
            );

            kafkaTemplate.send(
                    topicName,
                    event.getAggregateId().toString(),
                    message
            ).get(10, TimeUnit.SECONDS);

            event.markPublished();
            outboxEventRepository.save(event);
        } catch (Exception exception) {
            log.error(
                    "Failed to publish inventory outbox event {} for aggregate {}. Event will remain unpublished for retry.",
                    event.getId(),
                    event.getAggregateId(),
                    exception
            );
        }
    }
}
