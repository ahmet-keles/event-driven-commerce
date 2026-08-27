package com.ahmetkeles.paymentservice.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
@ConditionalOnProperty(
        name = "app.outbox.publisher-enabled",
        havingValue = "true",
        matchIfMissing = true
)
public class KafkaTopicConfig {

    @Bean
    NewTopic paymentEventsTopic(
            @Value("${app.kafka.payment-events-topic}") String topicName
    ) {
        return TopicBuilder.name(topicName)
                .partitions(3)
                .replicas(1)
                .build();
    }
}
