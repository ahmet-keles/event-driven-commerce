package com.ahmetkeles.inventoryservice.config;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.BackOff;

/**
 * Central failure policy for every {@code @KafkaListener} in this service.
 *
 * <p>Spring Boot applies the single {@link org.springframework.kafka.listener.CommonErrorHandler}
 * bean found in the context to its auto-configured listener container factory, so
 * declaring the handler here covers all listeners without any per-consumer code.
 *
 * <p>Policy: bounded retries with exponential backoff; once attempts are
 * exhausted the record is published to {@code <topic>.DLT} and the consumer
 * moves on to the next record.
 */
@Configuration
@EnableConfigurationProperties(KafkaRetryProperties.class)
public class KafkaErrorHandlingConfig {

    public static final String DEAD_LETTER_TOPIC_SUFFIX = ".DLT";

    private static final Logger log =
            LoggerFactory.getLogger(KafkaErrorHandlingConfig.class);

    /** Dead-letter topic name for a source topic, e.g. {@code order.events.DLT}. */
    public static String deadLetterTopicFor(String topic) {
        return topic + DEAD_LETTER_TOPIC_SUFFIX;
    }

    /**
     * Publishes exhausted records to the dead-letter topic for their source
     * topic. Partition {@code -1} lets the producer choose, so the dead-letter
     * topic does not have to mirror the source partition count.
     */
    @Bean
    DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(
            KafkaTemplate<String, String> kafkaTemplate
    ) {
        return new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, exception) -> new TopicPartition(
                        deadLetterTopicFor(record.topic()),
                        -1
                )
        );
    }

    @Bean
    DefaultErrorHandler kafkaErrorHandler(
            DeadLetterPublishingRecoverer recoverer,
            KafkaRetryProperties properties
    ) {
        DefaultErrorHandler errorHandler =
                new DefaultErrorHandler(recoverer, backOff(properties));

        errorHandler.setRetryListeners(new LoggingRetryListener());

        return errorHandler;
    }

    private static BackOff backOff(KafkaRetryProperties properties) {
        int retries = Math.max(properties.getAttempts() - 1, 0);

        ExponentialBackOffWithMaxRetries backOff =
                new ExponentialBackOffWithMaxRetries(retries);

        backOff.setInitialInterval(properties.getInitialInterval().toMillis());
        backOff.setMultiplier(properties.getMultiplier());
        backOff.setMaxInterval(properties.getMaxInterval().toMillis());

        return backOff;
    }

    /**
     * Dead-letter topic for the order events this service consumes. Guarded by
     * the same property as the other topic definitions so tests that run without
     * a broker do not attempt topic creation.
     */
    @Bean
    @ConditionalOnProperty(
            name = "app.outbox.publisher-enabled",
            havingValue = "true",
            matchIfMissing = true
    )
    org.apache.kafka.clients.admin.NewTopic orderEventsDeadLetterTopic(
            @Value("${app.kafka.order-events-topic}") String topicName
    ) {
        return org.springframework.kafka.config.TopicBuilder
                .name(deadLetterTopicFor(topicName))
                .partitions(3)
                .replicas(1)
                .build();
    }

    /**
     * Logs each failed delivery attempt and the eventual hand-off to the
     * dead-letter topic, so infrastructure failures stay visible without any
     * try/catch in the business consumers.
     */
    static final class LoggingRetryListener implements RetryListener {

        @Override
        public void failedDelivery(
                ConsumerRecord<?, ?> record,
                Exception exception,
                int deliveryAttempt
        ) {
            log.warn(
                    "Delivery attempt {} failed for {}-{}@{}: {}",
                    deliveryAttempt,
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    exception.getMessage()
            );
        }

        @Override
        public void recovered(
                ConsumerRecord<?, ?> record,
                Exception exception
        ) {
            log.error(
                    "Retries exhausted for {}-{}@{}; publishing to {}",
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    deadLetterTopicFor(record.topic()),
                    exception
            );
        }

        @Override
        public void recoveryFailed(
                ConsumerRecord<?, ?> record,
                Exception original,
                Exception failure
        ) {
            log.error(
                    "Dead-letter publication failed for {}-{}@{}; the record will be redelivered",
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    failure
            );
        }
    }
}
