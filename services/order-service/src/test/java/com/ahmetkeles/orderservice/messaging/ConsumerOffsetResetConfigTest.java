package com.ahmetkeles.orderservice.messaging;

import com.ahmetkeles.orderservice.PostgreSQLIntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Pins the consumer offset-reset policy to {@code earliest}. The Kafka
 * default (latest) would make a fresh order-service consumer group silently
 * skip inventory and payment events published before its first partition
 * assignment — orders left unconfirmed, payment outcomes never applied — so
 * a regression here must fail loudly, not surface as lost events in
 * production. The e2e stack deliberately injects no override for this
 * setting; it runs on the application default this test pins.
 */
class ConsumerOffsetResetConfigTest extends PostgreSQLIntegrationTest {

    @Autowired
    private Environment environment;

    @Test
    void consumerStartsFromEarliestOffsetOnAFreshGroup() {
        assertEquals(
                "earliest",
                environment.getProperty(
                        "spring.kafka.consumer.auto-offset-reset"),
                "order-service must not rely on Kafka's default (latest) "
                        + "offset reset: a fresh consumer group would skip "
                        + "saga events published before its first assignment"
        );
    }
}
