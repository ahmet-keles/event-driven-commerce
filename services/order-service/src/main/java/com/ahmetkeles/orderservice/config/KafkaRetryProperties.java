package com.ahmetkeles.orderservice.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Consumer failure policy for {@code @KafkaListener} methods.
 *
 * <p>Spring Kafka's out-of-the-box policy is 10 delivery attempts with no delay,
 * after which the record is logged and dropped. These properties replace that
 * with a bounded, backed-off policy whose exhausted records are published to a
 * dead-letter topic.
 */
@ConfigurationProperties(prefix = "app.kafka.retry")
public class KafkaRetryProperties {

    /**
     * Total number of delivery attempts, including the first one.
     * A value of 1 disables retries and sends the first failure straight to the
     * dead-letter topic.
     */
    private int attempts = 4;

    /** Delay before the second delivery attempt. */
    private Duration initialInterval = Duration.ofMillis(500);

    /** Factor applied to the delay after each failed attempt. */
    private double multiplier = 2.0;

    /** Upper bound for the delay between attempts. */
    private Duration maxInterval = Duration.ofSeconds(5);

    public int getAttempts() {
        return attempts;
    }

    public void setAttempts(int attempts) {
        this.attempts = attempts;
    }

    public Duration getInitialInterval() {
        return initialInterval;
    }

    public void setInitialInterval(Duration initialInterval) {
        this.initialInterval = initialInterval;
    }

    public double getMultiplier() {
        return multiplier;
    }

    public void setMultiplier(double multiplier) {
        this.multiplier = multiplier;
    }

    public Duration getMaxInterval() {
        return maxInterval;
    }

    public void setMaxInterval(Duration maxInterval) {
        this.maxInterval = maxInterval;
    }
}
