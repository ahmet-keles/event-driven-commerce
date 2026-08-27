package com.ahmetkeles.inventoryservice.retention;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.context.properties.bind.validation.BindValidationException;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Proves that dangerous retention configuration refuses to start the
 * application instead of running with it. A negative max age is the worst
 * case: {@code now().minus(-d)} puts the cutoff in the future, which would
 * make essentially every eligible row deletable in a single sweep — so these
 * values must die at binding, not at 3 a.m.
 */
class RetentionPropertiesValidationTest {

    private final ApplicationContextRunner contextRunner =
            new ApplicationContextRunner()
                    .withUserConfiguration(RetentionConfig.class);

    @Configuration
    @EnableConfigurationProperties(RetentionProperties.class)
    static class RetentionConfig {
    }

    @Test
    void defaultsAreValidAndBind() {
        contextRunner.run(context -> {
            assertNull(context.getStartupFailure());

            RetentionProperties properties =
                    context.getBean(RetentionProperties.class);
            assertEquals(Duration.ofDays(7),
                    properties.getPublishedOutboxMaxAge());
            assertEquals(Duration.ofDays(30),
                    properties.getProcessedEventsMaxAge());
            assertEquals(500, properties.getBatchSize());
            assertEquals(10, properties.getMaxBatchesPerRun());
        });
    }

    @Test
    void negativePublishedOutboxMaxAgeFailsStartup() {
        assertRejected("app.retention.published-outbox-max-age=-1d");
    }

    @Test
    void zeroPublishedOutboxMaxAgeFailsStartup() {
        assertRejected("app.retention.published-outbox-max-age=0s");
    }

    @Test
    void negativeProcessedEventsMaxAgeFailsStartup() {
        assertRejected("app.retention.processed-events-max-age=-7d");
    }

    @Test
    void zeroProcessedEventsMaxAgeFailsStartup() {
        assertRejected("app.retention.processed-events-max-age=0s");
    }

    @Test
    void nonPositiveBatchSizeFailsStartup() {
        assertRejected("app.retention.batch-size=0");
        assertRejected("app.retention.batch-size=-5");
    }

    @Test
    void nonPositiveMaxBatchesPerRunFailsStartup() {
        assertRejected("app.retention.max-batches-per-run=0");
        assertRejected("app.retention.max-batches-per-run=-1");
    }

    private void assertRejected(String property) {
        contextRunner
                .withPropertyValues(property)
                .run(context -> {
                    Throwable failure = context.getStartupFailure();

                    assertNotNull(failure,
                            "startup must fail for " + property);
                    assertTrue(hasCause(failure, BindValidationException.class),
                            property + " must fail property validation, "
                                    + "but failed with: " + failure);
                });
    }

    private static boolean hasCause(
            Throwable throwable, Class<? extends Throwable> type) {
        for (Throwable cause = throwable; cause != null;
                cause = cause.getCause()) {
            if (type.isInstance(cause)) {
                return true;
            }
        }
        return false;
    }
}
