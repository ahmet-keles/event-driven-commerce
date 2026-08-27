package com.ahmetkeles.orderservice.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Scheduling is on by default and switched off via app.scheduling.enabled,
 * e.g. by tests that drive scheduled methods explicitly and must not race the
 * immediate first run every fixedDelay task gets at context startup.
 */
@Configuration
@EnableScheduling
@ConditionalOnProperty(
        name = "app.scheduling.enabled",
        havingValue = "true",
        matchIfMissing = true
)
public class SchedulingConfig {
}
