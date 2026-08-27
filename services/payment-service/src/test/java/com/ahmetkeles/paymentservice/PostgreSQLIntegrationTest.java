package com.ahmetkeles.paymentservice;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;

@SpringBootTest
public abstract class PostgreSQLIntegrationTest {

    static final PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:17-alpine");

    static {
        postgres.start();
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);

        // These tests verify PostgreSQL behavior only.
        // They must not require a running Kafka broker.
        registry.add("spring.kafka.listener.auto-startup", () -> "false");
        registry.add("app.outbox.publisher-enabled", () -> "false");
        // Retention tests drive the job synchronously with their own policy;
        // the scheduled instance must not race them (or any other test).
        registry.add("app.retention.enabled", () -> "false");
    }
}
