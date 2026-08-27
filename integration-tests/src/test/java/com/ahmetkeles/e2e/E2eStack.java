package com.ahmetkeles.e2e;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

/**
 * The shared five-container stack: Kafka, one Postgres per service, and both
 * services running as black-box containers built from their boot jars. Started
 * once per JVM on first use and shared by every test class; tests isolate by
 * unique UUIDs, never by cleanup, so sharing is safe.
 *
 * <p>Two deliberate divergences from production configuration, both injected
 * via environment so no service code changes:
 * <ul>
 *   <li>{@code SPRING_KAFKA_CONSUMER_AUTO_OFFSET_RESET=earliest} for
 *       order-service (production default is {@code latest}): a fresh consumer
 *       group must not skip events produced before its first partition
 *       assignment.</li>
 *   <li>{@code APP_OUTBOX_PUBLISH_INTERVAL_MS=100} for both services, so each
 *       saga hop completes in well under a second.</li>
 * </ul>
 */
final class E2eStack {

    static final String ORDER_TOPIC = "order.events";
    static final String INVENTORY_TOPIC = "inventory.events";
    static final String PAYMENT_TOPIC = "payment.events";

    private static final Logger log = LoggerFactory.getLogger(E2eStack.class);

    private static final String KAFKA_IMAGE = "apache/kafka:4.0.0";
    private static final String POSTGRES_IMAGE = "postgres:17-alpine";
    private static final String JRE_IMAGE = "eclipse-temurin:21-jre";

    private static final String INTERNAL_BOOTSTRAP = "kafka:19092";
    private static final String DB_PASSWORD = "e2e";
    private static final int TOPIC_PARTITIONS = 3;

    private static final String ORDER_GROUP = "order-service";
    private static final String INVENTORY_GROUP = "inventory-service";

    private static E2eStack instance;

    final KafkaContainer kafka;
    final PostgreSQLContainer<?> orderDb;
    final PostgreSQLContainer<?> inventoryDb;
    final GenericContainer<?> orderApp;
    final GenericContainer<?> inventoryApp;

    static synchronized E2eStack get() {
        if (instance == null) {
            instance = new E2eStack();
        }
        return instance;
    }

    private E2eStack() {
        Network network = Network.newNetwork();

        kafka = new KafkaContainer(DockerImageName.parse(KAFKA_IMAGE))
                .withListener(INTERNAL_BOOTSTRAP)
                .withNetwork(network)
                .withNetworkAliases("kafka");

        orderDb = new PostgreSQLContainer<>(POSTGRES_IMAGE)
                .withDatabaseName("commerce")
                .withUsername("commerce_user")
                .withPassword(DB_PASSWORD)
                .withNetwork(network)
                .withNetworkAliases("order-db");

        inventoryDb = new PostgreSQLContainer<>(POSTGRES_IMAGE)
                .withDatabaseName("inventory")
                .withUsername("inventory_user")
                .withPassword(DB_PASSWORD)
                .withNetwork(network)
                .withNetworkAliases("inventory-db");

        Startables.deepStart(List.of(kafka, orderDb, inventoryDb)).join();

        // Neither service creates the topic it consumes from (each declares
        // only its outbound topic), so the harness creates both up front to
        // remove the startup race.
        createTopics();

        orderApp = appContainer(network, "order-service", Map.of(
                "POSTGRES_HOST", "order-db",
                "POSTGRES_PORT", "5432",
                "POSTGRES_DB", "commerce",
                "POSTGRES_USER", "commerce_user",
                "POSTGRES_PASSWORD", DB_PASSWORD,
                "KAFKA_BOOTSTRAP_SERVERS", INTERNAL_BOOTSTRAP,
                "APP_OUTBOX_PUBLISH_INTERVAL_MS", "100",
                "SPRING_KAFKA_CONSUMER_AUTO_OFFSET_RESET", "earliest"
        ))
                .withExposedPorts(8080)
                .waitingFor(Wait.forHttp("/actuator/health")
                        .forPort(8080)
                        .forStatusCode(200)
                        .withStartupTimeout(Duration.ofMinutes(3)));

        // inventory-service has no web server, so no health endpoint to probe;
        // the Started line proves the context (listeners included) is up, and
        // the group barrier below covers the subscription race.
        inventoryApp = appContainer(network, "inventory-service", Map.of(
                "INVENTORY_POSTGRES_HOST", "inventory-db",
                "INVENTORY_POSTGRES_PORT", "5432",
                "INVENTORY_POSTGRES_DB", "inventory",
                "INVENTORY_POSTGRES_USER", "inventory_user",
                "INVENTORY_POSTGRES_PASSWORD", DB_PASSWORD,
                "KAFKA_BOOTSTRAP_SERVERS", INTERNAL_BOOTSTRAP,
                "APP_OUTBOX_PUBLISH_INTERVAL_MS", "100"
        ))
                .waitingFor(Wait.forLogMessage(
                        ".*Started InventoryServiceApplication.*\\n", 1)
                        .withStartupTimeout(Duration.ofMinutes(3)));

        Startables.deepStart(List.of(orderApp, inventoryApp)).join();

        awaitConsumerGroupsAssigned();
    }

    String bootstrapServers() {
        return kafka.getBootstrapServers();
    }

    String orderApiBaseUrl() {
        return "http://" + orderApp.getHost() + ":" + orderApp.getMappedPort(8080);
    }

    private GenericContainer<?> appContainer(
            Network network,
            String service,
            Map<String, String> env
    ) {
        return new GenericContainer<>(DockerImageName.parse(JRE_IMAGE))
                .withNetwork(network)
                .withNetworkAliases(service)
                .withCopyFileToContainer(
                        MountableFile.forHostPath(resolveBootJar(service)),
                        "/app.jar")
                .withEnv(env)
                .withCommand("java", "-jar", "/app.jar")
                .withLogConsumer(new Slf4jLogConsumer(
                        LoggerFactory.getLogger("container." + service)));
    }

    /**
     * The services are consumed as built jar files, not Maven dependencies.
     * Override with -De2e.&lt;service&gt;.jar=/path/to.jar; by default the
     * newest boot jar in the service's target directory is used.
     */
    private static Path resolveBootJar(String service) {
        String override = System.getProperty("e2e." + service + ".jar");
        if (override != null && !override.isBlank()) {
            return Path.of(override);
        }

        Path target = Path.of("..", "services", service, "target");
        try (Stream<Path> files = Files.list(target)) {
            return files
                    .filter(path -> {
                        String name = path.getFileName().toString();
                        return name.endsWith(".jar")
                                && !name.endsWith(".jar.original")
                                && !name.contains("-sources")
                                && !name.contains("-javadoc");
                    })
                    .max((a, b) -> {
                        try {
                            return Files.getLastModifiedTime(a)
                                    .compareTo(Files.getLastModifiedTime(b));
                        } catch (IOException exception) {
                            return 0;
                        }
                    })
                    .orElseThrow(() -> missingJar(service, target));
        } catch (IOException exception) {
            throw missingJar(service, target);
        }
    }

    private static IllegalStateException missingJar(String service, Path target) {
        return new IllegalStateException(
                "No boot jar found in " + target.toAbsolutePath().normalize()
                        + ". Build it first: (cd services/" + service
                        + " && ./mvnw -DskipTests package)");
    }

    private void createTopics() {
        try (Admin admin = Admin.create(adminProperties())) {
            try {
                admin.createTopics(List.of(
                        new NewTopic(ORDER_TOPIC, TOPIC_PARTITIONS, (short) 1),
                        new NewTopic(INVENTORY_TOPIC, TOPIC_PARTITIONS, (short) 1),
                        new NewTopic(PAYMENT_TOPIC, TOPIC_PARTITIONS, (short) 1)
                )).all().get();
            } catch (ExecutionException exception) {
                if (!(exception.getCause() instanceof TopicExistsException)) {
                    throw new IllegalStateException(
                            "Failed to create topics", exception);
                }
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(
                        "Interrupted while creating topics", exception);
            }
        }
    }

    /**
     * Best-effort barrier: wait until both services' consumer groups have a
     * member with a partition assignment, so the first test doesn't spend its
     * budget on the initial rebalance. Correctness does not depend on it (both
     * consumers run with earliest offset reset against a per-run broker, so
     * pre-assignment events are replayed, never lost), which is why exhausting
     * the barrier logs instead of failing.
     */
    private void awaitConsumerGroupsAssigned() {
        long deadline = System.currentTimeMillis() + 60_000;

        try (Admin admin = Admin.create(adminProperties())) {
            while (System.currentTimeMillis() < deadline) {
                if (groupAssigned(admin, ORDER_GROUP)
                        && groupAssigned(admin, INVENTORY_GROUP)) {
                    return;
                }
            }
        }

        log.warn("Consumer groups not visibly assigned within 60s; "
                + "continuing (earliest offset reset makes this safe)");
    }

    private static boolean groupAssigned(Admin admin, String groupId) {
        try {
            ConsumerGroupDescription description = admin
                    .describeConsumerGroups(List.of(groupId))
                    .describedGroups()
                    .get(groupId)
                    .get();

            return description.members().stream().anyMatch(member ->
                    !member.assignment().topicPartitions().isEmpty());
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            return false;
        } catch (Exception exception) {
            // Group not registered yet — keep polling until the deadline.
            return false;
        }
    }

    private Properties adminProperties() {
        Properties properties = new Properties();
        properties.put(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers());
        return properties;
    }
}
