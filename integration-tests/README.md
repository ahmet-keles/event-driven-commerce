# integration-tests

End-to-end tests for the order ↔ inventory saga. Everything the per-service
suites cannot see lives here: both services running at once, talking through a
real Kafka broker, each on its own PostgreSQL, with the transactional outboxes
doing the relaying.

This is deliberately an **independent Maven project**, not a module of a root
reactor. It has no Maven dependency on either service — it consumes them as
**built boot jars**, copied into stock `eclipse-temurin:21-jre` containers. So
neither service's build changes, and the services stay black boxes: the tests
drive order-service over REST and raw Kafka, and assert through JDBC into both
databases.

## Running locally

Requirements: Docker and Java 21 (same as the service suites).

```bash
# 1. Build both service jars (skip their tests if you only want e2e)
(cd ../services/order-service && ./mvnw -DskipTests package)
(cd ../services/inventory-service && ./mvnw -DskipTests package)

# 2. Run the suite
./mvnw test
```

The newest jar in each service's `target/` is used. Override with
`-De2e.order-service.jar=…` / `-De2e.inventory-service.jar=…`.

## Topology

Five functional containers per run (plus Testcontainers' Ryuk reaper), on one
throwaway Docker network:

| Container | Image | Role |
|---|---|---|
| `kafka` | `apache/kafka:4.0.0` | broker (KRaft), internal listener `kafka:19092` for the apps, mapped listener for the tests |
| `order-db` | `postgres:17-alpine` | order-service database |
| `inventory-db` | `postgres:17-alpine` | inventory-service database |
| `order-service` | `eclipse-temurin:21-jre` + boot jar | real app; REST + health on mapped 8080 |
| `inventory-service` | `eclipse-temurin:21-jre` + boot jar | real app; no HTTP by design |

The harness pre-creates `order.events` and `inventory.events` (3 partitions,
as in production) before either app starts, because neither service creates
the topic it consumes from.

## Readiness

- order-service: HTTP 200 from `/actuator/health` (it already ships Actuator).
- inventory-service: the `Started InventoryServiceApplication` log line — it
  has no web server, and none is added just for tests.
- Both: a best-effort AdminClient barrier waits until each service's consumer
  group has a partition assignment, so the first test doesn't pay for the
  initial rebalance. Correctness never depends on the barrier — both consumers
  run with `auto-offset-reset=earliest` against a per-run broker, so events
  produced before assignment are replayed, not lost.

## Deliberate divergences from production config

Injected via environment variables only — no service code or config changes:

- `SPRING_KAFKA_CONSUMER_AUTO_OFFSET_RESET=earliest` for order-service
  (production default is `latest`; the production-side fix is tracked with the
  order-consumer idempotency work).
- `APP_OUTBOX_PUBLISH_INTERVAL_MS=100` for both services, so saga hops
  complete quickly.

## Test isolation

- **Unique IDs, no cleanup.** Every test mints fresh UUIDs and every assertion
  is correlation-scoped (`WHERE aggregate_id = …`, records matched by
  envelope content). Nothing truncates or deletes, so the shared stack and
  shared topics are safe.
- **No sleeps.** Awaitility for state waits; `KafkaConsumer.poll` as the wait
  primitive for record capture.
- **No fixed host ports.** Everything runs on Testcontainers-mapped ports.

## Golden wire-contract fixtures

`src/test/resources/fixtures/` holds one canonical envelope per event type
(`ORDER_CREATED`, `ORDER_ITEM_ADDED`, `INVENTORY_RESERVED`,
`INVENTORY_RESERVATION_FAILED`). The contract test compares what each service
really publishes against these files (field names always; values where they
aren't identities), and produces retargeted copies onto the real topics to
prove the consumers accept the documented shape. The envelope and payload
records are declared independently in each service — these fixtures are the
single executable definition of the shared wire format. A contract change must
update the fixture in the same PR, which is exactly the review signal they
exist to create.
