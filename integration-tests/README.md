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

Requirements: Docker and Java 21 (same as the service suites). Testcontainers
is pinned to the same 2.x version Spring Boot manages for the services, so any
engine that works for the service suites — Docker 29+ included — works here,
with no `DOCKER_API_VERSION` overrides.

```bash
# 1. Build the service jars (skip their tests if you only want e2e)
(cd ../services/order-service && ./mvnw -DskipTests package)
(cd ../services/inventory-service && ./mvnw -DskipTests package)
(cd ../services/payment-service && ./mvnw -DskipTests package)

# 2. Run the suite
./mvnw test
```

The newest jar in each service's `target/` is used. Override with
`-De2e.order-service.jar=…` / `-De2e.inventory-service.jar=…` /
`-De2e.payment-service.jar=…`.

## Topology

Seven functional containers per run (plus Testcontainers' Ryuk reaper), on one
throwaway Docker network:

| Container | Image | Role |
|---|---|---|
| `kafka` | `apache/kafka:4.0.0` | broker (KRaft), internal listener `kafka:19092` for the apps, mapped listener for the tests |
| `order-db` | `postgres:17-alpine` | order-service database |
| `inventory-db` | `postgres:17-alpine` | inventory-service database |
| `order-service` | `eclipse-temurin:21-jre` + boot jar | real app; REST + health on mapped 8080 |
| `inventory-service` | `eclipse-temurin:21-jre` + boot jar | real app; no HTTP by design |
| `payment-db` | `postgres:17-alpine` | payment-service database |
| `payment-service` | `eclipse-temurin:21-jre` + boot jar | real app; no HTTP by design; SIMULATED gateway (amounts >= 1000.00 decline, below approve) |

The harness pre-creates `order.events`, `inventory.events`, and
`payment.events` (3 partitions,
as in production) before either app starts, because neither service creates
the topic it consumes from. The `.DLT` dead-letter topics are declared by the
services themselves and appear at app startup; nothing in this suite produces
to them — dead-letter scenarios belong to the retry/DLT test group.

## Readiness

- order-service: HTTP 200 from `/actuator/health` (it already ships Actuator).
- inventory-service and payment-service: their `Started …Application` log
  lines — they have no web servers, and none are added just for tests.
- Both: a best-effort AdminClient barrier waits until each service's consumer
  group has a partition assignment, so the first test doesn't pay for the
  initial rebalance. Correctness never depends on the barrier — both consumers
  run with `auto-offset-reset=earliest` against a per-run broker, so events
  produced before assignment are replayed, not lost.

## Deliberate divergences from production config

Injected via environment variables only — no service code or config changes:

- `SPRING_KAFKA_CONSUMER_AUTO_OFFSET_RESET=earliest` for order-service.
  Production config still leaves this unset (Kafka default `latest`), even
  after the consumer idempotency ledger landed — a fresh production consumer
  group would skip history. Flagged as a standalone config gap; the harness
  papers over it for tests only.
- `APP_OUTBOX_PUBLISH_INTERVAL_MS=100` for all three services, so saga hops
  complete quickly.

Payment outcomes are driven through the simulated gateway's documented
contract, not by configuration overrides: order totals below 1000.00 approve,
totals at or above it decline.

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
(`ORDER_CREATED`, `ORDER_ITEM_ADDED`, `ORDER_CONFIRMED`, `ORDER_CANCELLED`,
`INVENTORY_RESERVED`, `INVENTORY_RESERVATION_FAILED`, `PAYMENT_COMPLETED`,
`PAYMENT_FAILED`). The contract test compares what each service
really publishes against these files (field names always; values where they
aren't identities), and produces retargeted copies onto the real topics to
prove the consumers accept the documented shape. The envelope and payload
records are declared independently in each service — these fixtures are the
single executable definition of the shared wire format. A contract change must
update the fixture in the same PR, which is exactly the review signal they
exist to create.
