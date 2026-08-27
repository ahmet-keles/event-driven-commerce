# Local Development

Everything below reflects the current repository layout: two independent Maven
projects under `services/`, no root aggregator POM, and one `compose.yaml` for
the shared infrastructure.

## Requirements

| Requirement | Why |
|---|---|
| **Java 21** | Both `pom.xml` files set `<java.version>21</java.version>` and inherit the Spring Boot `4.1.1` parent, whose baseline is Java 21. The build will not run on an older JDK. |
| **Docker** (Docker Desktop, Colima, or any Docker-API-compatible engine) | Runs PostgreSQL and Kafka locally via Compose, **and** is required for the test suites — the integration tests use Testcontainers and start real containers. |
| **Git** | Source control. |

No local Maven installation is needed: use the `./mvnw` wrapper inside each
service directory.

Check your toolchain:

```bash
java -version    # must report 21 (or the JDK your JAVA_HOME points at)
docker info      # must succeed; the daemon has to be running
```

## Repository layout

```
compose.yaml                    postgres, inventory-postgres, payment-postgres, kafka
.env.example                    template for local secrets/config
.github/workflows/ci.yml        CI: both test suites on Java 21
docs/                           this documentation
services/order-service/         Spring Boot app, REST API + Kafka
services/inventory-service/     Spring Boot app, Kafka only (no HTTP)
services/payment-service/       Spring Boot app, Kafka only (no HTTP)
```

## 1. Configure environment variables

```bash
cp .env.example .env
```

Fill in real local values. `.env` is the single source of truth shared by
Docker Compose (which reads it from the repository root automatically) and by
the services when you run them on the host. Every variable in
`.env.example` is listed below:

| Variable | Used by | Behavior if unset |
|---|---|---|
| `POSTGRES_DB` | compose `postgres`, order-service | **Compose fails fast** (`:?` guard); order-service has no default either |
| `POSTGRES_USER` | compose `postgres`, order-service | **Compose fails fast**; no default |
| `POSTGRES_PASSWORD` | compose `postgres`, order-service | **Compose fails fast**; no default |
| `POSTGRES_HOST` | order-service | `127.0.0.1` |
| `POSTGRES_PORT` | compose host port mapping, order-service | `5433` on both sides |
| `INVENTORY_POSTGRES_DB` | compose `inventory-postgres`, inventory-service | `inventory` |
| `INVENTORY_POSTGRES_USER` | compose `inventory-postgres`, inventory-service | `inventory_user` |
| `INVENTORY_POSTGRES_PASSWORD` | compose `inventory-postgres`, inventory-service | **Compose fails fast**; no default |
| `INVENTORY_POSTGRES_HOST` | inventory-service | `127.0.0.1` |
| `INVENTORY_POSTGRES_PORT` | compose host port mapping, inventory-service | `5434` on both sides |
| `PAYMENT_POSTGRES_DB` | compose `payment-postgres`, payment-service | `payment` |
| `PAYMENT_POSTGRES_USER` | compose `payment-postgres`, payment-service | `payment_user` |
| `PAYMENT_POSTGRES_PASSWORD` | compose `payment-postgres`, payment-service | **Compose fails fast**; no default |
| `PAYMENT_POSTGRES_HOST` | payment-service on the host | `127.0.0.1` |
| `PAYMENT_POSTGRES_PORT` | compose host port mapping | `5435` on both sides |
| `KAFKA_BOOTSTRAP_SERVERS` | all services | `localhost:29092` |

The three passwords/identities marked *fails fast* use Compose's
`${VAR:?message}` form, so `docker compose up` stops with a readable error
instead of starting a half-configured database. Everything else uses
`${VAR:-default}`, so the values above apply when the variable is absent.
Changing `POSTGRES_PORT` or `INVENTORY_POSTGRES_PORT` moves both the published
container port and the service's JDBC URL, keeping them in step.

`.env` is git-ignored — never commit it.

## 2. Start the infrastructure

```bash
docker compose up -d --wait
```

`--wait` blocks until every container reports healthy, so the services you start
next never race a database that is still running `initdb`. That starts the
containers below on the `commerce-net` bridge network, all with
`restart: unless-stopped`:

| Compose service | Container | Host port | Healthcheck |
|---|---|---|---|
| `postgres` | `commerce-postgres` | `${POSTGRES_PORT:-5433}` → 5432 | `pg_isready` over TCP, 15 s start period |
| `inventory-postgres` | `commerce-inventory-postgres` | `${INVENTORY_POSTGRES_PORT:-5434}` → 5432 | `pg_isready` over TCP, 15 s start period |
| `payment-postgres` | `commerce-payment-postgres` | `${PAYMENT_POSTGRES_PORT:-5435}` → 5432 | `pg_isready` over TCP, 15 s start period |
| `payment-service` | `commerce-payment-service` | — (no HTTP surface) | none — gated by `depends_on: service_healthy` on `payment-postgres` and `kafka` |
| `kafka` | `commerce-kafka` | `29092` (EXTERNAL listener) | `kafka-broker-api-versions.sh`, 30 s start period |

The PostgreSQL healthchecks probe `127.0.0.1:5432` rather than the unix socket
on purpose: during `initdb` the bootstrap server listens on the socket only, so
a socket probe would report ready before the database accepts client
connections.

Kafka runs as a single-node KRaft cluster (broker + controller in one process)
with 3 default partitions per topic, and `KAFKA_LOG_DIRS` pointed at the
`kafka_data` volume — so **topics and consumer offsets survive
`docker compose down`** along with all databases' data. Use
`docker compose down -v` to wipe every volume and start clean. The broker
gets a 30 s `stop_grace_period` for an orderly shutdown.

To bring up only part of the stack, name the services:
`docker compose up -d --wait postgres kafka`.

Check state:

```bash
docker compose ps          # includes each container's health status
docker compose logs -f kafka
```

## 3. Run order-service

```bash
cd services/order-service

set -a
source ../../.env
set +a

./mvnw spring-boot:run
```

Flyway applies `V1`/`V2` on startup and Hibernate validates the mappings against
the result. The API listens on `http://localhost:8080`.

Health check:

```bash
curl http://localhost:8080/actuator/health
# {"status":"UP"}
```

Exercise the API:

```bash
# create an order
curl -s -X POST http://localhost:8080/api/orders \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"11111111-1111-1111-1111-111111111111","currency":"USD"}'

# add an item (drives the inventory reservation flow)
curl -s -X POST http://localhost:8080/api/orders/<ORDER_ID>/items \
  -H 'Content-Type: application/json' \
  -d '{"productId":"22222222-2222-2222-2222-222222222222","quantity":2,"unitPrice":9.99}'

# read it back — CONFIRMED once inventory reserves, CANCELLED if it cannot
curl -s http://localhost:8080/api/orders/<ORDER_ID>
```

## 4. Run inventory-service

In a second terminal:

```bash
cd services/inventory-service

set -a
source ../../.env
set +a

./mvnw spring-boot:run
```

This service has **no HTTP endpoint and no actuator** — it is a Kafka
consumer/publisher process. Verify it by watching its log (`Reserved inventory
for order …`) or by querying its database.

## 5. payment-service

Unlike the other two services, payment-service **runs as a Compose container by
default** — `docker compose up -d --wait` builds it from
`services/payment-service/Dockerfile` (multi-stage: the boot jar is built
inside the image, so no host toolchain is involved) and starts it once
`payment-postgres` and `kafka` are healthy. Inside the network it talks to
`payment-postgres:5432` and the broker's internal listener `kafka:9092`;
the `PAYMENT_POSTGRES_*` variables from `.env` apply to both the container
and a host run.

For a faster dev loop you can run it on the host instead — stop the container
first so two instances don't compete in the same consumer group:

```bash
docker compose stop payment-service

cd services/payment-service

set -a
source ../../.env
set +a

./mvnw spring-boot:run
```

Like inventory-service, this is a Kafka consumer/publisher process with **no
HTTP endpoint and no actuator** — which is also why its container declares no
healthcheck: there is nothing meaningful to probe, so its readiness ordering
comes from `depends_on` and its liveness from
`docker compose logs -f payment-service`. It consumes `ORDER_CONFIRMED` from
`order.events`, charges through the **simulated** gateway, records the result
in its `payments` table (at most one payment per order, enforced by a unique
constraint), and publishes `PAYMENT_COMPLETED` or `PAYMENT_FAILED` to
`payment.events` — which order-service consumes and deduplicates by eventId.

The gateway is deterministic and never touches real money: totals **strictly
below** `app.payment.gateway.decline-threshold` (default `1000.00`) are
approved, totals at or above it are declined. So a `2 × 9.99` order pays
successfully, while a single item at `1000.00` produces a `PAYMENT_FAILED`
with a decline reason — useful for exercising both saga branches on purpose.

Verify it by watching its log (`Recorded completed payment …` /
`Recorded failed payment …` on the order-service side) or by querying its
database (see below).

## Seeding inventory

There is no API, admin endpoint, or seed migration that creates
`inventory_items` rows. An `ORDER_ITEM_ADDED` event for a product with no row
now produces an `INVENTORY_RESERVATION_FAILED` event with
`reason=INVENTORY_ITEM_NOT_FOUND`, and the order is **cancelled** rather than
confirmed — so insert stock before running the happy-path flow:

```bash
docker exec -it commerce-inventory-postgres \
  psql -U inventory_user -d inventory -c \
  "INSERT INTO inventory_items (product_id, available_quantity, reserved_quantity, version, updated_at)
   VALUES ('22222222-2222-2222-2222-222222222222', 100, 0, 0, now());"
```

Use the same `productId` in the `POST /api/orders/{id}/items` request above.

## Inspecting state

```bash
# orders and their outbox
docker exec -it commerce-postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -c "SELECT id, status, total_amount FROM orders ORDER BY created_at DESC LIMIT 5;" \
  -c "SELECT event_type, published_at FROM outbox_events ORDER BY occurred_at DESC LIMIT 10;"

# inventory, de-duplication log and outbox
docker exec -it commerce-inventory-postgres psql -U inventory_user -d inventory \
  -c "SELECT * FROM inventory_items;" \
  -c "SELECT event_id, event_type FROM processed_events ORDER BY processed_at DESC LIMIT 10;"

# payments and the payment outbox
docker exec -it commerce-payment-postgres psql -U payment_user -d payment \
  -c "SELECT order_id, amount, status, failure_reason FROM payments ORDER BY created_at DESC LIMIT 5;" \
  -c "SELECT event_type, published_at FROM outbox_events ORDER BY occurred_at DESC LIMIT 10;"

# topic contents (also works for inventory.events and payment.events)
docker exec -it commerce-kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic order.events --from-beginning
```

## Running the test suites

Both suites are independent; run them from their own service directory. **Docker
must be running** — Testcontainers starts `postgres:17-alpine` and
`apache/kafka:4.0.0` containers per test class that needs them, and the first
run pulls those images. The tests do **not** use `.env` or the Compose stack:
each test registers its container's JDBC URL and bootstrap servers through
`@DynamicPropertySource`, so no environment variables are required.

```bash
# from the repository root, one suite at a time
(cd services/order-service && ./mvnw test)
(cd services/inventory-service && ./mvnw test)
(cd services/payment-service && ./mvnw test)
```

Run a single class or method with the Surefire filter:

```bash
./mvnw test -Dtest=OrderApiIntegrationTest
./mvnw test -Dtest='OrderTest#addItemUpdatesTotal'
```

### What each suite covers

**order-service** (`services/order-service/src/test`)

| Test | Type | Covers |
|---|---|---|
| `domain/OrderTest`, `domain/OrderItemTest` | plain unit | Invariants, totals, `confirm()`/`cancel()` idempotency, terminal-state guards (a confirmed order is not cancelled and vice versa), defensive item list |
| `service/OrderServiceTest` | unit (mocks) | Confirming and cancelling a pending order, cancellation idempotency, cancelling an unknown order |
| `messaging/InventoryEventsConsumerTest` | unit (mocks) | `INVENTORY_RESERVED` and `INVENTORY_RESERVATION_FAILED` handling (including the `INVENTORY_ITEM_NOT_FOUND` reason), ignoring other types, rejecting malformed messages |
| `outbox/OutboxPublisherTest` | unit (mocks) | Publish success marks the row; failure leaves it for retry |
| `PostgreSQLIntegrationTest` | Testcontainers base | Starts PostgreSQL; sets `app.kafka.enabled=false` so subclasses need no broker |
| `OrderServiceApplicationTests`, `repository/OrderRepositoryIntegrationTest`, `api/OrderApiIntegrationTest`, `outbox/OrderOutboxIntegrationTest` | Postgres integration | Context load, persistence, full REST surface incl. validation/404s, outbox writes and transactional rollback |
| `outbox/KafkaOutboxIntegrationTest` | Postgres + Kafka | An `ORDER_CREATED` outbox row really reaches `order.events` |

**inventory-service** (`services/inventory-service/src/test`)

| Test | Type | Covers |
|---|---|---|
| `InventoryItemTest` | plain unit | Reservation rules, negative/zero quantities, invalid construction |
| `OrderEventsConsumerTest` | unit (mocks) | `ORDER_ITEM_ADDED` handling, ignoring other types, rejecting malformed messages |
| `InventoryReservationServiceTest` | unit (mocks) | Duplicate events short-circuit before any reservation attempt; insufficient stock and unknown items each write exactly one `INVENTORY_RESERVATION_FAILED` event; success still writes `INVENTORY_RESERVED` |
| `InventoryOutboxPublisherTest` | unit (mocks) | Publish success/failure bookkeeping |
| `PostgreSQLIntegrationTest` | Testcontainers base | Starts PostgreSQL; disables listener auto-startup and the outbox publisher so subclasses need no broker |
| `InventoryReservationIntegrationTest` | Postgres integration | Reserve + `processed_events` + outbox in one transaction, duplicate suppression, and a committed `INVENTORY_RESERVATION_FAILED` row for both insufficient stock and unknown items |
| `KafkaInventoryIntegrationTest` | Postgres + Kafka | A real `ORDER_ITEM_ADDED` message reserves stock once, even when delivered twice |

### Troubleshooting tests

- *`Could not find a valid Docker environment`* — the Docker daemon is not
  running or your user cannot reach the socket. If the attempted-configuration
  detail shows *`client version 1.32 is too old. Minimum supported API version
  is 1.40`*, the daemon is fine — the failure means an old Testcontainers 1.x
  client: Docker 29 raised the daemon's minimum API version above what 1.x
  pins. All modules here use Testcontainers 2.x, which negotiates the API
  version, so no `DOCKER_API_VERSION` / `DOCKER_MIN_API_VERSION` override is
  ever needed; if you see this error, the module is somehow resolving an old
  Testcontainers — check `./mvnw dependency:list -DincludeGroupIds=org.testcontainers`.
- *Image pull timeouts on the first run* — pre-pull with
  `docker pull postgres:17-alpine && docker pull apache/kafka:4.0.0`
  (plus `docker pull eclipse-temurin:21-jre` for the e2e suite).
- *Port conflicts* — Testcontainers uses random host ports and does not clash
  with the Compose stack; conflicts on `5433`/`5434`/`29092` come from another
  Compose stack or a local PostgreSQL/Kafka install.

## Continuous integration

`.github/workflows/ci.yml` runs on every push to `main` and every pull request
targeting `main`:

- A matrix job per service (`inventory-service`, `order-service`,
  `payment-service`) on
  `ubuntu-latest`, with `fail-fast: false` so both results are always reported —
  the workflow still fails if either service fails.
- An independent `e2e` job that builds both boot jars and runs the
  cross-service suite in `integration-tests/`.
- `actions/setup-java@v4` with the Temurin distribution, `java-version: '21'`,
  and a per-job Maven cache: each matrix leg keys on its own service's
  `pom.xml`, and the `e2e` job keys on all three `pom.xml` files since it
  builds both services and the e2e module. Cache entries are immutable per
  key, so jobs that run in parallel deliberately do not share a key.
- `.github/scripts/prepare-docker.sh` before the tests: fails fast with a
  readable error when the Docker daemon is unusable, logs the daemon's version
  and API range, and pre-pulls the test images with retries so a registry
  hiccup or Docker Hub rate limit surfaces as an attributed pull failure
  instead of a Testcontainers startup error buried in a test log.
- `./mvnw -B --no-transfer-progress test` in the module directory — the same
  command you run locally.
- 30-minute timeout per job and read-only `contents` permission.

No further Docker setup is needed: `ubuntu-latest` runners ship with a running
Docker daemon, which is all Testcontainers requires to start the PostgreSQL and
Kafka containers. CI does **not** use `compose.yaml` or `.env`.

Because CI runs exactly the local command, a green `./mvnw test` in both service
directories is a reliable predictor of a green pipeline.

## Building without tests

```bash
cd services/order-service && ./mvnw -DskipTests package
```

Produces an executable jar in `target/`. Each service must be built separately;
there is no parent POM that builds both.
