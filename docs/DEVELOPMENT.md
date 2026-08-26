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
compose.yaml                    postgres, inventory-postgres, kafka
.env.example                    template for local secrets/config
docs/                           this documentation
services/order-service/         Spring Boot app, REST API + Kafka
services/inventory-service/     Spring Boot app, Kafka only (no HTTP)
```

## 1. Configure environment variables

```bash
cp .env.example .env
```

Fill in real local values. The file drives both Docker Compose (which reads
`.env` from the repository root automatically) and the services:

| Variable | Used by | Default in code |
|---|---|---|
| `POSTGRES_DB` | compose `postgres`, order-service | none — required |
| `POSTGRES_USER` | compose `postgres`, order-service | none — required |
| `POSTGRES_PASSWORD` | compose `postgres`, order-service | none — required |
| `POSTGRES_HOST` | order-service | `127.0.0.1` |
| `POSTGRES_PORT` | order-service | `5433` |
| `INVENTORY_POSTGRES_PASSWORD` | compose `inventory-postgres`, inventory-service | none — required |
| `INVENTORY_POSTGRES_HOST` | inventory-service | `127.0.0.1` |
| `INVENTORY_POSTGRES_PORT` | inventory-service | `5434` |
| `INVENTORY_POSTGRES_DB` | inventory-service | `inventory` |
| `INVENTORY_POSTGRES_USER` | inventory-service | `inventory_user` |
| `KAFKA_BOOTSTRAP_SERVERS` | both services | `localhost:29092` |

The compose file hardcodes the inventory database name and user (`inventory` /
`inventory_user`), which is why only the password is templated there.

`.env` is git-ignored — never commit it.

## 2. Start the infrastructure

```bash
docker compose up -d
```

That starts three containers:

| Compose service | Container | Host port |
|---|---|---|
| `postgres` | `commerce-postgres` | `5433` → 5432 |
| `inventory-postgres` | `commerce-inventory-postgres` | `5434` → 5432 |
| `kafka` | `commerce-kafka` | `29092` (external listener) |

Kafka runs as a single-node KRaft cluster (broker + controller in one process)
with a default of 3 partitions per topic. Data for all three lives in named
volumes, so it survives `docker compose down`; use
`docker compose down -v` to wipe it.

To bring up only part of the stack, name the services:
`docker compose up -d postgres kafka`.

Check state:

```bash
docker compose ps
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

# read it back — status flips to CONFIRMED once inventory replies
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

## Seeding inventory

There is no API, admin endpoint, or seed migration that creates
`inventory_items` rows. An `ORDER_ITEM_ADDED` event for a product with no row
fails with `InventoryItemNotFoundException`, so insert stock manually before
running the end-to-end flow:

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

# topic contents
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
# order-service
cd services/order-service
./mvnw test

# inventory-service
cd services/inventory-service
./mvnw test
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
| `domain/OrderTest`, `domain/OrderItemTest` | plain unit | Invariants, totals, `confirm()` idempotency, defensive item list |
| `service/OrderServiceTest` | unit (mocks) | Confirming a pending order |
| `messaging/InventoryEventsConsumerTest` | unit (mocks) | `INVENTORY_RESERVED` handling, ignoring other types, rejecting malformed messages |
| `outbox/OutboxPublisherTest` | unit (mocks) | Publish success marks the row; failure leaves it for retry |
| `PostgreSQLIntegrationTest` | Testcontainers base | Starts PostgreSQL; sets `app.kafka.enabled=false` so subclasses need no broker |
| `OrderServiceApplicationTests`, `repository/OrderRepositoryIntegrationTest`, `api/OrderApiIntegrationTest`, `outbox/OrderOutboxIntegrationTest` | Postgres integration | Context load, persistence, full REST surface incl. validation/404s, outbox writes and transactional rollback |
| `outbox/KafkaOutboxIntegrationTest` | Postgres + Kafka | An `ORDER_CREATED` outbox row really reaches `order.events` |

**inventory-service** (`services/inventory-service/src/test`)

| Test | Type | Covers |
|---|---|---|
| `InventoryItemTest` | plain unit | Reservation rules, negative/zero quantities, invalid construction |
| `OrderEventsConsumerTest` | unit (mocks) | `ORDER_ITEM_ADDED` handling, ignoring other types, rejecting malformed messages |
| `InventoryOutboxPublisherTest` | unit (mocks) | Publish success/failure bookkeeping |
| `PostgreSQLIntegrationTest` | Testcontainers base | Starts PostgreSQL; disables listener auto-startup and the outbox publisher so subclasses need no broker |
| `InventoryReservationIntegrationTest` | Postgres integration | Reserve + `processed_events` + outbox in one transaction, duplicate suppression, rollback on insufficient stock |
| `KafkaInventoryIntegrationTest` | Postgres + Kafka | A real `ORDER_ITEM_ADDED` message reserves stock once, even when delivered twice |

### Troubleshooting tests

- *`Could not find a valid Docker environment`* — the Docker daemon is not
  running or your user cannot reach the socket.
- *Image pull timeouts on the first run* — pre-pull with
  `docker pull postgres:17-alpine && docker pull apache/kafka:4.0.0`.
- *Port conflicts* — Testcontainers uses random host ports and does not clash
  with the Compose stack; conflicts on `5433`/`5434`/`29092` come from another
  Compose stack or a local PostgreSQL/Kafka install.

## Building without tests

```bash
cd services/order-service && ./mvnw -DskipTests package
```

Produces an executable jar in `target/`. Each service must be built separately;
there is no parent POM that builds both.
