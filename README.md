# Event-Driven Commerce Platform

A production-style distributed commerce backend built with Java and Spring Boot:
three services that never call each other synchronously and never share a
database, coordinating an order → inventory → payment saga over Apache Kafka
with transactional outboxes, idempotent consumers, and compensating actions.

## Documentation

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — services, responsibilities, databases, the order lifecycle, and the reservation/payment/compensation flows
- [docs/EVENT_FLOW.md](docs/EVENT_FLOW.md) — Kafka topics, the message envelope, every event contract, outbox mechanics, idempotency, retries/DLT, and retention
- [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md) — requirements, environment setup, running the services, and running all test suites
- [docs/order-domain.md](docs/order-domain.md) — order domain model reference
- [integration-tests/README.md](integration-tests/README.md) — the cross-service end-to-end suite

## Architecture

- **order-service** — the only HTTP surface (`/api/orders`): clients assemble an
  order item by item, then explicitly **submit** it. Owns the order state
  machine, emits order events through its outbox, and applies inventory and
  payment outcomes idempotently.
- **inventory-service** — Kafka-only. Reserves stock per order item
  (optimistically locked, idempotent), reports success or failure as events,
  and **releases reservations** when an order cancels.
- **payment-service** — Kafka-only, runs as a Compose container. Charges
  confirmed orders through a **simulated, deterministic gateway**: amounts
  **below 1000.00 approve**, amounts **at or above 1000.00 decline**. Emits
  `PAYMENT_COMPLETED` / `PAYMENT_FAILED`; a decline cancels the order and the
  reserved stock is returned.
- **Apache Kafka** — single-node KRaft broker; topics `order.events`,
  `inventory.events`, `payment.events`, plus a `<topic>.DLT` dead-letter topic
  per consumed topic.
- **PostgreSQL 17** — one database per service (`5433`/`5434`/`5435`), schema
  managed by Flyway, JPA mappings validated at startup.

### The saga

```
create order ──▶ add items ──▶ submit
                                 │
              ORDER_ITEM_ADDED   ▼            per item
order-service ────────────────▶ inventory-service ──▶ INVENTORY_RESERVED
                                                  └─▶ INVENTORY_RESERVATION_FAILED
submitted AND all items reserved ⇒ CONFIRMED ── ORDER_CONFIRMED ──▶ payment-service
any reservation failure          ⇒ CANCELLED                            │
                                                    PAYMENT_COMPLETED ◀─┴─▶ PAYMENT_FAILED
                                                    (order settles)          (order CANCELLED,
                                                                             stock released)
```

An order confirms only when the client has **explicitly submitted it** and
every item is reserved — a fast reservation can never confirm an order that is
still being assembled. Every cancellation (reservation failure or payment
decline) triggers compensation: inventory-service returns each still-held
reservation to the available pool, exactly once.

## Reliability patterns

- **Transactional outbox** in all three services — state change and event
  committed atomically, published by a multi-replica-safe poller
  (`FOR UPDATE SKIP LOCKED`, bounded lock hold, per-order ordering preserved)
- **Idempotent consumers** in all three services — a `processed_events` ledger
  keyed by event id, claimed in the same transaction as the mutation, so
  at-least-once delivery never double-applies an effect
- **Bounded retries + dead-letter topics** — transient errors retry with
  exponential backoff (4 attempts, 500 ms → 5 s); contract violations go
  straight to `<topic>.DLT`; business failures (insufficient stock, declined
  charge) are modelled as events and never dead-letter
- **Optimistic locking** on the order aggregate and inventory rows — concurrent
  `addItem` / `submit` / reservation / cancellation writes serialize into
  exactly one committed ordering; REST callers see `409 concurrent_modification`
- **Deterministic payment idempotency** — the gateway idempotency key is the
  order id, `payments.order_id` is unique, and terminal outcomes are immutable:
  a replayed confirmation can never charge twice
- **Bounded retention** — published outbox rows and expired ledger rows are
  deleted in size-capped, lock-skipping batches; unpublished events are
  structurally undeletable

## Tech Stack

- Java 21, Spring Boot 4.1.1, Spring Kafka
- PostgreSQL 17, Flyway
- Apache Kafka 4.0 (KRaft)
- Docker / Docker Compose / Testcontainers
- Maven (wrapper included per module)

## Local Development

Full instructions, including environment variables, seeding inventory, and
inspecting state, are in [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md).

### Requirements

- Java 21
- Docker (required both for the local stack and for the Testcontainers-based tests)
- Git

### Start the infrastructure

```bash
cp .env.example .env          # then fill in your local values
docker compose up -d --wait   # three databases, kafka, payment-service
```

`--wait` returns once every container is up and each healthcheck passes (the
payment-service container is built from `services/payment-service/Dockerfile`
on first use). Data lives in named volumes and survives
`docker compose down`; use `down -v` to wipe it.

The databases publish on `POSTGRES_PORT` (default `5433`),
`INVENTORY_POSTGRES_PORT` (default `5434`), and `PAYMENT_POSTGRES_PORT`
(default `5435`); Kafka's external listener is on `29092`.

### Run order-service and inventory-service on the host

```bash
cd services/order-service      # same pattern for services/inventory-service

set -a
source ../../.env
set +a

./mvnw spring-boot:run
```

payment-service already runs as a Compose container; inventory-service and
payment-service have no HTTP endpoints — follow their logs. Verify
order-service with:

```bash
curl http://localhost:8080/actuator/health   # {"status":"UP"}
```

### Walk the saga

```bash
# create an order, add an item, then submit it
curl -s -X POST localhost:8080/api/orders \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"11111111-1111-1111-1111-111111111111","currency":"USD"}'

curl -s -X POST localhost:8080/api/orders/<orderId>/items \
  -H 'Content-Type: application/json' \
  -d '{"productId":"<seeded productId>","quantity":2,"unitPrice":"12.50"}'

curl -s -X POST localhost:8080/api/orders/<orderId>/submit
```

Once inventory reserves every item the order confirms, payment runs
(25.00 total → approved; 1000.00+ → declined and the order cancels with its
stock released). Seeding inventory rows is described in
[docs/DEVELOPMENT.md](docs/DEVELOPMENT.md#seeding-inventory).

### Run the tests

Docker must be running — the integration tests start real PostgreSQL and Kafka
containers via Testcontainers.

```bash
(cd services/order-service && ./mvnw test)        # 189 tests
(cd services/inventory-service && ./mvnw test)    # 83 tests
(cd services/payment-service && ./mvnw test)      # 57 tests
```

The cross-service end-to-end suite (16 tests: full saga, compensation,
duplicate delivery, dead-lettering, payment outcomes, wire contracts) builds
all three boot jars and runs them as containers against real Kafka and three
PostgreSQL instances — see
[integration-tests/README.md](integration-tests/README.md).

All four suites run in CI (`.github/workflows/ci.yml`) on Java 21, on every
push to `main` and every pull request.

## Project Status

v1.0 — the order → inventory → payment saga is complete, tested end to end,
and running locally under Docker Compose. Natural next steps: a notification
service, metrics/tracing beyond Actuator health, and cloud deployment.
