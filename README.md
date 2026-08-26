# Event-Driven Commerce Platform

A production-style distributed commerce backend built with Java and Spring Boot.

This project is designed to demonstrate backend engineering, event-driven architecture, distributed systems, database design, containerization, observability, and cloud deployment.

## Documentation

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — services, responsibilities, databases, order lifecycle, inventory reservation flow, and what is implemented vs. planned
- [docs/EVENT_FLOW.md](docs/EVENT_FLOW.md) — Kafka topics, message envelope, event types, the transactional outbox, idempotency, and failure behavior
- [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md) — requirements, environment setup, running both services, and running both test suites
- [docs/order-domain.md](docs/order-domain.md) — order domain model reference

## Current Architecture

Implemented and running today:

- **Order Service** — REST API (`/api/orders`), PostgreSQL persistence, transactional outbox, Kafka producer and consumer, Actuator health
- **Inventory Service** — Kafka consumer/producer with its own PostgreSQL database, idempotent stock reservation, transactional outbox (no HTTP API)
- **Apache Kafka** — single-node KRaft broker; topics `order.events` and `inventory.events`
- **PostgreSQL** — one database per service, schema managed by Flyway
- **Docker Compose** — local infrastructure with healthchecks and persistent volumes
- **Testcontainers** — integration tests against real PostgreSQL and Kafka
- **GitHub Actions CI** — both test suites on Java 21, on every push to `main` and every pull request

The end-to-end flow that works today: adding an item to an order publishes
`ORDER_ITEM_ADDED`; the inventory service either reserves stock and publishes
`INVENTORY_RESERVED` — confirming the order — or publishes
`INVENTORY_RESERVATION_FAILED`, which cancels it.

## Planned Services

- Payment Service (not implemented)
- Notification Service (not implemented)

## Planned Infrastructure

- Redis
- AWS
- Continuous deployment (CI is already in place)

## Engineering Goals

Already in place:

- Event-driven communication between services
- Transactional outbox pattern
- Idempotent event consumers (inventory service)
- Reservation-failure events and order cancellation
- Integration testing with Testcontainers
- Continuous integration on every push and pull request

Still to come:

- Multi-item reservation semantics and releasing reserved stock
- Saga-style workflows and compensating actions
- Retry and dead-letter handling
- Distributed caching
- Observability and metrics beyond Actuator health
- Load testing
- Payment and notification services
- Cloud deployment

## Tech Stack

- Java 21
- Spring Boot 4.1.1
- PostgreSQL 17
- Apache Kafka 4.0
- Flyway
- Docker / Docker Compose / Testcontainers
- Maven (wrapper included per service)

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
docker compose up -d --wait   # postgres (5433), inventory-postgres (5434), kafka (29092)
```

`--wait` returns once all three containers report healthy. Data lives in named
volumes and survives `docker compose down`; use `down -v` to wipe it.

### Run Order Service

```bash
cd services/order-service

set -a
source ../../.env
set +a

./mvnw spring-boot:run
```

### Run Inventory Service

```bash
cd services/inventory-service

set -a
source ../../.env
set +a

./mvnw spring-boot:run
```

The inventory service has no HTTP endpoint; follow its log output instead.

### Health Check

In another terminal:

```bash
curl http://localhost:8080/actuator/health
```

Expected response:

```json
{
  "status": "UP"
}
```

### Run the tests

Docker must be running — the integration tests start real PostgreSQL and Kafka
containers via Testcontainers.

From the repository root:

```bash
(cd services/order-service && ./mvnw test)        # 45 tests
(cd services/inventory-service && ./mvnw test)    # 22 tests
```

The same two commands run in CI (`.github/workflows/ci.yml`) on Java 21.

## Project Status

In Development
