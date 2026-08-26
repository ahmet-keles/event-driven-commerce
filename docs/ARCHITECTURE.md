# Architecture

This document describes the system **as it exists in this repository today**.
Anything not yet built is listed separately under
[Implemented vs. planned](#implemented-vs-planned).

## Overview

The platform is a set of independently deployable Spring Boot services that
communicate asynchronously over Apache Kafka. There are currently **two**
services, each owning its own PostgreSQL database. Services never call each
other synchronously and never share a database; the only integration point is
Kafka.

```mermaid
flowchart LR
    client([HTTP client])

    subgraph order["order-service (:8080)"]
        orderapi["REST API<br/>/api/orders"]
        orderdb[("PostgreSQL<br/>commerce :5433<br/>orders, order_items,<br/>outbox_events")]
    end

    subgraph inventory["inventory-service (no HTTP API)"]
        invlogic["reservation logic"]
        invdb[("PostgreSQL<br/>inventory :5434<br/>inventory_items,<br/>processed_events,<br/>outbox_events")]
    end

    ordertopic{{"Kafka topic<br/>order.events"}}
    inventorytopic{{"Kafka topic<br/>inventory.events"}}

    client --> orderapi
    orderapi --- orderdb
    orderdb -- "outbox publisher" --> ordertopic
    ordertopic -- "OrderEventsConsumer" --> invlogic
    invlogic --- invdb
    invdb -- "outbox publisher" --> inventorytopic
    inventorytopic -- "InventoryEventsConsumer" --> orderapi
```

Both directions of the loop use the same mechanism: a business transaction
writes domain state **and** an outbox row in one database transaction, and a
scheduled publisher later ships unpublished outbox rows to Kafka. See
[EVENT_FLOW.md](EVENT_FLOW.md) for the message contracts and the outbox
mechanics.

## Runtime components

| Component | Image / module | Local endpoint | Notes |
|---|---|---|---|
| order-service | `services/order-service` | `http://localhost:8080` | Spring MVC REST API + Actuator (`health`, `info`) |
| inventory-service | `services/inventory-service` | none | No web or actuator dependency; runs as a Kafka consumer/publisher process |
| PostgreSQL (orders) | `postgres:17-alpine` | `localhost:${POSTGRES_PORT:-5433}` | Compose service `postgres`; database, user and password are required from `.env`; `pg_isready` healthcheck |
| PostgreSQL (inventory) | `postgres:17-alpine` | `localhost:${INVENTORY_POSTGRES_PORT:-5434}` | Compose service `inventory-postgres`; defaults to database `inventory` / user `inventory_user`; `pg_isready` healthcheck |
| Kafka | `apache/kafka:4.0.0` | `localhost:29092` | Compose service `kafka`, single-node KRaft (broker + controller), 3 default partitions, broker-API healthcheck |

All three infrastructure containers join the `commerce-net` bridge network, use
`restart: unless-stopped`, and declare healthchecks, so
`docker compose up -d --wait` returns only once each one is actually accepting
connections. See [DEVELOPMENT.md](DEVELOPMENT.md#2-start-the-infrastructure).

Both services are built on the Spring Boot `4.1.1` parent with
`<java.version>21</java.version>`, use Flyway for schema migrations, and run
with `spring.jpa.hibernate.ddl-auto=validate` — the JPA mappings are validated
against the Flyway-managed schema at startup, never generated from it.

## order-service responsibilities

Source: `services/order-service/src/main/java/com/ahmetkeles/orderservice`

1. **Own the order aggregate.** `Order` and `OrderItem` (`domain/`) enforce the
   invariants: a customer id and a non-blank currency are required, item
   quantity must be positive, unit price must be non-negative, `totalAmount` is
   recomputed from item subtotals, and a new order starts as `PENDING`.
2. **Expose the public REST API** (`api/OrderController`):

   | Method | Path | Behavior |
   |---|---|---|
   | `POST` | `/api/orders` | Creates a `PENDING` order from `{customerId, currency}`, returns `201` |
   | `GET` | `/api/orders/{orderId}` | Returns the order with its items |
   | `POST` | `/api/orders/{orderId}/items` | Adds `{productId, quantity, unitPrice}` and returns the updated order |

   `RestExceptionHandler` maps `OrderNotFoundException` to `404 not_found`,
   bean-validation and path-type failures to `400 validation_error`, and
   `IllegalArgumentException` to `400 invalid_argument`, all as
   `ApiError {status, error, message}`.
3. **Write outbox events in the same transaction as the state change.**
   `OrderService.createOrder` writes the order plus an `ORDER_CREATED` outbox
   row; `OrderService.addItem` writes the item plus an `ORDER_ITEM_ADDED` row.
4. **Publish the outbox to Kafka.** `OutboxPublisher` polls unpublished rows and
   sends them to `order.events`.
5. **React to inventory events.** `InventoryEventsConsumer` listens to
   `inventory.events` and handles two event types: `INVENTORY_RESERVED` calls
   `OrderService.confirmOrder` (`PENDING → CONFIRMED`), and
   `INVENTORY_RESERVATION_FAILED` calls `OrderService.cancelOrder`
   (`PENDING → CANCELLED`). Any other event type is ignored.
6. **Expose health.** `management.endpoints.web.exposure.include=health,info`.

### order-service schema

`V1__create_order_schema.sql` creates `orders` (with a
`CHECK (status IN ('PENDING','CONFIRMED','CANCELLED'))` constraint) and
`order_items`. `V2__create_outbox_events.sql` creates `outbox_events` with a
partial index on `occurred_at WHERE published_at IS NULL` (the publisher's
query path) and an index on `aggregate_id`.

## inventory-service responsibilities

Source: `services/inventory-service/src/main/java/com/ahmetkeles/inventoryservice`

1. **Own per-product stock.** `InventoryItem` holds `availableQuantity`,
   `reservedQuantity`, a JPA `@Version` column for optimistic locking, and
   `updatedAt`. `reserve(quantity)` rejects non-positive quantities and throws
   `InsufficientInventoryException` when `availableQuantity < quantity`;
   otherwise it moves stock from available to reserved.
2. **Consume order events.** `OrderEventsConsumer` listens to `order.events` and
   acts only on `ORDER_ITEM_ADDED`; every other event type is ignored.
   (`ORDER_CREATED` is published but currently has no consumer.)
3. **Reserve idempotently and transactionally.**
   `InventoryReservationService.reserve` runs in one transaction. It returns
   immediately if the envelope's `eventId` is already in `processed_events`;
   otherwise it takes one of two paths, both of which commit:

   - **Reserved** — the `InventoryItem` exists and holds enough stock: quantity
     moves from available to reserved, a `processed_events` row is written, and
     an `INVENTORY_RESERVED` outbox row is written.
   - **Failed** — the product has no inventory row
     (`InventoryItemNotFoundException`) or too little stock
     (`InsufficientInventoryException`): the exception is caught and logged, no
     stock changes, a `processed_events` row is still written, and an
     `INVENTORY_RESERVATION_FAILED` outbox row is written carrying a `reason` of
     `INVENTORY_ITEM_NOT_FOUND` or `INSUFFICIENT_INVENTORY`.

   A reservation failure is therefore a business outcome reported downstream,
   not an exception that escapes the consumer.
4. **Publish the outbox to Kafka.** `OutboxPublisher` sends unpublished rows to
   `inventory.events` and logs failures, leaving `published_at` null for retry.
5. **No HTTP surface.** The service has no web or actuator dependency, so there
   is no REST API and no `/actuator/health` endpoint. There is also no API or
   migration that seeds `inventory_items` — stock rows must be inserted
   directly (see [DEVELOPMENT.md](DEVELOPMENT.md#seeding-inventory)).

### inventory-service schema

`V1__create_inventory_schema.sql` creates `inventory_items` (primary key
`product_id`, non-negative check constraints on both quantity columns) and
`processed_events` (primary key `event_id`). `V2__create_inventory_outbox.sql`
creates the service's own `outbox_events` table with the same shape and indexes
as the order-service outbox.

## Order lifecycle

`OrderStatus` defines three values: `PENDING`, `CONFIRMED`, `CANCELLED`, and
all three are reachable today:

```
                      POST /api/orders
                             │
                             ▼
                        ┌─────────┐
                        │ PENDING │◀── POST /api/orders/{id}/items
                        └─────────┘        (status unchanged)
                      ┌──────┴──────┐
    INVENTORY_RESERVED│             │INVENTORY_RESERVATION_FAILED
                      ▼             ▼
                ┌───────────┐ ┌───────────┐
                │ CONFIRMED │ │ CANCELLED │
                └───────────┘ └───────────┘

  Both are terminal: confirm() and cancel() are no-ops unless the order
  is still PENDING, so a confirmed order is never cancelled and a
  cancelled order is never confirmed.
```

Behavior worth knowing, as currently coded:

- `Order.confirm()` and `Order.cancel()` both return early unless the order is
  `PENDING`. Repeated or interleaved `INVENTORY_RESERVED` /
  `INVENTORY_RESERVATION_FAILED` events for the same order are therefore
  harmless — whichever arrives first wins, and later ones change nothing.
- The transition happens on the **first** inventory event for an order. There is
  no check that every item on the order has been reserved, so an order with
  several items is confirmed by the first successful reservation and cancelled
  by the first failed one.
- `addItem` has no status guard: items can still be added to a `CONFIRMED` or
  `CANCELLED` order, and doing so emits another `ORDER_ITEM_ADDED` event.
- Cancelling an order does **not** release stock reserved for its other items;
  there is no compensating release path yet.

## Inventory reservation flow

1. A client adds an item: `POST /api/orders/{orderId}/items`.
2. In one transaction, order-service persists the `OrderItem`, updates
   `totalAmount`, and inserts an `ORDER_ITEM_ADDED` row into its
   `outbox_events`.
3. Within roughly one polling interval, order-service's `OutboxPublisher` sends
   that row to `order.events`, keyed by the order id, and stamps `published_at`.
4. inventory-service's `OrderEventsConsumer` (group `inventory-service`)
   receives the envelope and deserializes the payload.
5. `InventoryReservationService.reserve` opens a transaction:
   - `processed_events` already contains `eventId` → return, nothing changes.
   - Product row missing → caught as `INVENTORY_ITEM_NOT_FOUND`:
     `processed_events` row + `INVENTORY_RESERVATION_FAILED` outbox row,
     committed.
   - `availableQuantity < quantity` → caught as `INSUFFICIENT_INVENTORY`:
     `processed_events` row + `INVENTORY_RESERVATION_FAILED` outbox row,
     committed, stock untouched.
   - Otherwise: available decreases, reserved increases, `processed_events` row
     written, `INVENTORY_RESERVED` outbox row written — all committed together.

   `InventoryReservationIntegrationTest` and `InventoryReservationServiceTest`
   cover all four branches.
6. inventory-service's `OutboxPublisher` sends the resulting row to
   `inventory.events`, keyed by the order id.
7. order-service's `InventoryEventsConsumer` (group `order-service`) receives it
   and either confirms the order (`INVENTORY_RESERVED`) or cancels it
   (`INVENTORY_RESERVATION_FAILED`).

An exception thrown out of either consumer propagates to Spring Kafka; no custom
error handler, retry topic, or dead-letter topic is configured, so the framework
defaults apply. Successfully reserved stock is never released and never turns
into a completed shipment — there is no downstream step yet.

## Cross-cutting patterns in place

- **Transactional outbox** in both services — see
  [EVENT_FLOW.md](EVENT_FLOW.md#transactional-outbox-pattern).
- **Consumer idempotency** in inventory-service via `processed_events`, keyed by
  the envelope `eventId`.
- **Optimistic locking** on `inventory_items` via `@Version`.
- **Flyway migrations** with `ddl-auto=validate` in both services.
- **Reservation-failure events** that drive order cancellation, closing the
  success and failure sides of the loop.
- **Testcontainers integration tests** covering persistence, the REST API, the
  outbox, and both Kafka hops — 45 tests in order-service, 22 in
  inventory-service.
- **GitHub Actions CI** running both suites on Java 21 with Testcontainers for
  every push to `main` and every pull request — see
  [DEVELOPMENT.md](DEVELOPMENT.md#continuous-integration).

## Implemented vs. planned

### Implemented

| Capability | Where |
|---|---|
| Order REST API (create, read, add item) | `order-service` `api/` |
| Order domain invariants and totals | `order-service` `domain/` |
| Order persistence with Flyway migrations | `order-service` `resources/db/migration` |
| Transactional outbox + scheduled Kafka publisher | both services, `outbox/` |
| `order.events` and `inventory.events` topics | `KafkaTopicConfig` in both services |
| Inventory reservation with optimistic locking | `inventory-service` `inventory/` |
| Idempotent order-event consumer | `inventory-service` `processed_events` |
| Reservation-failure events (`INVENTORY_RESERVATION_FAILED`) | `inventory-service` `InventoryReservationService` |
| Order confirmation driven by `INVENTORY_RESERVED` | `order-service` `messaging/` |
| Order cancellation driven by `INVENTORY_RESERVATION_FAILED` | `order-service` `messaging/`, `Order.cancel()` |
| Local Kafka + two PostgreSQL instances with healthchecks | `compose.yaml` |
| Unit and Testcontainers integration test suites (45 + 22 tests) | `src/test` in both services |
| GitHub Actions CI for both services on Java 21 | `.github/workflows/ci.yml` |
| Actuator health/info (order-service only) | `order-service` `application.properties` |

### Planned / not implemented

These appear in the project's goals but have **no code in this repository**:

- **payment-service** — planned only; nothing is implemented.
- **notification-service** — planned only.
- **Multi-item reservation semantics** — an order transitions on its first
  inventory event, not once every item is accounted for.
- **Releasing reserved stock** — nothing decrements `reserved_quantity`, so a
  cancelled order's already-reserved items stay reserved. No compensating or
  saga-style rollback exists.
- Retry policy, dead-letter topics, and consumer error handlers.
- Idempotency/dedupe on the order-service side of the loop.
- Redis or any distributed cache.
- Continuous **deployment** and cloud infrastructure (CI itself is implemented).
- Load testing.
- Metrics/tracing beyond the actuator health and info endpoints.
- An API or migration for seeding and managing `inventory_items`.
