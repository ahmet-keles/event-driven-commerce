# Event Flow

How the three services talk to each other: the topics, the message shape,
every event contract, the outbox mechanics, and the failure behavior that
exists in code. For the bigger picture see [ARCHITECTURE.md](ARCHITECTURE.md).

## Kafka topics

| Topic | Produced by | Consumed by | Partitions / replicas | Message key |
|---|---|---|---|---|
| `order.events` | order-service | inventory-service (group `inventory-service`), payment-service (group `payment-service`) | 3 / 1 | order id |
| `inventory.events` | inventory-service | order-service (group `order-service`) | 3 / 1 | order id |
| `payment.events` | payment-service | order-service (group `order-service`) | 3 / 1 | order id |
| `order.events.DLT`, `inventory.events.DLT`, `payment.events.DLT` | the consumer that exhausted retries on the source topic | operators (manual redrive) | producer-chosen partition | original record's key |

Topic names come from configuration (`app.kafka.*-events-topic`), not from
constants in code. Each service declares its **outbound** topic as a
`NewTopic` bean (`KafkaTopicConfig`), created by Spring Kafka's admin client
at startup; dead-letter topics are named `<source topic>.DLT` by the
consumer's `DeadLetterPublishingRecoverer`.

Keys and values are plain strings (`StringSerializer` / `StringDeserializer`);
the JSON structure below lives inside the value. Because the key is the order
id, all events for one order land on the same partition and keep their
relative order — the property the whole saga's determinism rests on.

All three consumers set `auto-offset-reset=earliest`: a fresh consumer group
replays its topic from the beginning instead of silently skipping events
published before its first partition assignment.

## Message envelope

Every message on every topic is the same envelope — the serialized form of
`PublishedOutboxEvent`, built from an outbox row:

```json
{
  "eventId": "0f0d1a2b-3c4d-5e6f-7081-92a3b4c5d6e7",
  "aggregateType": "Order",
  "aggregateId": "9b1c2d3e-4f50-6172-8394-a5b6c7d8e9f0",
  "eventType": "ORDER_ITEM_ADDED",
  "payload": "{\"orderId\":\"9b1c…\",\"orderItemId\":\"…\",\"productId\":\"…\",\"quantity\":2,\"unitPrice\":9.99,\"totalAmount\":19.98}",
  "occurredAt": "2026-01-01T12:00:00Z"
}
```

- `payload` is a **JSON string**, not a nested object: the producer serialized
  the domain event before storing it in the outbox row, and the consumer
  deserializes it in a second step.
- `aggregateType` is `"Order"` on **every** topic — each service writes its
  outbox row against the order aggregate (`aggregateId` = order id).
- `eventId` is the outbox row's primary key and the consumers' deduplication
  key.
- Consumers validate the envelope before claiming it: a missing `eventId`, a
  missing payload order id, or an envelope/payload order-id mismatch is a
  contract violation (`InvalidEventException`) that dead-letters without
  retries.

## Event contracts

### `order.events`

**`ORDER_CREATED`** — written with the order in `createOrder`. No consumer
acts on it today; both consumers filter it out.

```json
{ "orderId": "…", "customerId": "…", "currency": "USD", "status": "PENDING" }
```

**`ORDER_ITEM_ADDED`** — written with each item in `addItem`; drives the
reservation flow. inventory-service reserves `quantity` of `productId` for
exactly this `orderItemId`.

```json
{ "orderId": "…", "orderItemId": "…", "productId": "…", "quantity": 2, "unitPrice": 9.99, "totalAmount": 19.98 }
```

**`ORDER_CONFIRMED`** — written in the same transaction as the
`PENDING → CONFIRMED` transition, which requires the client to have
**explicitly submitted** the order AND every item to be reserved (whichever
half completes last performs the transition, and also moves the order's
`payment_status` to `PENDING`). payment-service charges on it.

```json
{ "orderId": "…", "customerId": "…", "totalAmount": 19.98, "currency": "USD" }
```

**`ORDER_CANCELLED`** — written in the same transaction as every
cancellation: a reservation failure cancelling a `PENDING` order, or a
payment failure cancelling a `CONFIRMED` one. inventory-service releases the
order's held stock on it.

```json
{ "orderId": "…" }
```

### `inventory.events`

**`INVENTORY_RESERVED`** — one per successfully reserved order item.
order-service marks that item reserved; the order confirms once it is
submitted and no unreserved items remain.

```json
{ "orderId": "…", "orderItemId": "…", "productId": "…", "quantity": 2 }
```

**`INVENTORY_RESERVATION_FAILED`** — the reservation could not be satisfied;
stock is untouched. order-service cancels the order. Note the field name:
`requestedQuantity`, not `quantity`.

```json
{ "orderId": "…", "orderItemId": "…", "productId": "…", "requestedQuantity": 5, "reason": "INSUFFICIENT_INVENTORY" }
```

| `reason` | Cause |
|---|---|
| `INSUFFICIENT_INVENTORY` | The product row exists but `availableQuantity < requestedQuantity` |
| `INVENTORY_ITEM_NOT_FOUND` | No `inventory_items` row exists for the product |

### `payment.events`

**`PAYMENT_COMPLETED`** — the simulated gateway approved the charge
(`totalAmount` strictly below the `1000.00` decline threshold). order-service
moves `payment_status` to `COMPLETED`; the order stays `CONFIRMED`.

```json
{ "orderId": "…", "paymentId": "…", "amount": 19.98, "currency": "USD" }
```

**`PAYMENT_FAILED`** — the gateway declined (`totalAmount` at or above the
threshold). order-service moves `payment_status` to `FAILED`, cancels the
confirmed order through a dedicated payment-failure transition, and emits
`ORDER_CANCELLED` in the same transaction — triggering the inventory-release
compensation. The first terminal payment outcome wins and is never
overwritten.

```json
{ "orderId": "…", "paymentId": "…", "amount": 1200.00, "currency": "USD", "reason": "simulated decline: amount at or above threshold" }
```

## End-to-end sequence

```mermaid
sequenceDiagram
    participant C as Client
    participant O as order-service
    participant K as Kafka
    participant I as inventory-service
    participant P as payment-service

    C->>O: POST /api/orders/{id}/items  (per item)
    Note over O: item + ORDER_ITEM_ADDED outbox row,<br/>one transaction each
    C->>O: POST /api/orders/{id}/submit
    Note over O: submitted = true (assembly finished)

    O->>K: ORDER_ITEM_ADDED (outbox publisher, key = orderId)
    K->>I: ORDER_ITEM_ADDED
    Note over I: dedup on eventId, then reserve<br/>or record the failure — one transaction

    alt stock reserved
        I->>K: INVENTORY_RESERVED (per item)
        K->>O: INVENTORY_RESERVED
        Note over O: mark item reserved;<br/>submitted AND all reserved ⇒ CONFIRMED,<br/>payment_status → PENDING,<br/>ORDER_CONFIRMED emitted — one transaction
        O->>K: ORDER_CONFIRMED
        K->>P: ORDER_CONFIRMED
        Note over P: dedup, charge via simulated gateway,<br/>payment row + outcome outbox row — one transaction
        alt amount < 1000.00
            P->>K: PAYMENT_COMPLETED
            K->>O: PAYMENT_COMPLETED
            Note over O: payment_status → COMPLETED
        else amount ≥ 1000.00
            P->>K: PAYMENT_FAILED
            K->>O: PAYMENT_FAILED
            Note over O: payment_status → FAILED,<br/>order CANCELLED,<br/>ORDER_CANCELLED emitted
            O->>K: ORDER_CANCELLED
            K->>I: ORDER_CANCELLED
            Note over I: release every still-RESERVED<br/>ledger row for the order
        end
    else reservation failed
        I->>K: INVENTORY_RESERVATION_FAILED
        K->>O: INVENTORY_RESERVATION_FAILED
        Note over O: order CANCELLED, ORDER_CANCELLED emitted
        O->>K: ORDER_CANCELLED
        K->>I: ORDER_CANCELLED
        Note over I: release earlier reservations
    end
```

If reservations finish **before** the client submits, the order stays
`PENDING` until the submit, which then confirms it in the submitting
transaction itself — an order still being assembled can never confirm.

## Transactional outbox pattern

All three services use the same implementation in their own `outbox` package.

**Write side.** The business transaction persists domain state and an
`OutboxEvent` row through the same JPA transaction. If either half fails, both
roll back — nothing is ever sent to Kafka from inside the business
transaction, so a committed state change can never be missing its event and an
event can never describe an uncommitted change. This composes with optimistic
locking: a transaction that loses a version race rolls its staged outbox row
back with the stale mutation.

The `outbox_events` table (same shape in all three services):

| Column | Notes |
|---|---|
| `id` | UUID primary key; travels as the envelope's `eventId` |
| `aggregate_type` | `"Order"` everywhere |
| `aggregate_id` | order id; also the Kafka message key |
| `event_type` | one of the eight types above |
| `payload` | `TEXT`, the JSON-serialized domain event |
| `occurred_at` | set at construction |
| `published_at` | `NULL` until the row reaches Kafka |

Partial indexes on the unpublished predicate keep the publisher's claim and
its ordering guard cheap as the table grows.

**Publish side.** `OutboxPublisher` runs on a fixed delay
(`app.outbox.publish-interval-ms`, 1 s everywhere) and is **safe to run in
multiple replicas**:

1. A short transaction claims up to `app.outbox.batch-size` (25) unpublished
   rows, oldest first, with `FOR UPDATE SKIP LOCKED` — concurrent replicas
   partition the backlog instead of double-publishing.
2. A cross-replica **per-order ordering guard** ensures a replica never
   publishes an order's later event while an earlier unpublished event for the
   same order is claimed elsewhere.
3. Each row is sent with a bounded wait (`send-timeout-ms` 2 s, producer
   `max.block.ms` 2 s) under a poll deadline (`poll-deadline-ms` 4 s), so the
   worst-case lock/connection hold is ~8 s regardless of batch size.
4. Success stamps `published_at`; failure leaves it `NULL` for the next cycle
   and is logged.

Consequences, by design:

- **At-least-once delivery.** A crash or send-timeout between a broker-accepted
  send and the `published_at` stamp re-sends the row later. Every consumer
  deduplicates on `eventId`, so duplicates are absorbed.
- **Per-order ordering is preserved** end to end: same key → same partition,
  and the publisher never reorders one order's events across replicas.
- **Published rows are pruned** by the retention job (below), never the
  unpublished backlog.

## Idempotency

Every consumer in the system records each processed envelope's `eventId` in
its own `processed_events` table **inside the same transaction as the
mutation**, so a mutation that rolls back — an optimistic-lock loss, a flush
error — releases its ledger entry for the redelivery to take. order-service
claims with `INSERT … ON CONFLICT DO NOTHING`; inventory-service and
payment-service check-then-insert, with the ledger's primary key as the
backstop against a racing duplicate. A duplicate delivery therefore applies
no effect and emits no second event, on the failure paths as well as the
success paths.

Beyond the ledgers, the domain itself is defensive:

- order-service: per-item `reserved` flags mean a redelivered
  `INVENTORY_RESERVED` for an already-reserved item cannot advance the order;
  terminal order states and terminal payment outcomes latch.
- inventory-service: the reservation ledger is keyed by order item id, and a
  cancelled order's state row blocks late reservations entirely.
- payment-service: `payments.order_id` is unique and the gateway idempotency
  key is the order id — even a confirmation re-emitted under a *fresh*
  `eventId` cannot charge twice.

## Failure behavior

All three services configure the same consumer failure policy
(`KafkaErrorHandlingConfig`, tuned by `app.kafka.retry.*`): a
`DefaultErrorHandler` with **exponential backoff — 4 attempts, 500 ms initial,
×2 up to 5 s** — and a `DeadLetterPublishingRecoverer` that publishes the
exhausted record, unchanged and under its original key, to
`<source topic>.DLT`.

Failures are classified:

| Class | Examples | Handling |
|---|---|---|
| **Business outcome** | insufficient stock, unknown product, declined charge | Never an error: the transaction commits and emits the failure **event** (`INVENTORY_RESERVATION_FAILED`, `PAYMENT_FAILED`) |
| **Contract violation** | malformed JSON, missing `eventId`/order id, envelope–payload mismatch, integrity violations | `InvalidEventException` → **no retries**, straight to the DLT; the partition keeps moving |
| **Transient** | DB connection loss, lock timeouts, deadlocks, optimistic-lock conflicts | Retried with backoff; the rolled-back claim lets the retry re-apply cleanly |
| **Duplicate** | redelivered `eventId` | Consumed and dropped by the ledger; never an error |
| **Stale transition** | event for an order already terminal | No-op by domain guards; consumed successfully |

A record on a DLT carries the original value verbatim plus the standard
`kafka_dlt-*` headers naming its source; redrive is a manual, operational
action. The end-to-end suite (`DeadLetterE2eTest`) pins this contract on both
directions of the loop.

## Retention

All three services bound their bookkeeping growth
(order/payment: `RetentionJob`; inventory: `RetentionCleanupJob`; configured
under `app.retention.*`):

- **Published outbox rows** older than the outbox max age (default 7 d,
  measured from `published_at`) are deleted. **Unpublished rows are never
  deleted at any age** — the delete predicate requires a non-null
  `published_at`, so no configuration can reach an event the broker has not
  acknowledged.
- **`processed_events` rows** older than the ledger max age (default 30 d)
  are deleted.

Every `DELETE` is bounded (`batch-size` 500, at most `max-batches-per-run` 10
batches per table per run, each batch its own short transaction), and batches
claim victims with `FOR UPDATE SKIP LOCKED`, so concurrent replicas partition
the work and never touch the publisher's claims. The bounds are
bean-validated at startup: a zero or negative age or batch bound refuses to
boot.

**The ledger max age bounds the deduplication window.** Deleting a
`processed_events` row re-enables its `eventId`; the age MUST exceed the
Kafka source topic's retention plus the worst-case consumer lag and any
operational replay/DLT-redrive window, and must be raised in step with them.
Past the window the secondary guards (per-item reservation keys, the unique
`payments.order_id`, terminal-state latching) still prevent double stock
mutation and double charging, but the general guarantee is gone. The outbox,
by contrast, is not the replay log — Kafka is; deleting published rows costs
operational forensics only.
