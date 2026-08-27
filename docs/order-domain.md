# Order Domain Model

Reference for the order aggregate as modelled in `order-service`.
For how these statuses are actually driven at runtime, see
[ARCHITECTURE.md](ARCHITECTURE.md#order-lifecycle).

## Order

Represents a customer's purchase request.

### Fields

- `id` — UUID that uniquely identifies the order
- `customerId` — UUID identifying the customer
- `status` — current state of the order (`PENDING`, `CONFIRMED`, `CANCELLED`)
- `submitted` — whether the client has finished assembling the order; items
  can only be added while the order is `PENDING` and unsubmitted, and the
  order can only confirm once submitted
- `paymentStatus` — the payment leg (`NOT_STARTED`, `PENDING`, `COMPLETED`,
  `FAILED`); opened when the order confirms, settled by payment events
- `totalAmount` — total monetary value of the order
- `currency` — currency code such as USD
- `version` — optimistic-locking version; concurrent writes to the aggregate
  serialize into exactly one committed ordering
- `createdAt` — timestamp when the order was created
- `updatedAt` — timestamp when the order was last modified
- `items` — products included in the order

## Order Item

Represents one product inside an order.

### Fields

- `id` — UUID identifying the order item
- `productId` — identifier of the purchased product
- `quantity` — number of units ordered
- `unitPrice` — price of one unit
- `reserved` — whether inventory has reserved this item; confirmation requires
  every item to be reserved

## Order Status

Supported states (`OrderStatus`, also enforced by a `CHECK` constraint on the
`orders` table):

- `PENDING`
- `CONFIRMED`
- `CANCELLED`

Implemented transitions: an order is created `PENDING` and assembled item by
item; the client then explicitly submits it. It becomes `CONFIRMED` only once
it is **submitted and every item is reserved**, and `CANCELLED` when a
reservation fails or when payment on a confirmed order is declined. Terminal
states latch — a late reservation cannot confirm a cancelled order, and a late
failure cannot cancel a confirmed one (the sole exception being the dedicated
payment-failure cancellation).

## Initial Business Rules

1. An order must contain at least one item.
2. Item quantity must be greater than zero.
3. Unit price cannot be negative.
4. Every order must have a customer.
5. Every order must specify a currency.
6. A new order begins with status `PENDING`.
7. `totalAmount` is calculated from the order items.

All seven rules are enforced. `POST /api/orders` creates an empty `PENDING`
order and items are added afterwards, but rule 1 is enforced at submission:
an order without items cannot be submitted (`409 order_empty`), and an
unsubmitted order can never confirm.
