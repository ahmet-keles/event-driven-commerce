# Order Domain Model

Reference for the order aggregate as modelled in `order-service`.
For how these statuses are actually driven at runtime, see
[ARCHITECTURE.md](ARCHITECTURE.md#order-lifecycle).

## Order

Represents a customer's purchase request.

### Fields

- `id` — UUID that uniquely identifies the order
- `customerId` — UUID identifying the customer
- `status` — current state of the order
- `totalAmount` — total monetary value of the order
- `currency` — currency code such as USD
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

## Order Status

Supported states (`OrderStatus`, also enforced by a `CHECK` constraint on the
`orders` table):

- `PENDING`
- `CONFIRMED`
- `CANCELLED`

Implemented transitions: an order is created `PENDING` and becomes `CONFIRMED`
when an `INVENTORY_RESERVED` event is consumed. `CANCELLED` is defined but no
code path currently sets it.

## Initial Business Rules

1. An order must contain at least one item.
2. Item quantity must be greater than zero.
3. Unit price cannot be negative.
4. Every order must have a customer.
5. Every order must specify a currency.
6. A new order begins with status `PENDING`.
7. `totalAmount` is calculated from the order items.

Rules 2–7 are enforced in code today. Rule 1 is not: `POST /api/orders`
creates an empty `PENDING` order and items are added afterwards, so an order
with no items is currently representable.
