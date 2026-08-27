-- Order-side payment saga state (renumbered to V7: V6 carries the explicit
-- order submission state this saga's trigger depends on). NOT_STARTED until the order is confirmed;
-- confirmation moves it to PENDING in the same transaction that emits
-- ORDER_CONFIRMED, and the payment outcome events drive it to COMPLETED or
-- FAILED. The first terminal outcome wins and is never overwritten.
ALTER TABLE orders
    ADD COLUMN payment_status VARCHAR(20) NOT NULL DEFAULT 'NOT_STARTED';

ALTER TABLE orders
    ADD CONSTRAINT orders_payment_status_check
        CHECK (payment_status IN
            ('NOT_STARTED', 'PENDING', 'COMPLETED', 'FAILED'));
