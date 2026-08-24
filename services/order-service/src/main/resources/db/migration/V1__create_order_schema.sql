CREATE TABLE orders (
    id UUID NOT NULL,
    created_at TIMESTAMP(6) WITH TIME ZONE,
    currency VARCHAR(255),
    customer_id UUID,
    status VARCHAR(255),
    total_amount NUMERIC(38,2),
    updated_at TIMESTAMP(6) WITH TIME ZONE,

    CONSTRAINT orders_pkey PRIMARY KEY (id),
    CONSTRAINT orders_status_check
        CHECK (status IN ('PENDING', 'CONFIRMED', 'CANCELLED'))
);

CREATE TABLE order_items (
    id UUID NOT NULL,
    product_id UUID,
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(38,2),
    order_id UUID NOT NULL,

    CONSTRAINT order_items_pkey PRIMARY KEY (id),
    CONSTRAINT fk_order_items_order
        FOREIGN KEY (order_id) REFERENCES orders(id)
);

CREATE INDEX idx_order_items_order_id
    ON order_items(order_id);
