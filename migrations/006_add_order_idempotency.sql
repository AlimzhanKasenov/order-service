-- Автор: Kassenov Alimzhan
-- Идемпотентность создания заказа через Idempotency-Key.

CREATE TABLE IF NOT EXISTS order_idempotency_keys
(
    idempotency_key VARCHAR(128) PRIMARY KEY,
    request_hash    CHAR(64)     NOT NULL,
    order_id        BIGINT       NOT NULL UNIQUE,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_order_idempotency_order
        FOREIGN KEY (order_id)
        REFERENCES orders(id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_order_idempotency_order_id
    ON order_idempotency_keys (order_id);
