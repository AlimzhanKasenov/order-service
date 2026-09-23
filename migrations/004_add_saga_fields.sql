ALTER TABLE orders
    ADD COLUMN IF NOT EXISTS product_id BIGINT,
    ADD COLUMN IF NOT EXISTS quantity BIGINT,
    ADD COLUMN IF NOT EXISTS delivery_slot TIMESTAMP WITH TIME ZONE,
    ADD COLUMN IF NOT EXISTS saga_error TEXT;

ALTER TABLE orders
DROP CONSTRAINT IF EXISTS orders_status_check;

ALTER TABLE orders
    ADD CONSTRAINT orders_status_check
        CHECK (
            status IN (
                       'NEW',
                       'PAID',
                       'PAYMENT_FAILED',
                       'SAGA_STARTED',
                       'CONFIRMED',
                       'SAGA_FAILED',
                       'COMPENSATION_FAILED'
                )
            );

ALTER TABLE orders
DROP CONSTRAINT IF EXISTS orders_product_id_check;

ALTER TABLE orders
    ADD CONSTRAINT orders_product_id_check
        CHECK (
            product_id IS NULL
                OR product_id > 0
            );

ALTER TABLE orders
DROP CONSTRAINT IF EXISTS orders_quantity_check;

ALTER TABLE orders
    ADD CONSTRAINT orders_quantity_check
        CHECK (
            quantity IS NULL
                OR quantity > 0
            );