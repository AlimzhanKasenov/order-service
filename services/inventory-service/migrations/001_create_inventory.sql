CREATE TABLE IF NOT EXISTS inventory_products
(
    id                 BIGSERIAL PRIMARY KEY,
    name               TEXT NOT NULL,
    available_quantity BIGINT NOT NULL DEFAULT 0,
    created_at         TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT inventory_products_available_quantity_check
    CHECK (available_quantity >= 0)
    );

CREATE TABLE IF NOT EXISTS inventory_reservations
(
    id         BIGSERIAL PRIMARY KEY,
    order_id   BIGINT NOT NULL UNIQUE,
    product_id BIGINT NOT NULL,
    quantity   BIGINT NOT NULL,
    status     VARCHAR(32) NOT NULL DEFAULT 'RESERVED',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,

                             CONSTRAINT inventory_reservations_product_fk
                             FOREIGN KEY (product_id)
    REFERENCES inventory_products (id),

    CONSTRAINT inventory_reservations_order_id_check
    CHECK (order_id > 0),

    CONSTRAINT inventory_reservations_quantity_check
    CHECK (quantity > 0),

    CONSTRAINT inventory_reservations_status_check
    CHECK (
              status IN (
              'RESERVED',
              'RELEASED'
                        )
    )
    );

CREATE INDEX IF NOT EXISTS idx_inventory_reservations_product_id
    ON inventory_reservations (product_id);

CREATE INDEX IF NOT EXISTS idx_inventory_reservations_status
    ON inventory_reservations (status);

INSERT INTO inventory_products
(
    id,
    name,
    available_quantity
)
VALUES
    (
        1,
        'Учебный товар',
        10
    ),
    (
        2,
        'Товар без остатка',
        0
    )
    ON CONFLICT (id) DO NOTHING;

SELECT setval(
               pg_get_serial_sequence(
                       'inventory_products',
                       'id'
               ),
               GREATEST(
                       (
                           SELECT COALESCE(
                                          MAX(id),
                                          1
                                  )
                           FROM inventory_products
                       ),
                       1
               ),
               true
       );