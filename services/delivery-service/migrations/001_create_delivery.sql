CREATE TABLE IF NOT EXISTS delivery_couriers
(
    id         BIGSERIAL PRIMARY KEY,
    name       TEXT NOT NULL,
    active     BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS delivery_reservations
(
    id            BIGSERIAL PRIMARY KEY,
    order_id      BIGINT NOT NULL UNIQUE,
    courier_id    BIGINT NOT NULL,
    delivery_slot TIMESTAMP WITH TIME ZONE NOT NULL,
    status        VARCHAR(32) NOT NULL DEFAULT 'RESERVED',
    created_at    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,

                                CONSTRAINT delivery_reservations_courier_fk
                                FOREIGN KEY (courier_id)
    REFERENCES delivery_couriers (id),

    CONSTRAINT delivery_reservations_order_id_check
    CHECK (order_id > 0),

    CONSTRAINT delivery_reservations_status_check
    CHECK (
              status IN (
              'RESERVED',
              'RELEASED'
                        )
    )
    );

CREATE UNIQUE INDEX IF NOT EXISTS idx_delivery_active_slot
    ON delivery_reservations
    (
    courier_id,
    delivery_slot
    )
    WHERE status = 'RESERVED';

CREATE INDEX IF NOT EXISTS idx_delivery_reservations_order
    ON delivery_reservations (order_id);

CREATE INDEX IF NOT EXISTS idx_delivery_reservations_slot
    ON delivery_reservations (delivery_slot);

INSERT INTO delivery_couriers
(
    id,
    name,
    active
)
VALUES
    (
        1,
        'Учебный курьер',
        TRUE
    )
    ON CONFLICT (id) DO NOTHING;

SELECT setval(
               pg_get_serial_sequence(
                       'delivery_couriers',
                       'id'
               ),
               GREATEST(
                       (
                           SELECT COALESCE(
                                          MAX(id),
                                          1
                                  )
                           FROM delivery_couriers
                       ),
                       1
               ),
               true
       );