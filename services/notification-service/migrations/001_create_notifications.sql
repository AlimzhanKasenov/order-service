CREATE TABLE IF NOT EXISTS notifications
(
    id         BIGSERIAL PRIMARY KEY,
    user_id    BIGINT                   NOT NULL,
    order_id   BIGINT                   NOT NULL,
    email      VARCHAR(255)             NOT NULL,
    subject    VARCHAR(255)             NOT NULL,
    message    TEXT                     NOT NULL,
    status     VARCHAR(20)              NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT notifications_status_check
        CHECK (status IN ('SUCCESS', 'FAILED'))
);

CREATE INDEX IF NOT EXISTS idx_notifications_user_id
    ON notifications(user_id);

CREATE INDEX IF NOT EXISTS idx_notifications_order_id
    ON notifications(order_id);
