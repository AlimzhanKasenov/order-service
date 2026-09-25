-- Автор: Kassenov Alimzhan
-- Transactional Outbox + Inbox для Billing Service.

CREATE TABLE IF NOT EXISTS outbox_events
(
    id           BIGSERIAL PRIMARY KEY,
    event_id     VARCHAR(64)  NOT NULL UNIQUE,
    topic        VARCHAR(255) NOT NULL,
    event_key    VARCHAR(255) NOT NULL,
    payload      JSONB        NOT NULL,
    status       VARCHAR(20)  NOT NULL DEFAULT 'PENDING',
    attempts     INTEGER      NOT NULL DEFAULT 0,
    last_error   TEXT,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP,
    published_at TIMESTAMPTZ,

    CONSTRAINT outbox_events_status_check
        CHECK (status IN ('PENDING', 'PUBLISHED')),

    CONSTRAINT outbox_events_attempts_check
        CHECK (attempts >= 0)
);

CREATE INDEX IF NOT EXISTS idx_outbox_events_pending
    ON outbox_events (id)
    WHERE status = 'PENDING';

CREATE TABLE IF NOT EXISTS inbox_events
(
    event_id     VARCHAR(64)  PRIMARY KEY,
    topic        VARCHAR(255) NOT NULL,
    processed_at TIMESTAMPTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP
);
