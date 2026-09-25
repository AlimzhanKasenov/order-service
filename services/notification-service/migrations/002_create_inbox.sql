-- Автор: Kassenov Alimzhan
-- Inbox Pattern для Notification Service.

CREATE TABLE IF NOT EXISTS inbox_events
(
    event_id     VARCHAR(64)  PRIMARY KEY,
    topic        VARCHAR(255) NOT NULL,
    processed_at TIMESTAMPTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP
);
