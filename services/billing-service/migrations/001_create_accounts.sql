CREATE TABLE IF NOT EXISTS accounts
(
    id         BIGSERIAL PRIMARY KEY,
    user_id    BIGINT                   NOT NULL UNIQUE,
    balance    BIGINT                   NOT NULL DEFAULT 0 CHECK (balance >= 0),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_accounts_user_id
    ON accounts(user_id);
