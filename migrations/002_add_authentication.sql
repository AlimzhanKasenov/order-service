ALTER TABLE users
    ADD COLUMN IF NOT EXISTS password_hash VARCHAR(100);

COMMENT ON COLUMN users.password_hash IS
    'BCrypt password hash. Nullable only for users created before authentication was introduced.';
