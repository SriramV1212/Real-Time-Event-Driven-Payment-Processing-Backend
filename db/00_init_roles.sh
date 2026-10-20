#!/usr/bin/env bash
set -e

: "${PAYMENT_APP_DB_PASSWORD:?Payment app database password is required}"

psql \
    -v ON_ERROR_STOP=1 \
    -v app_password="$PAYMENT_APP_DB_PASSWORD" \
    --username "$POSTGRES_USER" \
    --dbname "$POSTGRES_DB" <<'SQL'

SELECT 'CREATE ROLE payment_app LOGIN'
WHERE NOT EXISTS (
    SELECT 1 FROM pg_roles WHERE rolname = 'payment_app'
)
\gexec

ALTER ROLE payment_app WITH PASSWORD :'app_password';

SQL