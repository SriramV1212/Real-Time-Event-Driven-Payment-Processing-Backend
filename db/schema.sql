CREATE TABLE IF NOT EXISTS users (
    user_id TEXT PRIMARY KEY,
    balance INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS payments (
    payment_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    amount INTEGER NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS processed_events (
    event_id TEXT PRIMARY KEY
);

ALTER TABLE processed_events
ADD CONSTRAINT unique_event_id UNIQUE (event_id);


-- Metrics table to track the total number of processed events for monitoring purposes 

CREATE TABLE IF NOT EXISTS metrics (
    id INT PRIMARY KEY,
    total_processed INT
);

INSERT INTO metrics (id, total_processed)
VALUES (1, 0)
ON CONFLICT (id) DO NOTHING;

UPDATE metrics SET total_processed = 0 WHERE id = 1;

CREATE TABLE IF NOT EXISTS outbox (
    id BIGSERIAL PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE,
    aggregate_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    published_at TIMESTAMP NULL
);
