CREATE EXTENSION pg_stat_statements;

-- 创建event_logs表
CREATE TABLE IF NOT EXISTS event_logs (
    id BIGSERIAL PRIMARY KEY,
    address VARCHAR(42) NOT NULL,
    topics TEXT[] NOT NULL,
    data BYTEA NOT NULL,
    block_number BIGINT NOT NULL,
    tx_hash VARCHAR(66) NOT NULL,
    tx_index INTEGER NOT NULL,
    block_hash VARCHAR(66) NOT NULL,
    log_index INTEGER NOT NULL,
    removed BOOLEAN NOT NULL DEFAULT FALSE,
    status SMALLINT NOT NULL DEFAULT 1,
    type SMALLINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_event_logs_tx_hash ON event_logs(tx_hash);
CREATE INDEX IF NOT EXISTS idx_event_logs_block_number ON event_logs(block_number);
CREATE INDEX IF NOT EXISTS idx_event_logs_address ON event_logs(address);

CREATE INDEX CONCURRENTLY idx_event_logs_status_block ON event_logs(status, block_number, tx_index, log_index);