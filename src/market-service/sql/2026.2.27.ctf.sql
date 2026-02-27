-- CTF Schema Migration
-- Add CTF fields to t_market and t_option, create t_prediction_event

-- 1. Add CTF fields to t_market
ALTER TABLE t_market ADD COLUMN IF NOT EXISTS event_id VARCHAR(66) NOT NULL DEFAULT '';
ALTER TABLE t_market ADD COLUMN IF NOT EXISTS condition_id VARCHAR(66) NOT NULL DEFAULT '';
ALTER TABLE t_market ADD COLUMN IF NOT EXISTS question_id VARCHAR(66) NOT NULL DEFAULT '';
ALTER TABLE t_market ADD COLUMN IF NOT EXISTS outcome_slot_count INT NOT NULL DEFAULT 2;

CREATE INDEX IF NOT EXISTS idx_market_event_id ON t_market(event_id);
CREATE INDEX IF NOT EXISTS idx_market_condition_id ON t_market(condition_id);

-- 2. Add position_id to t_option (ERC1155 token ID)
ALTER TABLE t_option ADD COLUMN IF NOT EXISTS position_id VARCHAR(78) NOT NULL DEFAULT '';

-- 3. Create prediction_events table
CREATE TABLE IF NOT EXISTS t_prediction_event (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    event_id VARCHAR(66) NOT NULL DEFAULT '',
    title VARCHAR(512) NOT NULL DEFAULT '',
    outcome_slot_count INT NOT NULL DEFAULT 2,
    collateral VARCHAR(42) NOT NULL DEFAULT '',
    status SMALLINT NOT NULL DEFAULT 1,
    metadata_hash VARCHAR(128) NOT NULL DEFAULT '',

    CONSTRAINT prediction_event_idx_event_id UNIQUE (event_id)
);
CREATE INDEX IF NOT EXISTS idx_prediction_event_status ON t_prediction_event(status);
