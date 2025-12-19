ALTER TABLE t_send_tx ADD COLUMN uid VARCHAR(24) NOT NULL DEFAULT '';
ALTER TABLE t_send_tx ADD COLUMN base_token_type SMALLINT NOT NULL DEFAULT 1;
CREATE INDEX IF NOT EXISTS idx_send_tx_uid ON t_send_tx(uid);

-- 创建用户mint积分表
CREATE TABLE IF NOT EXISTS t_user_mint_points (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    uuid VARCHAR(36) NOT NULL,
    uid VARCHAR(24) NOT NULL,
    token_address VARCHAR(42) NOT NULL,
    base_token_type SMALLINT NOT NULL DEFAULT 1,
    amount NUMERIC NOT NULL DEFAULT 0,
    status SMALLINT NOT NULL DEFAULT 3,
    source SMALLINT NOT NULL DEFAULT 0,
    event_processed SMALLINT NOT NULL DEFAULT 2,
    tx_hash VARCHAR(66) NOT NULL DEFAULT '',
    op_hash VARCHAR(66) NOT NULL DEFAULT '',
    invite_uid VARCHAR(24) NOT NULL DEFAULT ''
);

-- 创建用户mint积分表索引
CREATE UNIQUE INDEX IF NOT EXISTS idx_user_mint_points_uuid ON t_user_mint_points(uuid);
CREATE UNIQUE INDEX IF NOT EXISTS idx_user_mint_points_tx_hash ON t_user_mint_points(tx_hash);
CREATE INDEX IF NOT EXISTS idx_user_mint_points_uid ON t_user_mint_points(uid);
CREATE INDEX IF NOT EXISTS idx_user_mint_points_status ON t_user_mint_points(status);
CREATE INDEX IF NOT EXISTS idx_user_mint_points_source ON t_user_mint_points(source);

-- 创建用户转账代币表
CREATE TABLE IF NOT EXISTS t_user_transfer_tokens (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    uuid VARCHAR(36) NOT NULL,
    uid VARCHAR(24) NOT NULL,
    external_address VARCHAR(42) NOT NULL,
    token_address VARCHAR(42) NOT NULL,
    side SMALLINT NOT NULL DEFAULT 1,
    base_token_type SMALLINT NOT NULL DEFAULT 1,
    amount NUMERIC NOT NULL DEFAULT 0,
    status SMALLINT NOT NULL DEFAULT 3,
    event_processed SMALLINT NOT NULL DEFAULT 2,
    tx_hash VARCHAR(66) NOT NULL DEFAULT '',
    op_hash VARCHAR(66) NOT NULL DEFAULT ''
);

-- 创建用户转账代币表索引
CREATE UNIQUE INDEX IF NOT EXISTS idx_user_transfer_tokens_uuid ON t_user_transfer_tokens(uuid);
--- CREATE UNIQUE INDEX IF NOT EXISTS idx_user_transfer_tokens_tx_hash ON t_user_transfer_tokens(tx_hash);
CREATE INDEX IF NOT EXISTS idx_user_transfer_tokens_uid ON t_user_transfer_tokens(uid);
CREATE INDEX IF NOT EXISTS idx_user_transfer_tokens_status ON t_user_transfer_tokens(status);
CREATE INDEX IF NOT EXISTS idx_user_transfer_tokens_side ON t_user_transfer_tokens(side);
CREATE INDEX IF NOT EXISTS idx_user_transfer_tokens_base_token_type ON t_user_transfer_tokens(base_token_type);




-- usercenter库
ALTER TABLE t_user ADD COLUMN privy_user_info jsonb not null default '{}';
ALTER TABLE t_user ADD COLUMN source int8 not null default 1;
ALTER TABLE t_user DROP CONSTRAINT user_idx_email;
CREATE INDEX IF NOT EXISTS idx_user_email ON t_user(email);
-- 创建email和source字段的联合索引
ALTER TABLE t_user ADD CONSTRAINT idx_user_email_source UNIQUE (email, source);

ALTER TABLE t_user_notification ADD COLUMN base_token_type SMALLINT NOT NULL DEFAULT 1;

