-- 创建扩展
CREATE EXTENSION IF NOT EXISTS "vector";

-- 创建市场表
CREATE TABLE IF NOT EXISTS t_market (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    address VARCHAR(42) NOT NULL,
    tx_hash VARCHAR(66) NOT NULL,
    name VARCHAR(512) NOT NULL DEFAULT '',
    fee INTEGER NOT NULL DEFAULT 0,
    token_type SMALLINT NOT NULL DEFAULT 1,
    is_show SMALLINT NOT NULL DEFAULT 1,
    oracle_address VARCHAR(42) NOT NULL DEFAULT '',
    volume NUMERIC NOT NULL DEFAULT 0,
    participants_count BIGINT NOT NULL DEFAULT 0,
    result VARCHAR(42) NOT NULL DEFAULT '',
    asserted_truthfully BOOLEAN NOT NULL DEFAULT false,
    deadline BIGINT NOT NULL DEFAULT 0,
    assertion_id BYTEA NOT NULL DEFAULT '',
    description TEXT NOT NULL DEFAULT '',
    rules TEXT NOT NULL DEFAULT '',
    rules_url VARCHAR(256) NOT NULL DEFAULT '',
    pic_url VARCHAR(256) NOT NULL DEFAULT '',
    status SMALLINT NOT NULL DEFAULT 1,
    tags JSONB NOT NULL DEFAULT '[]',
    embedding vector(384) DEFAULT NULL,
    block_number BIGINT NOT NULL DEFAULT 0,
    categories jsonb NOT NULL DEFAULT '[]',
    CONSTRAINT market_idx_address UNIQUE (address)
);
CREATE INDEX IF NOT EXISTS idx_market_embedding ON t_market USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);
CREATE INDEX IF NOT EXISTS idx_market_tags ON t_market USING GIN (tags);

-- 创建条件代币表
CREATE TABLE IF NOT EXISTS t_option (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    address VARCHAR(42) NOT NULL,
    market_address VARCHAR(42) NOT NULL,
    name VARCHAR(32) NOT NULL DEFAULT '',
    symbol VARCHAR(32) NOT NULL DEFAULT '',
    description VARCHAR(256) NOT NULL DEFAULT '',
    decimal SMALLINT NOT NULL DEFAULT 0,
    weight INTEGER NOT NULL DEFAULT 0,
    index INTEGER NOT NULL DEFAULT 0,
    pic_url VARCHAR(256) NOT NULL DEFAULT '',
    base_token_type SMALLINT NOT NULL DEFAULT 1,
    
    CONSTRAINT option_idx_address UNIQUE (address)
);
CREATE INDEX IF NOT EXISTS idx_option_market_address ON t_option(market_address);

-- 创建代币价格表
CREATE TABLE IF NOT EXISTS t_option_token_price (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    block_time TIMESTAMPTZ NOT NULL,
    token_address VARCHAR(42) NOT NULL,
    block_number BIGINT NOT NULL DEFAULT 0,
    price NUMERIC NOT NULL DEFAULT 0,
    decimals SMALLINT NOT NULL DEFAULT 0,
    base_token_type SMALLINT NOT NULL DEFAULT 1,
    
    CONSTRAINT token_price_idx_token_block UNIQUE (token_address, block_number)
);
CREATE INDEX IF NOT EXISTS idx_price_token_block_time ON t_option_token_price(token_address, block_time);
CREATE INDEX IF NOT EXISTS idx_price_block_time_token ON t_option_token_price(block_time, token_address);

-- 创建标签表
CREATE TABLE IF NOT EXISTS t_tag (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    tag_name VARCHAR(256) NOT NULL DEFAULT '',
    
    CONSTRAINT tag_idx_name UNIQUE (tag_name)
);

-- 创建用户关注市场表
CREATE TABLE IF NOT EXISTS t_user_market_follow (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    uid VARCHAR(24) NOT NULL,
    market_address VARCHAR(42) NOT NULL,
    base_token_type SMALLINT NOT NULL DEFAULT 1,
    status SMALLINT NOT NULL DEFAULT 1,
    
    CONSTRAINT user_market_follow_idx_uid_market UNIQUE (uid, market_address)
);
CREATE INDEX IF NOT EXISTS idx_follow_market_address ON t_user_market_follow(market_address);

-- 创建用户代币余额表
CREATE TABLE IF NOT EXISTS t_user_token_balance (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    uid VARCHAR(24) NOT NULL,
    token_address VARCHAR(42) NOT NULL,
    market_address VARCHAR(42) NOT NULL,
    balance NUMERIC NOT NULL,
    decimal SMALLINT NOT NULL DEFAULT 6,
    block_number BIGINT NOT NULL DEFAULT 0,
    type SMALLINT NOT NULL DEFAULT 1,
    base_token_type SMALLINT NOT NULL DEFAULT 1,
    avg_buy_price NUMERIC NOT NULL DEFAULT 0,
    status SMALLINT NOT NULL DEFAULT 1,
    
    CONSTRAINT user_token_balance_idx_uid_token UNIQUE (uid, token_address)
);
CREATE INDEX IF NOT EXISTS idx_balance_uid_market ON t_user_token_balance(uid, market_address);
CREATE INDEX IF NOT EXISTS idx_balance_market_token ON t_user_token_balance(market_address);
CREATE INDEX IF NOT EXISTS idx_balance_token_address ON t_user_token_balance(token_address);

-- 创建订单表
CREATE TABLE IF NOT EXISTS t_order (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    uuid VARCHAR(36) NOT NULL,
    uid VARCHAR(24) NOT NULL,
    market_address VARCHAR(42) NOT NULL,
    option_address VARCHAR(42) NOT NULL,
    side SMALLINT NOT NULL DEFAULT 0,
    price NUMERIC NOT NULL DEFAULT 0,
    deal_price NUMERIC NOT NULL DEFAULT 0,
    amount NUMERIC NOT NULL DEFAULT 0,
    min_receive_amount NUMERIC NOT NULL DEFAULT 0,
    receive_amount NUMERIC NOT NULL DEFAULT 0,
    status SMALLINT NOT NULL DEFAULT 3,
    event_processed SMALLINT NOT NULL DEFAULT 2,
    tx_hash VARCHAR(66) NOT NULL DEFAULT '',
    op_hash VARCHAR(66) NOT NULL DEFAULT '',
    deadline TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01 00:00:00+00',
    pnl NUMERIC NOT NULL DEFAULT 0,
    base_token_type SMALLINT NOT NULL DEFAULT 1,
    
    CONSTRAINT order_idx_uuid UNIQUE (uuid),
    CONSTRAINT order_idx_tx_hash UNIQUE (tx_hash)
);
CREATE INDEX IF NOT EXISTS idx_order_uid_market_address ON t_order(uid, market_address);
CREATE INDEX IF NOT EXISTS idx_order_market_address ON t_order(market_address);
CREATE INDEX IF NOT EXISTS idx_order_op_hash ON t_order(op_hash);


-- 创建用户领取结果表
CREATE TABLE IF NOT EXISTS t_user_claim_result (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    uuid VARCHAR(36) NOT NULL,
    uid VARCHAR(24) NOT NULL,
    market_address VARCHAR(42) NOT NULL,
    option_address VARCHAR(42) NOT NULL,
    amount NUMERIC NOT NULL DEFAULT 0,
    base_token_type SMALLINT NOT NULL DEFAULT 1,
    status SMALLINT NOT NULL DEFAULT 3,
    event_processed SMALLINT NOT NULL DEFAULT 2,
    tx_hash VARCHAR(66) NOT NULL DEFAULT '',
    op_hash VARCHAR(66) NOT NULL DEFAULT '',
    
    CONSTRAINT claim_idx_uuid UNIQUE (uuid),
    CONSTRAINT claim_idx_tx_hash UNIQUE (tx_hash)
);
CREATE INDEX IF NOT EXISTS idx_claim_uid_market ON t_user_claim_result(uid, market_address);
CREATE INDEX IF NOT EXISTS idx_claim_market_address ON t_user_claim_result(market_address);
CREATE INDEX IF NOT EXISTS idx_claim_option_address ON t_user_claim_result(option_address);
CREATE INDEX IF NOT EXISTS idx_claim_op_hash ON t_user_claim_result(op_hash);

-- 创建用户资产总价值表
CREATE TABLE IF NOT EXISTS t_user_asset_value (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    time TIMESTAMPTZ NOT NULL,
    uid VARCHAR(24) NOT NULL,
    asset_address VARCHAR(42) NOT NULL,
    value NUMERIC NOT NULL DEFAULT 0,
    balance NUMERIC NOT NULL DEFAULT 0,
    portfolio NUMERIC NOT NULL DEFAULT 0,
    pnl NUMERIC NOT NULL DEFAULT 0,
    base_token_type SMALLINT NOT NULL DEFAULT 1,
    
    CONSTRAINT asset_value_idx_uid_asset_time UNIQUE (uid, asset_address, time)
);
CREATE INDEX IF NOT EXISTS idx_asset_value_uid_time ON t_user_asset_value(uid, time);
CREATE INDEX IF NOT EXISTS idx_asset_value_time ON t_user_asset_value(time);

-- 创建交易表
CREATE TABLE IF NOT EXISTS t_send_tx (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    tx_hash VARCHAR(66) NOT NULL DEFAULT '',
    op_hash VARCHAR(66) NOT NULL DEFAULT '',
    status SMALLINT NOT NULL DEFAULT 0,
    chain VARCHAR(32) NOT NULL DEFAULT 'arb',
    err_msg VARCHAR(1024) NOT NULL DEFAULT '',
    retry_count SMALLINT NOT NULL DEFAULT 0,
    source SMALLINT NOT NULL DEFAULT 0,
    type SMALLINT NOT NULL DEFAULT 0,
    user_operation JSONB NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_send_tx_tx_hash ON t_send_tx(tx_hash);
CREATE INDEX IF NOT EXISTS idx_send_tx_op_hash ON t_send_tx(op_hash);
CREATE INDEX IF NOT EXISTS idx_send_tx_user_operation ON t_send_tx USING GIN (user_operation); 
