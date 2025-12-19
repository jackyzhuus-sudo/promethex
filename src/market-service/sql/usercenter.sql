-- 创建用户表
CREATE TABLE IF NOT EXISTS t_user (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    uid VARCHAR(24) NOT NULL,
    eoa_address VARCHAR(42) NOT NULL,
    address VARCHAR(42) NOT NULL,
    email VARCHAR(256) NOT NULL DEFAULT '',
    avatar VARCHAR(256) NOT NULL DEFAULT '',
    description VARCHAR(256) NOT NULL DEFAULT '',
    name VARCHAR(128) NOT NULL DEFAULT '',
    invite_code VARCHAR(16) NOT NULL DEFAULT '',
    inviter_uid VARCHAR(24) NOT NULL DEFAULT '',
    issuer VARCHAR(64) NOT NULL DEFAULT '',
    follower_count BIGINT NOT NULL DEFAULT 0,
    follow_count BIGINT NOT NULL DEFAULT 0,
    post_count BIGINT NOT NULL DEFAULT 0,
    
    CONSTRAINT user_idx_uid UNIQUE (uid),
    CONSTRAINT user_idx_eoa_address UNIQUE (eoa_address),
    CONSTRAINT user_idx_address UNIQUE (address),
    CONSTRAINT user_idx_email UNIQUE (email),
    CONSTRAINT user_idx_invite_code UNIQUE (invite_code)
);

-- 创建用户关注表
CREATE TABLE IF NOT EXISTS t_user_follow (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    uid VARCHAR(24) NOT NULL,
    follow_uid VARCHAR(24) NOT NULL,
    status SMALLINT NOT NULL DEFAULT 0,
    
    CONSTRAINT user_follow_idx_uid_follow_uid UNIQUE (uid, follow_uid)
);

-- 创建用户通知表
CREATE TABLE IF NOT EXISTS t_user_notification (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    uuid VARCHAR(36) NOT NULL,
    uid VARCHAR(24) NOT NULL,
    type SMALLINT NOT NULL DEFAULT 0,
    category SMALLINT NOT NULL DEFAULT 0,
    biz_json JSONB NOT NULL,
    status SMALLINT NOT NULL DEFAULT 2,
    
    CONSTRAINT notification_idx_uuid UNIQUE (uuid)
);
CREATE INDEX IF NOT EXISTS idx_notification_uid ON t_user_notification(uid);
CREATE UNIQUE INDEX IF NOT EXISTS idx_user_post_like_notification ON t_user_notification (uid, (biz_json->>'post_uuid')) WHERE type = 7 AND status = 2;
CREATE INDEX IF NOT EXISTS idx_notification_biz_json ON t_user_notification USING GIN (biz_json);

-- 创建用户动态表
CREATE TABLE IF NOT EXISTS t_user_activity (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    uuid VARCHAR(36) NOT NULL,
    uid VARCHAR(24) NOT NULL,
    activity_type SMALLINT NOT NULL DEFAULT 0,
    biz_json JSONB NOT NULL,
    
    CONSTRAINT activity_idx_uuid UNIQUE (uuid)
);
CREATE INDEX IF NOT EXISTS idx_activity_uid ON t_user_activity(uid);
CREATE INDEX IF NOT EXISTS idx_activity_biz_json ON t_user_activity USING GIN (biz_json);

-- 创建帖子表
CREATE TABLE IF NOT EXISTS t_post (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    uuid VARCHAR(36) NOT NULL,
    uid VARCHAR(24) NOT NULL DEFAULT '',
    market_address VARCHAR(42) NOT NULL DEFAULT '',
    title VARCHAR(256) NOT NULL DEFAULT '',
    content TEXT NOT NULL DEFAULT '',
    like_count BIGINT NOT NULL DEFAULT 0,
    comment_count BIGINT NOT NULL DEFAULT 0,
    view_count BIGINT NOT NULL DEFAULT 0,
    status SMALLINT NOT NULL DEFAULT 1,
    
    CONSTRAINT post_idx_uuid UNIQUE (uuid)
);
CREATE INDEX IF NOT EXISTS idx_post_uid ON t_post(uid);
CREATE INDEX IF NOT EXISTS idx_post_market_address ON t_post(market_address);

-- 创建评论表
CREATE TABLE IF NOT EXISTS t_comment (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    uuid VARCHAR(36) NOT NULL,
    uid VARCHAR(24) NOT NULL DEFAULT '',
    market_address VARCHAR(42) NOT NULL,
    post_uuid VARCHAR(36) NOT NULL,
    root_uuid VARCHAR(36) NOT NULL DEFAULT '',
    parent_uuid VARCHAR(36) NOT NULL DEFAULT '',
    parent_user_uid VARCHAR(24) NOT NULL DEFAULT '',
    content TEXT NOT NULL DEFAULT '',
    status SMALLINT NOT NULL DEFAULT 1,
    like_count BIGINT NOT NULL DEFAULT 0,
    
    CONSTRAINT comment_idx_uuid UNIQUE (uuid)
);
CREATE INDEX IF NOT EXISTS idx_comment_uid ON t_comment(uid);
CREATE INDEX IF NOT EXISTS idx_comment_market_address ON t_comment(market_address);
CREATE INDEX IF NOT EXISTS idx_comment_parent_uuid ON t_comment(parent_uuid);
CREATE INDEX IF NOT EXISTS idx_comment_post_root ON t_comment(post_uuid, root_uuid);
CREATE INDEX IF NOT EXISTS idx_comment_root_parent ON t_comment(root_uuid, parent_uuid);

-- 创建用户点赞表
CREATE TABLE IF NOT EXISTS t_user_like (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    uid VARCHAR(24) NOT NULL,
    content_uuid VARCHAR(36) NOT NULL,
    type SMALLINT NOT NULL DEFAULT 0,
    status SMALLINT NOT NULL DEFAULT 0,
    
    CONSTRAINT user_like_idx_uid_content_type UNIQUE (uid, content_uuid, type)
);
CREATE INDEX IF NOT EXISTS idx_like_content_uuid ON t_user_like(content_uuid);

-- 创建用户社区信息表
CREATE TABLE IF NOT EXISTS t_user_community_info (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    uid VARCHAR(24) NOT NULL,
    post_count BIGINT NOT NULL DEFAULT 0,
    comment_count BIGINT NOT NULL DEFAULT 0,
    like_count BIGINT NOT NULL DEFAULT 0,
    
    CONSTRAINT user_community_idx_uid UNIQUE (uid)
);
