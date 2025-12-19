-- marketcenter库
alter table t_user_mint_points add column user_task_uuid varchar(36) not null default '';


-- usercenter库
-- 创建用户mint积分表
CREATE TABLE IF NOT EXISTS t_task(
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    uuid VARCHAR(36) NOT NULL,
    key VARCHAR(128) NOT NULL,
    is_show SMALLINT NOT NULL DEFAULT 1,
    type SMALLINT NOT NULL DEFAULT 1,
    name VARCHAR(512) NOT NULL DEFAULT '',
    description VARCHAR(1024) NOT NULL DEFAULT '',
    pic_url VARCHAR(512) NOT NULL DEFAULT '',
    reward BIGINT NOT NULL DEFAULT 0,
    jump_url VARCHAR(512) NOT NULL DEFAULT '',

    CONSTRAINT task_idx_uuid UNIQUE (uuid),
    CONSTRAINT task_idx_key UNIQUE (key)
);


CREATE TABLE IF NOT EXISTS t_user_task(
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    uuid VARCHAR(36) NOT NULL,
    uid VARCHAR(24) NOT NULL,
    task_uuid VARCHAR(36) NOT NULL,
    task_key VARCHAR(128) NOT NULL,
    reward BIGINT NOT NULL DEFAULT 0,
    claimed SMALLINT NOT NULL DEFAULT 2,
    claimed_at BIGINT NOT NULL DEFAULT 0,

    CONSTRAINT user_task_idx_uuid UNIQUE (uuid)
);

CREATE INDEX IF NOT EXISTS idx_user_task_uid_and_task_key ON t_user_task(uid, task_key);
CREATE INDEX IF NOT EXISTS idx_user_task_task_uuid ON t_user_task(task_uuid);