-- 目标库：embedding
-- 用途：推荐服务的向量存储表结构（pgvector）
-- 注意：由 init 脚本在 \connect embedding 后加载

-- pgvector 扩展与向量表初始化
CREATE EXTENSION IF NOT EXISTS vector;

-- 数据嵌入表：复合主键 (id, pattern)
CREATE TABLE IF NOT EXISTS data_embedding (
    id TEXT NOT NULL,
    pattern TEXT NOT NULL,
    embedding vector(384) NOT NULL,
    PRIMARY KEY (id, pattern)
);

-- 模式嵌入表：主键 id
CREATE TABLE IF NOT EXISTS pattern_embedding (
    id TEXT PRIMARY KEY,
    embedding vector(384) NOT NULL
);
