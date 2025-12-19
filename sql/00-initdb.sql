-- 初始化所有业务数据库与表结构
-- 说明：
-- - 本文件由容器的 Postgres 初始化自动执行
-- - 其余表结构文件位于 /sql 目录，仅通过本文件的 \i 指令按库加载
-- - 数据库列表：usercenter、marketcenter、embedding、chain_events
-- - 注意：请勿直接将 /sql 目录挂载到 /docker-entrypoint-initdb.d，否则会造成表结构在错误库中执行

-- 创建数据库
CREATE DATABASE usercenter;
CREATE DATABASE marketcenter;
CREATE DATABASE embedding;
CREATE DATABASE chain_events;

-- 加载 Usercenter 表结构
\connect usercenter
\i /sql/market-service.usercenter.sql

-- 加载 Marketcenter 表结构
\connect marketcenter
\i /sql/market-service.marketcenter.sql

-- 加载 Recommendation（Embedding）表结构
\connect embedding
\i /sql/recommendation.sql

-- 加载链上事件（Block Listener）表结构
\connect chain_events
\i /sql/block-listener.sql
