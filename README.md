# Bayes Backend

## 快速启动

- configs/xxx/config.yaml 为配置文件，需根据实际情况修改，可见使用 `docs/示例配置`
- 一键启动（根目录）：
  - `docker compose up -d --build`
- 端口映射：
  - `127.0.0.1:8000` → `market-backend`
  - `127.0.0.1:9000` → `market-service`
  - `127.0.0.1:8080` → `block-listener`
  - `127.0.0.1:50051` → `recommendation`
  - `127.0.0.1:5432` → `PostgreSQL`
  - `127.0.0.1:6379` → `Redis`

## 配置
- 外置配置位于 `configs/*/config.yaml`
- 推荐服务环境变量示例：`recommendation/.env.example`
- 示例最小配置：`docs/示例配置/*`

## 文档
- 项目总览与配置指南：`docs/项目总览与配置指南.md`
- 数据库初始化与运维：`docs/数据库初始化与运维.md`

## 容器网络配置
- 容器内服务已使用 `configs/*` 的配置文件，直连 `market-postgres` 与 `market-redis`，以及 `market-service` 与 `recommendation` 服务名
- 若需要宿主机 DB/Redis，请改为挂载 `configs/*/config.yaml` 或调整地址为 `host.docker.internal`
