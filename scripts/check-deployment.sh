#!/usr/bin/env bash
# 在 Prome Docker VPS 上查看服务部署情况
# 用法:
#   本地在 VPS 上执行:  ./scripts/check-deployment.sh
#   从本机 SSH 执行:    ssh user@your-vps 'cd /path/to/prome/backend && ./scripts/check-deployment.sh'
#   或:                 make vps-status PROME_VPS=user@your-vps PROME_BACKEND_PATH=/path/to/prome/backend

set -e
cd "$(dirname "$0")/.."

echo "=============================================="
echo "  Prome Docker 服务状态"
echo "=============================================="
echo ""

echo "--- docker compose ps ---"
docker compose ps
echo ""

echo "--- 容器健康 (docker ps -a) ---"
docker ps -a --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
echo ""

echo "--- 最近日志 (各服务最后 5 行) ---"
for name in market-postgres market-redis recommendation market-backend block-listener market-service; do
  if docker ps -a --format '{{.Names}}' | grep -q "^${name}$"; then
    echo ">>> $name"
    docker logs "$name" --tail 5 2>&1 || true
    echo ""
  fi
done

echo "=============================================="
echo "  检查完成"
echo "=============================================="
