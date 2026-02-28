.PHONY: tunnel tunnel-stop tunnel-domains vps-status help

# VPS 部署查看 (需设置 PROME_VPS=user@host，可选 PROME_BACKEND_PATH=/path/to/prome/backend)
PROME_BACKEND_PATH ?= backend
vps-status:
	@if [ -z "$(PROME_VPS)" ]; then \
		echo "用法: make vps-status PROME_VPS=user@host"; \
		echo "可选: PROME_BACKEND_PATH=/path/to/prome/backend"; \
		echo ""; \
		echo "或在 VPS 上直接执行:"; \
		echo "  ssh \$${PROME_VPS} 'cd \$${PROME_BACKEND_PATH} && docker compose ps'"; \
		exit 1; \
	fi
	@ssh "$(PROME_VPS)" "cd $(PROME_BACKEND_PATH) && docker compose ps && echo '' && docker ps -a --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'"

# Cloudflare 隧道配置
TUNNEL_LOG := /tmp/cloudflared.log

# 启动隧道 (tmux 会话名: cf)
tunnel:
	@echo "🚀 启动 Cloudflare 隧道..."
	@tmux kill-session -t cf 2>/dev/null || true
	@tmux new-session -d -s cf "cloudflared tunnel --url http://localhost:8000 2>&1 | tee $(TUNNEL_LOG)"
	@echo "⏳ 等待隧道创建..."
	@sleep 5
	@$(MAKE) tunnel-domains

# 关闭隧道
tunnel-stop:
	@echo "🛑 关闭隧道..."
	@tmux kill-session -t cf 2>/dev/null && echo "✅ 隧道已关闭" || echo "⚠️  没有运行中的隧道"
	@pkill cloudflared 2>/dev/null || true

# 打印隧道域名
tunnel-domains:
	@echo ""
	@echo "🌐 market-backend (8000) 隧道域名:"
	@echo "================================================"
	@grep "trycloudflare.com" $(TUNNEL_LOG) 2>/dev/null | tail -1 | sed 's/.*https/https/' | sed 's/|.*//' || echo "⏳ 等待中..."
	@echo "================================================"

# 帮助信息
help:
	@echo "Cloudflare 隧道管理命令:"
	@echo ""
	@echo "  make tunnel         - 启动隧道 (tmux 会话名: cf)"
	@echo "  make tunnel-stop    - 关闭隧道"
	@echo "  make tunnel-domains - 打印隧道域名"
