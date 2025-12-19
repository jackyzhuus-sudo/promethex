#!/bin/bash

# Lark通知脚本
# 用法: ./notify.sh <status> <version> [error_message]
# 示例: ./notify.sh success v1.2.3
#       ./notify.sh failed v1.2.3 "镜像拉取失败"

# 配置
LARK_WEBHOOK_TOKEN="${LARK_WEBHOOK_TOKEN}"  # 从环境变量读取hook token
LARK_WEBHOOK_URL="https://open.larksuite.com/open-apis/bot/v2/hook/${LARK_WEBHOOK_TOKEN}"
SERVICE_NAME="block-listener"
HOSTNAME=$(hostname)

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 日志函数
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 帮助信息
show_help() {
    cat << EOF
📢 Lark部署通知脚本

用法: $0 <status> <version> [error_message]

参数:
  status         通知状态: success 或 failed
  version        部署版本号 (如: v1.2.3)
  error_message  失败时的错误信息 (可选)

示例:
  $0 success v1.2.3
  $0 failed v1.2.3 "镜像拉取超时"

环境变量:
  LARK_WEBHOOK_TOKEN  Lark机器人的webhook token (必需)

EOF
}

# 检查参数
if [[ "$1" == "-h" || "$1" == "--help" ]]; then
    show_help
    exit 0
fi

if [[ $# -lt 2 ]]; then
    log_error "参数不足"
    show_help
    exit 1
fi

STATUS="$1"
VERSION="$2"
ERROR_MESSAGE="$3"

# 检查webhook配置
if [[ -z "$LARK_WEBHOOK_TOKEN" ]]; then
    log_error "未设置 LARK_WEBHOOK_TOKEN 环境变量"
    log_warn "请设置: export LARK_WEBHOOK_TOKEN='你的webhook token'"
    exit 1
fi

# 获取当前时间
CURRENT_TIME=$(date '+%Y-%m-%d %H:%M:%S')

# 获取服务状态信息
get_service_info() {
    local container_status=""
    local image_info=""
    local uptime=""
    
    if docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Image}}" | grep -q "$SERVICE_NAME"; then
        container_status="✅ 运行中"
        image_info=$(docker inspect "$SERVICE_NAME" --format='{{.Config.Image}}' 2>/dev/null || echo "无法获取")
        uptime=$(docker ps --format "{{.Status}}" --filter "name=$SERVICE_NAME" 2>/dev/null || echo "未知")
    else
        container_status="❌ 未运行"
        image_info="N/A"
        uptime="N/A"
    fi
    
    echo "$container_status|$image_info|$uptime"
}

# 发送成功通知
send_success_notification() {
    local version="$1"
    local service_info=$(get_service_info)
    local container_status=$(echo "$service_info" | cut -d'|' -f1)
    local image_info=$(echo "$service_info" | cut -d'|' -f2)
    local uptime=$(echo "$service_info" | cut -d'|' -f3)
    
    # 构造消息内容
    local message="{
        \"msg_type\": \"interactive\",
        \"card\": {
            \"config\": {
                \"wide_screen_mode\": true,
                \"enable_forward\": true
            },
            \"elements\": [
                {
                    \"tag\": \"div\",
                    \"text\": {
                        \"content\": \"**✅ 部署成功**\",
                        \"tag\": \"lark_md\"
                    }
                },
                {
                    \"tag\": \"hr\"
                },
                {
                    \"tag\": \"div\",
                    \"fields\": [
                        {
                            \"is_short\": true,
                            \"text\": {
                                \"content\": \"**📦 版本**\\n$version\",
                                \"tag\": \"lark_md\"
                            }
                        },
                        {
                            \"is_short\": true,
                            \"text\": {
                                \"content\": \"**🖥️ 服务器**\\n$HOSTNAME\",
                                \"tag\": \"lark_md\"
                            }
                        },
                        {
                            \"is_short\": true,
                            \"text\": {
                                \"content\": \"**📊 容器状态**\\n$container_status\",
                                \"tag\": \"lark_md\"
                            }
                        },
                        {
                            \"is_short\": true,
                            \"text\": {
                                \"content\": \"**⏰ 完成时间**\\n$CURRENT_TIME\",
                                \"tag\": \"lark_md\"
                            }
                        }
                    ]
                },
                {
                    \"tag\": \"div\",
                    \"text\": {
                        \"content\": \"**🏗️ 镜像信息**\\n\`$image_info\`\",
                        \"tag\": \"lark_md\"
                    }
                },
                {
                    \"tag\": \"div\",
                    \"text\": {
                        \"content\": \"**⏱️ 运行时长**\\n$uptime\",
                        \"tag\": \"lark_md\"
                    }
                }
            ],
            \"header\": {
                \"template\": \"green\",
                \"title\": {
                    \"content\": \"🎉 $SERVICE_NAME 部署成功\",
                    \"tag\": \"plain_text\"
                }
            }
        }
    }"
    
    # 发送消息
    local response=$(curl -s -w "HTTPSTATUS:%{http_code}" \
        -X POST \
        -H "Content-Type: application/json" \
        -d "$message" \
        "$LARK_WEBHOOK_URL")
    
    local http_code=$(echo "$response" | tr -d '\n' | sed -e 's/.*HTTPSTATUS://')
    local body=$(echo "$response" | sed -e 's/HTTPSTATUS:.*//g')
    
    if [[ "$http_code" -eq 200 ]]; then
        log_info "✅ 成功通知已发送到Lark群"
    else
        log_error "❌ Lark通知发送失败 (HTTP: $http_code)"
        log_error "响应: $body"
    fi
}

# 发送失败通知
send_failure_notification() {
    local version="$1"
    local error_msg="$2"
    
    if [[ -z "$error_msg" ]]; then
        error_msg="未知错误"
    fi
    
    # 构造消息内容
    local message="{
        \"msg_type\": \"interactive\",
        \"card\": {
            \"config\": {
                \"wide_screen_mode\": true,
                \"enable_forward\": true
            },
            \"elements\": [
                {
                    \"tag\": \"div\",
                    \"text\": {
                        \"content\": \"**❌ 部署失败**\",
                        \"tag\": \"lark_md\"
                    }
                },
                {
                    \"tag\": \"hr\"
                },
                {
                    \"tag\": \"div\",
                    \"fields\": [
                        {
                            \"is_short\": true,
                            \"text\": {
                                \"content\": \"**📦 版本**\\n$version\",
                                \"tag\": \"lark_md\"
                            }
                        },
                        {
                            \"is_short\": true,
                            \"text\": {
                                \"content\": \"**🖥️ 服务器**\\n$HOSTNAME\",
                                \"tag\": \"lark_md\"
                            }
                        },
                        {
                            \"is_short\": false,
                            \"text\": {
                                \"content\": \"**⏰ 失败时间**\\n$CURRENT_TIME\",
                                \"tag\": \"lark_md\"
                            }
                        }
                    ]
                },
                {
                    \"tag\": \"div\",
                    \"text\": {
                        \"content\": \"**💥 错误信息**\\n\`$error_msg\`\",
                        \"tag\": \"lark_md\"
                    }
                },
                {
                    \"tag\": \"div\",
                    \"text\": {
                        \"content\": \"**🔄 建议操作**\\n• 检查网络连接\\n• 验证镜像版本是否存在\\n• 查看容器日志: \`docker logs $SERVICE_NAME\`\\n• 重试部署: \`./deploy.sh $version\`\",
                        \"tag\": \"lark_md\"
                    }
                }
            ],
            \"header\": {
                \"template\": \"red\",
                \"title\": {
                    \"content\": \"🚨 $SERVICE_NAME 部署失败\",
                    \"tag\": \"plain_text\"
                }
            }
        }
    }"
    
    # 发送消息
    local response=$(curl -s -w "HTTPSTATUS:%{http_code}" \
        -X POST \
        -H "Content-Type: application/json" \
        -d "$message" \
        "$LARK_WEBHOOK_URL")
    
    local http_code=$(echo "$response" | tr -d '\n' | sed -e 's/.*HTTPSTATUS://')
    local body=$(echo "$response" | sed -e 's/HTTPSTATUS:.*//g')
    
    if [[ "$http_code" -eq 200 ]]; then
        log_info "✅ 失败通知已发送到Lark群"
    else
        log_error "❌ Lark通知发送失败 (HTTP: $http_code)"
        log_error "响应: $body"
    fi
}

# 主逻辑
main() {
    log_info "📢 准备发送Lark通知..."
    log_info "状态: $STATUS, 版本: $VERSION"
    
    case "$STATUS" in
        "success")
            send_success_notification "$VERSION"
            ;;
        "failed")
            send_failure_notification "$VERSION" "$ERROR_MESSAGE"
            ;;
        *)
            log_error "无效的状态: $STATUS (支持: success, failed)"
            exit 1
            ;;
    esac
}

# 执行主函数
main "$@" 
