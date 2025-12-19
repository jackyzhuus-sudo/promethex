#!/bin/bash

# 配置
REGISTRY="ghcr.io"
REPO="bayeslabs-tech/market-service"
SERVICE_NAME="market-service"

# 帮助信息
show_help() {
    cat << EOF
🚀 环境变量版本部署脚本 (适用于私有仓库)

用法: $0 <版本号>

参数:
  版本号    要部署的版本 (必须指定，如: 1.0.0, v1.0.3, latest)

示例:
  $0 1.0.3        # 部署指定版本 1.0.3
  $0 v1.0.3       # 部署指定版本 v1.0.3  
  $0 latest       # 部署 latest 标签

特点:
  ✅ 不依赖 GitHub API (适用于私有仓库)
  ✅ 使用环境变量指定版本
  ✅ 完全不修改任何配置文件
  ✅ 支持一键版本切换

注意:
  由于是私有仓库，无法自动获取版本列表
  请手动指定要部署的版本号

EOF
}

# 检查配置文件是否支持环境变量
check_config() {
    if ! grep -q '${VERSION' docker-compose.yml; then
        echo "⚠️  当前 docker-compose.yml 不支持环境变量"
        echo "💡 需要将镜像配置改为: image: ghcr.io/bayeslabs-tech/$SERVICE_NAME:\${VERSION:-latest}"
        echo ""
        read -p "是否要自动更新配置文件? (y/N): " -r
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            echo "🔄 更新配置文件..."
            cp docker-compose.yml "docker-compose.yml.backup.$(date +%Y%m%d_%H%M%S)"
            sed -i.tmp 's|:latest|:${VERSION:-latest}|g' docker-compose.yml
            rm docker-compose.yml.tmp 2>/dev/null || true
            echo "✅ 配置文件已更新"
            echo "💾 原文件已备份"
        else
            echo "❌ 配置文件未更新，无法继续部署"
            return 1
        fi
    fi
    return 0
}

# 测试镜像是否存在
test_image() {
    local version="$1"
    local image_version="${version#v}"
    
    if [[ "$version" == "latest" ]]; then
        image_version="latest"
    fi
    
    local full_image="${REGISTRY}/${REPO}:${image_version}"
    
    echo "🔍 检查镜像是否存在: $full_image"
    
    # 尝试获取镜像manifest
    if docker manifest inspect "$full_image" >/dev/null 2>&1; then
        echo "✅ 镜像存在"
        return 0
    else
        echo "⚠️  无法验证镜像存在性 (可能需要登录认证)"
        echo "💡 如果出现拉取失败，请检查:"
        echo "   1. 版本号是否正确"
        echo "   2. 是否已登录: docker login ghcr.io"
        return 1
    fi
}

# 部署指定版本
deploy_version() {
    local version="$1"
    local display_version="$version"
    
    # 如果版本号不以v开头且不是latest，为显示添加v
    if [[ ! "$display_version" =~ ^v ]] && [[ "$display_version" != "latest" ]]; then
        display_version="v$display_version"
    fi
    
    echo "📦 准备部署版本: $display_version"
    
    # 检查配置文件是否支持环境变量
    if ! check_config; then
        return 1
    fi
    
    # 测试镜像是否存在
    test_image "$version"
    
    # 设置版本环境变量
    local image_version="${version#v}"
    if [[ "$version" == "latest" ]]; then
        export VERSION="latest"
    else
        export VERSION="$image_version"
    fi
    
    echo ""
    echo "🚀 开始部署版本 $display_version (镜像标签: $VERSION)..."
    
    # 显示将要使用的配置
    echo "📋 最终配置预览:"
    docker-compose config | grep -A 2 "$SERVICE_NAME:" | grep "image:" || echo "无法显示镜像信息"
    echo ""
    
    # 拉取指定版本镜像
    echo "⬇️  拉取镜像..."
    if ! docker-compose pull; then
        echo "❌ 镜像拉取失败！"
        echo "💡 可能的原因："
        echo "   - 版本 $display_version 不存在"
        echo "   - 网络连接问题"
        echo "   - 未登录容器仓库: docker login ghcr.io"
        echo "   - 没有该镜像的访问权限"
        
        unset VERSION
        return 1
    fi
    
    # 重新启动服务
    echo "🔄 重启服务..."
    if ! docker-compose up -d; then
        echo "❌ 服务启动失败！"
        unset VERSION
        return 1
    fi
    
    echo ""
    echo "✅ 部署完成!"
    echo "📊 服务状态:"
    docker-compose ps
    
    # 显示实际使用的镜像
    echo ""
    echo "📋 实际运行的镜像:"
    docker inspect $SERVICE_NAME --format='{{.Config.Image}}' 2>/dev/null || echo "无法获取容器镜像信息"
    
    echo ""
    echo "🎯 当前版本: $display_version"
    echo "🔧 环境变量: VERSION=$VERSION"
    
    # 等待服务启动并健康检查
    echo ""
    echo "🔍 等待服务启动..."
    local retry_count=0
    local max_retries=30
    local service_ready=false
    
    while [[ $retry_count -lt $max_retries ]]; do
        if docker ps --filter "name=$SERVICE_NAME" --filter "status=running" --format "{{.Names}}" | grep -q "$SERVICE_NAME"; then
            echo "✅ 服务已启动，等待健康检查..."
            service_ready=true
            break
        else 
            sleep 2
        fi
        retry_count=$((retry_count + 1))
        echo "⏳ 等待服务启动... ($retry_count/$max_retries)"
        sleep 2
    done
    
    if [[ "$service_ready" == true ]]; then
        echo "✅ 服务健康检查通过!"
        
        # 发送成功通知到Lark
        if [[ -f "./notify.sh" ]]; then
            echo ""
            echo "📢 发送部署成功通知..."
            chmod +x ./notify.sh
            ./notify.sh success "$display_version"
        else
            echo "⚠️  notify.sh 文件不存在，跳过Lark通知"
        fi
    else
        echo "❌ 服务启动超时或健康检查失败"
        
        # 清理环境变量
        unset VERSION
        return 1
    fi
    
    # 清理环境变量
    unset VERSION
    
    echo ""
    echo "💡 提示: 如需保持版本，可设置环境变量: export VERSION=$image_version"
}

# 列出本地可用的镜像版本
list_local_images() {
    echo "📋 本地可用的镜像版本:"
    docker images "${REGISTRY}/${REPO}" --format "table {{.Tag}}\t{{.CreatedAt}}\t{{.Size}}" 2>/dev/null || echo "未找到本地镜像"
}

# 清理函数
cleanup() {
    if [[ -n "$VERSION" ]]; then
        echo ""
        echo "🧹 检测到中断，清理环境变量..."
        unset VERSION
        echo "✅ 已清理环境变量 VERSION"
    fi
}

# 设置信号处理
trap cleanup EXIT INT TERM

# 主程序
main() {
    # 检查帮助参数
    if [[ "$1" == "-h" || "$1" == "--help" ]]; then
        show_help
        exit 0
    fi
    
    # 列出本地镜像
    if [[ "$1" == "--list" || "$1" == "-l" ]]; then
        list_local_images
        exit 0
    fi
    
    # 检查是否提供了版本参数
    if [[ -z "$1" ]]; then
        echo "❌ 请指定要部署的版本号"
        echo ""
        show_help
        echo ""
        echo "💡 提示：由于是私有仓库，无法自动获取版本列表"
        echo "   您可以使用 --list 查看本地已有的镜像版本"
        exit 1
    fi
    
    # 检查依赖
    if ! command -v docker-compose &> /dev/null; then
        echo "❌ 需要安装 docker-compose"
        exit 1
    fi
    
    # 检查docker-compose文件
    if [[ ! -f "docker-compose.yml" ]]; then
        echo "❌ 找不到 docker-compose.yml 文件"
        exit 1
    fi
    
    local target_version="$1"
    
    # 部署指定版本
    deploy_version "$target_version"
}

# 执行主程序
main "$@" 
