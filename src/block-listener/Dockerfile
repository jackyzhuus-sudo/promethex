# 第一阶段：构建二进制文件
FROM golang:1.25 AS builder

# 设置Go环境变量
ENV CGO_ENABLED=0 \
    GOOS=linux \
    GOPROXY=https://goproxy.cn,direct

# 设置工作目录
WORKDIR /app

# 复制依赖文件（缓存优化）
COPY go.mod go.sum ./

# 复制外部引入的包到容器内的对应路径
COPY --from=market-proto . /market-proto/

# 下载依赖
RUN go mod download

# 复制当前项目的所有文件
COPY . .

# 编译服务
RUN go build -ldflags "-s -w" -o /app/bin/block-listener ./cmd/block-listener

# 第二阶段：创建运行环境
FROM alpine:3.23
RUN apk --no-cache add ca-certificates tzdata curl \
    && addgroup -S appgroup && adduser -S appuser -G appgroup

# 设置工作目录
WORKDIR /app

# 从构建阶段复制二进制文件和配置
COPY --from=builder /app/bin/block-listener /app/bin/

RUN chown -R appuser:appgroup /app
USER appuser

# 暴露服务端口
EXPOSE 8080

# 添加健康检查
HEALTHCHECK --interval=30s --timeout=5s --retries=3 --start-period=15s \
  CMD curl -f http://localhost:8080/health || exit 1

# 启动服务
CMD ["/app/bin/block-listener", "-conf", "/app/configs"]
