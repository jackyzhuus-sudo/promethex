# 第一阶段：构建二进制文件
FROM golang:1.25 AS builder

# 设置Go环境变量
ENV CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64 \
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
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags "-s -w" -o /app/bin/market-backend ./cmd/market-backend

# 第二阶段：创建运行环境
FROM alpine:3.23
RUN apk --no-cache add ca-certificates tzdata

# 设置工作目录
WORKDIR /app

# 从构建阶段复制二进制文件和配置
COPY --from=builder /app/bin/market-backend /app/bin/

# 暴露服务端口
EXPOSE 8000

# 添加健康检查
HEALTHCHECK --interval=30s --timeout=3s --retries=3 \
  CMD ps -ef | grep market-backend | grep -v grep || exit 1

# 启动服务
CMD ["/app/bin/market-backend", "-conf", "/app/configs"] 
