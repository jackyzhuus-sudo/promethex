# 第一阶段：构建二进制文件
FROM pytorch/pytorch:2.6.0-cuda12.6-cudnn9-runtime

# 设置工作目录
WORKDIR /app

# 复制依赖文件（缓存优化）
COPY requirements.txt ./

# 安装 Python 依赖
RUN pip install -r requirements.txt

# 复制应用代码（缓存优化）
COPY . /app

# Define environment variable
ENV PYTHONUNBUFFERED=1
ENV DB_DRIVER=psycopg
ENV DB_USER=myuser
ENV DB_PASSWORD=mypassword
ENV DB_HOST=db
ENV DB_PORT=5432
ENV DB_NAME=embedding
ENV DB_ECHO=false
ENV GRPC_WORKERS=10
ENV GRPC_PORT=50051

# Run the application
CMD ["python", "main.py"]
