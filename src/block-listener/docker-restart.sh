docker stop block-listener

docker rm block-listener

docker rmi block-listener

    
docker build -t block-listener .

# 运行服务容器并连接到同一网络
docker run -d \
  --name block-listener \
  --network bridge \
  -v $(pwd)/configs:/app/configs \
  -v $(pwd)/logs:/app/logs \
  --add-host=host.docker.internal:host-gateway \
  block-listener

echo "服务已启动，正在检查日志..."
sleep 2
docker logs block-listener 
