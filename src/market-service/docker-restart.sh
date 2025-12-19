docker stop market-service

docker rm market-service

docker rmi market-service

    
docker build -t market-service .

# 运行服务容器并连接到同一网络
docker run -d \
  --name market-service \
  --network bridge \
  -p 9000:9000 \
  -v $(pwd)/configs:/app/configs \
  -v $(pwd)/logs:/app/logs \
  --add-host=host.docker.internal:host-gateway \
  market-service

echo "服务已启动，正在检查日志..."
sleep 2
docker logs market-service 
