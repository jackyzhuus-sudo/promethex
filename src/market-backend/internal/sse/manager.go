package sse

import (
	"context"
	"encoding/json"
	"fmt"
	"market-backend/internal/data"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/google/wire"
)

// ProviderSet is service providers.
var ProviderSet = wire.NewSet(NewConnectionManager)

// Connection SSE连接
type Connection struct {
	ID       string // 连接唯一ID
	UID      string // 用户ID，未登录为空
	Writer   http.ResponseWriter
	Done     chan struct{} // 连接关闭信号
	MsgChan  chan SSEMsg   // 消息channel，每个连接独立
	LastPing time.Time
	mu       sync.RWMutex
}

// UpdateLastPing 更新最后心跳时间
func (c *Connection) UpdateLastPing() {
	c.mu.Lock()
	c.LastPing = time.Now()
	c.mu.Unlock()
}

// ConnectionManager SSE连接管理器
type ConnectionManager struct {
	connections         map[string]*Connection            // connectionID -> Connection
	userConnections     map[string]map[string]*Connection // uid -> map[connectionID]*Connection (仅登录用户)
	marketSubscriptions map[string]map[string]*Connection // topic -> map[connectionID]*Connection (市场频道订阅)
	mu                  sync.RWMutex
	logger              log.Logger
	redisClient         *redis.Client // 用于普通Redis操作（publish等）
	subRedisClient      *redis.Client // 专门用于订阅
	ctx                 context.Context
	cancel              context.CancelFunc
}

// NewConnectionManager 创建连接管理器
func NewConnectionManager(data *data.Data, logger log.Logger) *ConnectionManager {
	ctx, cancel := context.WithCancel(context.Background())

	// 创建专门用于订阅的Redis客户端
	subRedisClient := redis.NewClient(data.RedisClient.Options())

	cm := &ConnectionManager{
		connections:         make(map[string]*Connection),
		userConnections:     make(map[string]map[string]*Connection),
		marketSubscriptions: make(map[string]map[string]*Connection),
		logger:              logger,
		redisClient:         data.RedisClient, // 用于普通操作
		subRedisClient:      subRedisClient,   // 专门用于订阅
		ctx:                 ctx,
		cancel:              cancel,
	}

	// 启动后台任务
	go cm.subscribeRedisMessages()

	return cm
}

// AddConnection 添加连接
func (cm *ConnectionManager) AddConnection(uid string, w http.ResponseWriter) (*Connection, error) {
	conn := &Connection{
		ID:       uuid.New().String(),
		UID:      uid,
		Writer:   w,
		Done:     make(chan struct{}),
		MsgChan:  make(chan SSEMsg, 1000),
		LastPing: time.Now(),
	}

	cm.mu.Lock()
	defer cm.mu.Unlock()

	// 1. 添加到全局连接表
	cm.connections[conn.ID] = conn

	// 2. 如果是登录用户，添加到用户连接映射
	if uid != "" {
		if cm.userConnections[uid] == nil {
			cm.userConnections[uid] = make(map[string]*Connection)
		}
		cm.userConnections[uid][conn.ID] = conn
	}

	log.Infof("SSE连接建立: 连接ID=%s, 用户=%s", conn.ID, uid)
	return conn, nil
}

// RemoveConnection 移除连接
func (cm *ConnectionManager) RemoveConnection(connectionID string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	conn := cm.connections[connectionID]
	if conn == nil {
		return
	}

	// 1. 从全局连接表移除
	delete(cm.connections, connectionID)

	// 2. 从用户连接映射移除（如果是登录用户）
	if conn.UID != "" {
		delete(cm.userConnections[conn.UID], connectionID)
		// 如果用户没有连接了，删除用户entry
		if len(cm.userConnections[conn.UID]) == 0 {
			delete(cm.userConnections, conn.UID)
		}
	}

	// 3. 从所有市场订阅中移除
	for topic := range cm.marketSubscriptions {
		delete(cm.marketSubscriptions[topic], connectionID)
		// 如果topic没有订阅者了，删除topic entry
		if len(cm.marketSubscriptions[topic]) == 0 {
			delete(cm.marketSubscriptions, topic)
		}
	}

	close(conn.Done)
	close(conn.MsgChan)

	log.Infof("SSE连接移除: 连接ID=%s, 用户=%s", connectionID, conn.UID)
}

func (cm *ConnectionManager) GetConnection(connectionID string) *Connection {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.connections[connectionID]
}

// GetUserConnections 获取用户的所有连接
func (cm *ConnectionManager) GetUserConnections(uid string) []*Connection {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	userConns := cm.userConnections[uid]
	if len(userConns) == 0 {
		return nil
	}

	// 返回连接列表
	var result []*Connection
	for _, conn := range userConns {
		result = append(result, conn)
	}
	return result
}

// SubscribeToMarketTopic 订阅市场频道（无需认证）
func (cm *ConnectionManager) SubscribeToMarketTopic(connectionID, topic string) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// 检查连接是否存在
	conn, exists := cm.connections[connectionID]
	if !exists {
		return fmt.Errorf("连接不存在")
	}

	// 添加订阅
	if cm.marketSubscriptions[topic] == nil {
		cm.marketSubscriptions[topic] = make(map[string]*Connection)
	}
	cm.marketSubscriptions[topic][connectionID] = conn

	log.Infof("连接 %s 订阅市场频道: %s", connectionID, topic)
	return nil
}

// UnsubscribeFromMarketTopic 取消订阅市场频道
func (cm *ConnectionManager) UnsubscribeFromMarketTopic(connectionID, topic string) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.marketSubscriptions[topic] != nil {
		delete(cm.marketSubscriptions[topic], connectionID)
		log.Infof("连接 %s 取消订阅市场频道: %s", connectionID, topic)

		// 如果topic没有订阅者了，删除topic entry
		if len(cm.marketSubscriptions[topic]) == 0 {
			delete(cm.marketSubscriptions, topic)
		}
	}
	return nil
}

// BroadcastToAll 广播频道：推送给所有连接
func (cm *ConnectionManager) BroadcastToAll(msg Msg) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	for _, conn := range cm.connections {
		cm.sendToConnection(conn, msg)
	}
}

// BroadcastToLoggedInUsers 用户频道：只推送给登录用户
func (cm *ConnectionManager) BroadcastToLoggedInUsers(msg Msg) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	for _, conn := range cm.connections {
		if conn.UID != "" {
			cm.sendToConnection(conn, msg)
		}
	}
}

// SendToUser 推送给特定用户的所有连接
func (cm *ConnectionManager) SendToUser(uid string, msg Msg) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	userConns := cm.userConnections[uid]
	for _, conn := range userConns {
		cm.sendToConnection(conn, msg)
	}
}

// SendToMarketTopic 市场频道：推送给订阅了该topic的连接
func (cm *ConnectionManager) SendToMarketTopic(topic string, msg Msg) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	subscribers := cm.marketSubscriptions[topic]
	for _, conn := range subscribers {
		cm.sendToConnection(conn, msg)
	}
}

// subscribeRedisMessages 订阅Redis消息
func (cm *ConnectionManager) subscribeRedisMessages() {
	pubsub := cm.subRedisClient.PSubscribe(cm.ctx,
		"sse:broadcast*", // 广播频道 -> BroadcastToAll
		"sse:user:*",     // 用户频道 -> SendToUser 或 BroadcastToLoggedInUsers
		"sse:market:*",   // 市场频道 -> SendToMarketTopic
	)
	defer pubsub.Close()

	log.Info("Started Redis message subscription for SSE")

	for {
		select {
		case <-cm.ctx.Done():
			return
		case msg := <-pubsub.Channel():
			if msg == nil {
				continue
			}

			var message Msg
			if err := json.Unmarshal([]byte(msg.Payload), &message); err != nil {
				log.Errorf("Failed to parse SSE message: %v", err)
				continue
			}

			switch {
			case msg.Channel == "sse:broadcast":
				cm.BroadcastToAll(message)
			case strings.HasPrefix(msg.Channel, "sse:user:"):
				if uid := strings.TrimPrefix(msg.Channel, "sse:user:"); uid == "all" {
					cm.BroadcastToLoggedInUsers(message)
				} else {
					cm.SendToUser(uid, message)
				}
			case strings.HasPrefix(msg.Channel, "sse:market:"):
				topic := strings.TrimPrefix(msg.Channel, "sse:market:")
				cm.SendToMarketTopic(topic, message)
			}
		}
	}
}

// sendToConnection 发送消息到特定连接
func (cm *ConnectionManager) sendToConnection(conn *Connection, baseMsgData Msg) {
	// 创建SSE消息结构
	sseMsg := SSEMsg{
		Msg:   baseMsgData,
		ID:    uuid.New().String(),
		Retry: 3000,
	}

	// 非阻塞写入消息到连接的channel
	select {
	case conn.MsgChan <- sseMsg:
		// 成功写入
	default:
		// channel满了，说明连接可能有问题，记录警告
		log.Warnf("连接 %s 消息channel已满，跳过此消息", conn.ID)
	}
}

// GetStats 获取连接统计信息
func (cm *ConnectionManager) GetStats() map[string]interface{} {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	totalConnections := len(cm.connections)
	loggedInUsers := len(cm.userConnections)
	var anonymousConnections int

	for _, conn := range cm.connections {
		if conn.UID == "" {
			anonymousConnections++
		}
	}

	return map[string]interface{}{
		"total_connections":     totalConnections,
		"logged_in_users":       loggedInUsers,
		"anonymous_connections": anonymousConnections,
		"timestamp":             time.Now().Unix(),
	}
}

// Close 关闭连接管理器
func (cm *ConnectionManager) Close() {
	cm.cancel()

	cm.mu.Lock()
	defer cm.mu.Unlock()

	for _, conn := range cm.connections {
		close(conn.Done)
		close(conn.MsgChan)
	}
	cm.connections = make(map[string]*Connection)
	cm.userConnections = make(map[string]map[string]*Connection)
	cm.marketSubscriptions = make(map[string]map[string]*Connection)

	// 关闭专门的订阅客户端
	if cm.subRedisClient != nil {
		cm.subRedisClient.Close()
	}
}
