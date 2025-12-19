package bayes_sse

import (
	"context"
	"encoding/json"
	"fmt"
	"market-backend/internal/conf"
	"market-backend/internal/data"
	"market-backend/internal/pkg"
	"market-backend/internal/pkg/middleware"
	"market-backend/internal/pkg/util"
	"market-backend/internal/sse"
	bayespb "market-proto/proto/market-backend/v1"
	"net/http"
	"strings"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/uuid"
)

type BayesSseService struct {
	bayespb.BayesSseServer
	manager   *sse.ConnectionManager
	data      *data.Data
	logger    log.Logger
	cfgData   *conf.Data
	cfgCustom *conf.Custom
}

func NewBayesSseService(data *data.Data, cfgData *conf.Data, custom *conf.Custom, logger log.Logger, manager *sse.ConnectionManager) *BayesSseService {
	return &BayesSseService{
		data:      data,
		logger:    logger,
		cfgData:   cfgData,
		cfgCustom: custom,
		manager:   manager,
	}
}

func (s *BayesSseService) Subscribe(ctx context.Context, req *bayespb.SubscribeRequest) (*bayespb.SubscribeReply, error) {
	// 1. 参数校验：topics必须存在
	if len(req.Topics) == 0 {
		return nil, fmt.Errorf("topics不能为空")
	}

	// 2. 获取用户信息（如果有的话）
	uid := util.GetUidFromCtx(ctx)

	// 3. 参数校验：未登录用户必须提供connection_id
	if uid == "" && req.ConnectionId == "" {
		return nil, fmt.Errorf("未登录用户必须提供connection_id")
	}

	if req.ConnectionId != "" && s.manager.GetConnection(req.ConnectionId) == nil {
		return nil, pkg.ErrParam
	}

	for _, topic := range req.Topics {
		if req.ConnectionId != "" {
			if err := s.manager.SubscribeToMarketTopic(req.ConnectionId, topic); err != nil {
				log.Errorf("为连接 %s 订阅topic %s 失败: %v", req.ConnectionId, topic, err)
				return nil, fmt.Errorf("订阅失败: %v", err)
			}
		} else {
			connections := s.manager.GetUserConnections(uid)
			if len(connections) == 0 {
				return nil, fmt.Errorf("用户没有活跃的SSE连接")
			}

			for _, conn := range connections {
				if err := s.manager.SubscribeToMarketTopic(conn.ID, topic); err != nil {
					log.Errorf("为连接 %s 订阅topic %s 失败: %v", conn.ID, topic, err)
				}
			}
		}
	}

	return &bayespb.SubscribeReply{}, nil
}

func (s *BayesSseService) Unsubscribe(ctx context.Context, req *bayespb.UnsubscribeRequest) (*bayespb.UnsubscribeReply, error) {
	// 1. 参数校验：topics必须存在
	if len(req.Topics) == 0 {
		return nil, pkg.ErrParam
	}

	// 2. 获取用户信息（如果有的话）
	uid := util.GetUidFromCtx(ctx)

	// 3. 参数校验：未登录用户必须提供connection_id
	if uid == "" && req.ConnectionId == "" {
		return nil, pkg.ErrParam
	}

	if req.ConnectionId != "" && s.manager.GetConnection(req.ConnectionId) == nil {
		return nil, pkg.ErrParam
	}

	for _, topic := range req.Topics {
		if req.ConnectionId != "" {
			// 精确模式：取消订阅指定连接
			if err := s.manager.UnsubscribeFromMarketTopic(req.ConnectionId, topic); err != nil {
				log.Errorf("为连接 %s 取消订阅topic %s 失败: %v", req.ConnectionId, topic, err)
				return nil, fmt.Errorf("取消订阅失败: %v", err)
			}
		} else {
			// 广播模式：给该用户所有连接都取消订阅
			connections := s.manager.GetUserConnections(uid)
			if len(connections) == 0 {
				return nil, fmt.Errorf("用户没有活跃的SSE连接")
			}

			for _, conn := range connections {
				if err := s.manager.UnsubscribeFromMarketTopic(conn.ID, topic); err != nil {
					log.Errorf("为连接 %s 取消订阅topic %s 失败: %v", conn.ID, topic, err)
				}
			}
		}
	}

	return &bayespb.UnsubscribeReply{}, nil
}

// HandleConnection 处理SSE连接请求
func (s *BayesSseService) HandleConnection(w http.ResponseWriter, r *http.Request) {
	// 1. 尝试认证（可选）
	var uid string
	ctx := r.Context() // 使用请求的context

	if authHeader := r.Header.Get("Authorization"); authHeader != "" {
		if userCtx, userID, err := middleware.AuthenticateFromHTTPRequest(r, s.cfgData, s.cfgCustom, s.data); err == nil {
			uid = userID
			ctx = userCtx // 使用认证后的context
		}
		// 认证失败不报错，继续作为未登录用户
	}

	// 2. 设置SSE响应头
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Cache-Control")

	// 3. 建立SSE连接（无论是否登录）
	conn, err := s.manager.AddConnection(uid, w)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to establish SSE connection: %v", err), http.StatusInternalServerError)
		return
	}

	defer s.manager.RemoveConnection(conn.ID)

	log.Infof("SSE连接建立: 连接ID=%s, 用户=%s", conn.ID, uid)

	connectedData := sse.SSEMsg{
		Msg: sse.Msg{
			Event: sse.EventConnected,
			Data:  sse.ConnectionMsgData{ConnectionID: conn.ID},
		},
		ID:    uuid.New().String(),
		Retry: 3000,
	}
	writeSSEMessage(w, connectedData)

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			// 客户端断开连接
			log.Infof("SSE连接关闭: 用户=%s, 连接ID=%s", uid, conn.ID)
			return

		case msg := <-conn.MsgChan:
			// 处理业务消息
			if err := writeSSEMessage(w, msg); err != nil {
				log.Errorf("连接 %s 写入消息失败: %v", conn.ID, err)
				return
			}

		case <-ticker.C:
			// 发送心跳
			pingMsg := sse.SSEMsg{
				Msg: sse.Msg{
					Event: sse.EventPing,
				},
				ID:    uuid.New().String(),
				Retry: 3000,
			}
			if err := writeSSEMessage(w, pingMsg); err != nil {
				log.Errorf("连接 %s 心跳失败: %v", conn.ID, err)
				return
			}

			// 更新心跳时间
			conn.UpdateLastPing()
		}
	}
}

// writeSSEMessage 写入SSE消息
func writeSSEMessage(w http.ResponseWriter, msg sse.SSEMsg) error {

	var sseFormat strings.Builder
	if msg.Event != "" {
		sseFormat.WriteString(fmt.Sprintf("event: %s\n", msg.Event))
	}
	if msg.ID != "" {
		sseFormat.WriteString(fmt.Sprintf("id: %s\n", msg.ID))
	}
	if msg.Retry > 0 {
		sseFormat.WriteString(fmt.Sprintf("retry: %d\n", msg.Retry))
	}
	if msg.Data != nil {
		data, err := json.Marshal(msg.Data)
		if err != nil {
			return fmt.Errorf("msg data marshal failed: %v", err)
		}
		sseFormat.WriteString(fmt.Sprintf("data: %s\n", string(data)))
	}

	// SSE消息必须以两个换行符结尾
	sseFormat.WriteString("\n")

	// 写入到连接
	_, err := w.Write([]byte(sseFormat.String()))
	if err != nil {
		return err
	}

	// 立即flush
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
	return nil
}
