package server

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"block-listener/internal/biz"
	"block-listener/pkg/alarm"
	"block-listener/pkg/common"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/uuid"
)

// EventProcessorServer 实现了Kratos的Server接口
type EventProcessorServer struct {
	processor *biz.EventProcessor
	log       *log.Helper
	logger    log.Logger // 保存原始logger用于common.NewBaseCtx

	// 控制循环的通道
	stopCh    chan struct{}
	doneCh    chan struct{}
	mu        sync.RWMutex
	isRunning bool
}

// NewEventProcessorServer 创建事件处理服务
func NewEventProcessorServer(processor *biz.EventProcessor, logger log.Logger) *EventProcessorServer {
	return &EventProcessorServer{
		processor: processor,
		log:       log.NewHelper(logger),
		logger:    logger, // 保存原始logger
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
		mu:        sync.RWMutex{},
		isRunning: false,
	}
}

// Start 启动事件处理服务
func (s *EventProcessorServer) Start(ctx context.Context) error {
	defer func() {
		if err := recover(); err != nil {
			s.log.Errorf("event processor panic: %v, stack: %s", err, string(debug.Stack()))
		}
	}()

	s.mu.Lock()
	if s.isRunning {
		s.mu.Unlock()
		return nil
	}
	s.isRunning = true
	s.mu.Unlock()

	s.log.Info("事件处理服务启动中")
	ctx = context.WithValue(ctx, "server", "EVENT_PROCESSOR")
	go func() {
		defer func() {
			if err := recover(); err != nil {
				s.log.Errorf("event processor panic: %v, stack: %s", err, string(debug.Stack()))
				msg := fmt.Sprintf("event processor panic: [%v], stack: [%s]", err, string(debug.Stack()))
				alarm.Lark.Send(msg)
			}
		}()
		s.scan(ctx)
	}()
	return nil
}

// Stop 停止事件处理服务
func (s *EventProcessorServer) Stop(ctx context.Context) error {
	s.mu.Lock()
	if !s.isRunning {
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()

	s.log.Info("事件处理服务停止中")
	close(s.stopCh)

	select {
	case <-s.doneCh:
		s.log.Info("事件处理服务已正常停止")
	case <-time.After(time.Second * 30):
		s.log.Warn("等待事件处理停止超时")
	case <-ctx.Done():
		s.log.Warn("上下文取消，事件处理停止")
	}

	s.mu.Lock()
	s.isRunning = false
	s.mu.Unlock()
	return nil
}

// scan 事件处理主循环
func (s *EventProcessorServer) scan(ctx context.Context) {
	defer close(s.doneCh)

	for {
		select {
		case <-s.stopCh:
			s.log.Info("收到停止信号，退出事件处理循环")
			return
		case <-ctx.Done():
			s.log.Info("上下文取消，退出事件处理循环")
			return
		default:
			// 为每次处理创建新的context和traceId
			traceId := uuid.New().String()
			processCtx := context.WithValue(ctx, "server", "EVENT_PROCESSOR")
			processCtx = context.WithValue(processCtx, "traceId", traceId)

			c := common.NewBaseCtx(processCtx, s.logger)
			c.Log.Infof("task event_processor start")
			startTime := time.Now()

			if err := s.processor.Run(processCtx); err != nil {
				c.Log.Errorf("task event_processor failed, err: %v", err)
				alarm.Lark.Send(fmt.Sprintf("task event_processor failed,traceId: %s, err: %v", traceId, err))
				// 错误时休眠1秒后重试
				time.Sleep(1 * time.Second)
			} else {
				c.Log.Infof("task event_processor done, cost: %v", time.Since(startTime))
				// 正常处理完成后休眠1秒
				time.Sleep(1 * time.Second)
			}
		}
	}
}
