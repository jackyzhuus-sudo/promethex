package server

import (
	"context"
	"fmt"
	"market-service/internal/data/base"
	"market-service/internal/pkg/alarm"
	"market-service/internal/pkg/common"
	"market-service/internal/redis_stream_consumer"
	"runtime/debug"
	"sync"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/uuid"
)

// Consumer 定义消费者接口
type Consumer interface {
	Name() string
	Initialize(ctx context.Context) error
	ConsumeOnce(ctx context.Context) error
}

// ConsumerServer 实现了Kratos的Server接口
type ConsumerServer struct {
	consumers map[string]Consumer
	logger    log.Logger
	mu        sync.RWMutex
	infraRepo base.MarketcenterInfra

	// 生命周期管理
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	started bool
}

// NewConsumerServer 创建消费者服务
func NewConsumerServer(
	logger log.Logger,
	mintPointsConsumer *redis_stream_consumer.MintPointsHandler,
	verifyTaskConsumer *redis_stream_consumer.VerifyTaskHandler,
	pnlAllTimeLeaderboardConsumer *redis_stream_consumer.PnlAllTimeLeaderboardHandler,
	pnlDailyLeaderboardConsumer *redis_stream_consumer.PnlDailyLeaderboardHandler,
	pnlMonthlyLeaderboardConsumer *redis_stream_consumer.PnlMonthlyLeaderboardHandler,
	pnlWeeklyLeaderboardConsumer *redis_stream_consumer.PnlWeeklyLeaderboardHandler,
	tradesAllTimeLeaderboardConsumer *redis_stream_consumer.TradesAllTimeLeaderboardHandler,
	tradesDailyLeaderboardConsumer *redis_stream_consumer.TradesDailyLeaderboardHandler,
	tradesMonthlyLeaderboardConsumer *redis_stream_consumer.TradesMonthlyLeaderboardHandler,
	tradesWeeklyLeaderboardConsumer *redis_stream_consumer.TradesWeeklyLeaderboardHandler,
	volumeAllTimeLeaderboardConsumer *redis_stream_consumer.VolumeAllTimeLeaderboardHandler,
	volumeDailyLeaderboardConsumer *redis_stream_consumer.VolumeDailyLeaderboardHandler,
	volumeMonthlyLeaderboardConsumer *redis_stream_consumer.VolumeMonthlyLeaderboardHandler,
	volumeWeeklyLeaderboardConsumer *redis_stream_consumer.VolumeWeeklyLeaderboardHandler,
	infraRepo base.MarketcenterInfra,
) *ConsumerServer {
	srv := &ConsumerServer{
		consumers: make(map[string]Consumer),
		logger:    logger,
		infraRepo: infraRepo,
	}

	// 注册所有消费者
	if err := srv.RegisterConsumer(mintPointsConsumer); err != nil {
		log.Fatalf("register mint points consumer failed, err: %v", err)
	}

	if err := srv.RegisterConsumer(verifyTaskConsumer); err != nil {
		log.Fatalf("register verify task consumer failed, err: %v", err)
	}

	if err := srv.RegisterConsumer(pnlAllTimeLeaderboardConsumer); err != nil {
		log.Fatalf("register pnl all time leaderboard consumer failed, err: %v", err)
	}

	if err := srv.RegisterConsumer(pnlDailyLeaderboardConsumer); err != nil {
		log.Fatalf("register pnl daily leaderboard consumer failed, err: %v", err)
	}

	if err := srv.RegisterConsumer(pnlMonthlyLeaderboardConsumer); err != nil {
		log.Fatalf("register pnl monthly leaderboard consumer failed, err: %v", err)
	}

	if err := srv.RegisterConsumer(pnlWeeklyLeaderboardConsumer); err != nil {
		log.Fatalf("register pnl weekly leaderboard consumer failed, err: %v", err)
	}

	if err := srv.RegisterConsumer(tradesAllTimeLeaderboardConsumer); err != nil {
		log.Fatalf("register trades all time leaderboard consumer failed, err: %v", err)
	}

	if err := srv.RegisterConsumer(tradesDailyLeaderboardConsumer); err != nil {
		log.Fatalf("register trades daily leaderboard consumer failed, err: %v", err)
	}

	if err := srv.RegisterConsumer(tradesMonthlyLeaderboardConsumer); err != nil {
		log.Fatalf("register trades monthly leaderboard consumer failed, err: %v", err)
	}

	if err := srv.RegisterConsumer(tradesWeeklyLeaderboardConsumer); err != nil {
		log.Fatalf("register trades weekly leaderboard consumer failed, err: %v", err)
	}

	if err := srv.RegisterConsumer(volumeAllTimeLeaderboardConsumer); err != nil {
		log.Fatalf("register volume all time leaderboard consumer failed, err: %v", err)
	}

	if err := srv.RegisterConsumer(volumeDailyLeaderboardConsumer); err != nil {
		log.Fatalf("register volume daily leaderboard consumer failed, err: %v", err)
	}

	if err := srv.RegisterConsumer(volumeMonthlyLeaderboardConsumer); err != nil {
		log.Fatalf("register volume monthly leaderboard consumer failed, err: %v", err)
	}

	if err := srv.RegisterConsumer(volumeWeeklyLeaderboardConsumer); err != nil {
		log.Fatalf("register volume weekly leaderboard consumer failed, err: %v", err)
	}

	log.NewHelper(logger).Infof("registered consumers: %+v", srv.ListConsumers())
	return srv
}

// RegisterConsumer 注册消费者
func (s *ConsumerServer) RegisterConsumer(consumer Consumer) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.consumers[consumer.Name()]; exists {
		return fmt.Errorf("consumer %s already exists", consumer.Name())
	}

	s.consumers[consumer.Name()] = consumer
	return nil
}

// Start 启动消费者服务
func (s *ConsumerServer) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.started {
		return fmt.Errorf("ConsumerServer already started")
	}

	logHelper := log.NewHelper(s.logger)
	logHelper.Info("Starting ConsumerServer")

	// 创建可取消的上下文
	s.ctx, s.cancel = context.WithCancel(ctx)

	for name, consumer := range s.consumers {
		s.wg.Add(1)
		go s.runConsumer(name, consumer)
	}

	s.started = true
	logHelper.Info("ConsumerServer started successfully")
	return nil
}

// Stop 停止消费者服务
func (s *ConsumerServer) Stop(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.started {
		return nil
	}

	logHelper := log.NewHelper(s.logger)
	logHelper.Info("Stopping ConsumerServer")

	// 取消上下文，通知所有消费者停止
	if s.cancel != nil {
		s.cancel()
	}

	// 等待所有消费者完成或超时
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		logHelper.Info("ConsumerServer stopped gracefully")
	case <-ctx.Done():
		alarm.Lark.Send("ConsumerServer stop timeout, force closing")
		logHelper.Warn("ConsumerServer stop timeout, force closing")
	}

	s.started = false
	return nil
}

// runConsumer 运行单个消费者
func (s *ConsumerServer) runConsumer(name string, consumer Consumer) {
	defer s.wg.Done()

	logHelper := log.NewHelper(s.logger)
	logHelper.Infof("Consumer %s loop started", name)

	// 初始化消费者
	initCtx := context.WithValue(s.ctx, "server", "CONSUMER-"+name)
	initCtx = context.WithValue(initCtx, "traceId", uuid.New().String())

	if err := consumer.Initialize(initCtx); err != nil {
		logHelper.Errorf("Consumer %s initialize failed: %v", name, err)
		alarm.Lark.Send(fmt.Sprintf("Consumer %s initialize failed: %v", name, err))
		return
	}

	// 消费循环
	for {
		select {
		case <-s.ctx.Done():
			logHelper.Infof("Consumer %s loop stopped by context", name)
			return
		default:
			// 在每次消费循环中处理 panic
			func() {
				defer func() {
					if err := recover(); err != nil {
						logHelper.Errorf("consumer %s panic: %v, stack: %s", name, err, string(debug.Stack()))

						// 发送告警
						alarmMsg := fmt.Sprintf("consumer %s panic: %v, stack: %s", name, err, string(debug.Stack()))
						alarm.Lark.Send(alarmMsg)

						// panic 后短暂等待，避免频繁重试
						s.sleepWithContext(5 * time.Second)
					}
				}()

				// 设置上下文
				consumeCtx := context.WithValue(s.ctx, "server", "CONSUMER-"+name)
				consumeCtx = context.WithValue(consumeCtx, "traceId", uuid.New().String())

				// 获取分布式锁，防止同一个消费者多实例冲突
				commonCtx := common.NewBaseCtx(consumeCtx, s.logger)
				lockKey := fmt.Sprintf("consumer_lock_%s", name)
				lockID, acquired, err := s.infraRepo.AcquireLock(commonCtx, lockKey, 1*time.Minute)

				if err != nil {
					alarm.Lark.Send(fmt.Sprintf("Consumer %s acquire lock err: %v", name, err))
					logHelper.Errorf("Consumer %s acquire lock failed: %v", name, err)
					s.sleepWithContext(5 * time.Second)
					return
				}

				if !acquired {
					// 其他实例正在处理，等待一段时间再试
					s.sleepWithContext(5 * time.Second)
					return
				}

				// 确保锁在函数退出时被释放，即使发生 panic
				defer s.infraRepo.ReleaseLock(commonCtx, lockKey, lockID)

				err = consumer.ConsumeOnce(consumeCtx)

				if err != nil {
					alarm.Lark.Send(fmt.Sprintf("Consumer %s error: %v", name, err))
					logHelper.Errorf("Consumer %s error: %v", name, err)
					// 错误重试间隔
					s.sleepWithContext(5 * time.Second)
				}
			}()
		}
	}
}

func (s *ConsumerServer) sleepWithContext(duration time.Duration) {
	select {
	case <-s.ctx.Done():
		return
	case <-time.After(duration):
		return
	}
}

// ListConsumers 列出所有已注册的消费者
func (s *ConsumerServer) ListConsumers() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	consumers := make([]string, 0, len(s.consumers))
	for name := range s.consumers {
		consumers = append(consumers, name)
	}
	return consumers
}

// GetStatus 获取服务状态
func (s *ConsumerServer) GetStatus() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.started
}
