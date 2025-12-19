package redis_stream_consumer

import (
	"context"
	"fmt"
	"time"

	"market-service/internal/data/base"
	"market-service/internal/pkg/common"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-redis/redis/v8"
	"github.com/google/wire"
)

// ProviderSet is redis stream consumer providers.
var ProviderSet = wire.NewSet(
	NewVerifyTaskHandler,
	NewMintPointsHandler,
	NewPnlAllTimeLeaderboardHandler,
	NewPnlDailyLeaderboardHandler,
	NewPnlMonthlyLeaderboardHandler,
	NewPnlWeeklyLeaderboardHandler,
	NewTradesAllTimeLeaderboardHandler,
	NewTradesDailyLeaderboardHandler,
	NewTradesMonthlyLeaderboardHandler,
	NewTradesWeeklyLeaderboardHandler,
	NewVolumeAllTimeLeaderboardHandler,
	NewVolumeDailyLeaderboardHandler,
	NewVolumeMonthlyLeaderboardHandler,
	NewVolumeWeeklyLeaderboardHandler,
)

// StreamConsumerConfig 消费者配置
type StreamConsumerConfig struct {
	Name          string
	GroupName     string
	ConsumerName  string
	StreamKeys    []string
	BatchSize     int64
	BlockDuration time.Duration
}

// MessageHandler 消息处理接口
type MessageHandler interface {
	ProcessMessage(ctx common.Ctx, streamKey string, msg redis.XMessage) error
	GetConfig() StreamConsumerConfig
}

// baseStreamConsumer 基础消费者实现
type baseStreamConsumer struct {
	handler MessageHandler
	repo    base.UsercenterInfra
	log     log.Logger
}

func newBaseStreamConsumer(handler MessageHandler, repo base.UsercenterInfra, log log.Logger) *baseStreamConsumer {
	return &baseStreamConsumer{
		handler: handler,
		repo:    repo,
		log:     log,
	}
}

func (c *baseStreamConsumer) Name() string {
	return c.handler.GetConfig().Name
}

func (c *baseStreamConsumer) Initialize(ctx context.Context) error {
	config := c.handler.GetConfig()
	commonCtx := common.NewBaseCtx(ctx, c.log)

	for _, streamKey := range config.StreamKeys {
		err := c.repo.StreamCreateGroup(commonCtx, streamKey, config.GroupName, "$")
		if err != nil {
			return fmt.Errorf("failed to create consumer group for stream %s: %w", streamKey, err)
		}
	}
	return nil
}

func (c *baseStreamConsumer) ConsumeOnce(ctx context.Context) error {
	config := c.handler.GetConfig()
	commonCtx := common.NewBaseCtx(ctx, c.log)

	// 1. 先尝试读取pending消息（重试失败的消息）
	pendingStreams, err := c.repo.StreamReadPendingMessagesMultiple(
		commonCtx,
		config.StreamKeys,
		config.GroupName,
		config.ConsumerName,
		config.BatchSize,
		config.BlockDuration,
	)
	if err != nil {
		return fmt.Errorf("failed to read pending messages: %w", err)
	}

	if c.hasMessages(pendingStreams) {
		commonCtx.Log.Infof("Found pending messages from multiple streams, processing retries first")
		return c.processStreamMessages(ctx, pendingStreams)
	}

	// 2. 读取新消息
	newStreams, err := c.repo.StreamReadGroupBlockingMultiple(
		commonCtx,
		config.StreamKeys,
		config.GroupName,
		config.ConsumerName,
		config.BatchSize,
		config.BlockDuration,
	)
	if err != nil {
		return fmt.Errorf("failed to read new messages: %w", err)
	}

	if len(newStreams) == 0 {
		return nil
	}

	commonCtx.Log.Infof("Read %d new messages from multiple streams", len(newStreams))
	return c.processStreamMessages(ctx, newStreams)
}

func (c *baseStreamConsumer) processStreamMessages(ctx context.Context, streams []redis.XStream) error {
	for _, stream := range streams {
		commonCtx := common.NewBaseCtx(ctx, c.log)
		streamKey := stream.Stream
		commonCtx.Log.Infof("Start processStreamMessages stream %s, %d messages", streamKey, len(stream.Messages))

		err := c.processMessages(ctx, streamKey, stream.Messages)
		if err != nil {
			commonCtx.Log.Errorf("processStreamMessages %s err: %v", streamKey, err)
			return err
		}
		commonCtx.Log.Infof("End processStreamMessages stream %s, %d messages", streamKey, len(stream.Messages))
	}
	return nil
}

// processMessages 处理消息列表
func (c *baseStreamConsumer) processMessages(ctx context.Context, streamKey string, messages []redis.XMessage) error {
	// 顺序处理每条消息，处理完一条ACK一条
	for i, msg := range messages {
		spanId := msg.ID
		ctx = context.WithValue(ctx, "spanId", spanId)
		commonCtx := common.NewBaseCtx(ctx, c.log)
		commonCtx.Log.Infof("Processing stream %s message %d/%d, ID: %s", streamKey, i+1, len(messages), msg.ID)

		// 委托给具体的处理器
		err := c.handler.ProcessMessage(commonCtx, streamKey, msg)
		if err != nil {
			return fmt.Errorf("process message stream %s id %s err: %v", streamKey, msg.ID, err)
		}

		// 消息处理成功，立即ACK
		config := c.handler.GetConfig()
		_, err = c.repo.StreamAckMessage(commonCtx, streamKey, config.GroupName, msg.ID)
		if err != nil {
			return fmt.Errorf("ack message %s err: %v", msg.ID, err)
		}
	}

	return nil
}

func (c *baseStreamConsumer) hasMessages(streams []redis.XStream) bool {
	for _, stream := range streams {
		if len(stream.Messages) > 0 {
			return true
		}
	}
	return false
}
