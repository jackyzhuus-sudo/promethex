package redis_stream_consumer

import (
	"fmt"
	"time"

	assetBiz "market-service/internal/biz/asset"
	marketBiz "market-service/internal/biz/market"
	"market-service/internal/biz/task"
	taskBiz "market-service/internal/biz/task"
	userBiz "market-service/internal/biz/user"
	"market-service/internal/conf"
	"market-service/internal/data/base"
	"market-service/internal/pkg/common"
	"market-service/internal/task_verify"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-redis/redis/v8"
)

// VerifyTaskHandler 任务验证消费者处理器
type VerifyTaskHandler struct {
	*baseStreamConsumer
	log        log.Logger
	userBiz    *userBiz.UserHandler
	assetBiz   *assetBiz.AssetHandler
	marketBiz  *marketBiz.MarketHandler
	taskBiz    *taskBiz.TaskHandler
	confCustom *conf.Custom
}

func NewVerifyTaskHandler(
	repo base.UsercenterInfra,
	log log.Logger,
	taskBiz *taskBiz.TaskHandler,
	userBiz *userBiz.UserHandler,
	assetBiz *assetBiz.AssetHandler,
	marketBiz *marketBiz.MarketHandler,
	confCustom *conf.Custom,
) *VerifyTaskHandler {
	handler := &VerifyTaskHandler{
		log:        log,
		userBiz:    userBiz,
		assetBiz:   assetBiz,
		marketBiz:  marketBiz,
		taskBiz:    taskBiz,
		confCustom: confCustom,
	}
	handler.baseStreamConsumer = newBaseStreamConsumer(handler, repo, log)
	return handler
}

func (h *VerifyTaskHandler) GetConfig() StreamConsumerConfig {
	return StreamConsumerConfig{
		Name:         "verify_task_consumer",
		GroupName:    "verify-task-group",
		ConsumerName: "verify-task-consumer",
		StreamKeys: []string{
			userBiz.UserTradeStreamKey,
		},
		BatchSize:     10,
		BlockDuration: 5 * time.Second,
	}
}

func (h *VerifyTaskHandler) ProcessMessage(ctx common.Ctx, streamKey string, msg redis.XMessage) error {
	switch streamKey {
	case userBiz.UserTradeStreamKey:
		return h.processUserTradeMessage(ctx, msg)
	default:
		return fmt.Errorf("unknown stream key: %s", streamKey)
	}
}

// 从原VerifyTaskConsumer中提取的业务逻辑方法，保持完全不变

func (h *VerifyTaskHandler) processUserTradeMessage(ctx common.Ctx, msg redis.XMessage) error {
	userTradeStreamMsg := &userBiz.UserTradeStreamMsg{}
	err := userTradeStreamMsg.ParseUserTradeMessage(msg)
	if err != nil {
		return fmt.Errorf("failed to parse user trade message: %w", err)
	}

	for _, taskKey := range task.TxTradeTaskKeyList {

		verifier, err := task_verify.GetTaskVerifier(taskKey)
		if err != nil {
			ctx.Log.Errorf("failed to get task verifier: %w", err)
			return fmt.Errorf("failed to get task verifier: %w", err)
		}

		err = verifier.Init(ctx, h.taskBiz, h.userBiz, h.marketBiz, h.assetBiz,
			userTradeStreamMsg.UID, taskKey, userTradeStreamMsg.Timestamp)
		if err != nil {
			ctx.Log.Errorf("failed to init task verifier: %w", err)
			return fmt.Errorf("failed to init task verifier: %w", err)
		}

		isRecord, err := verifier.PreCheck(ctx)
		if err != nil {
			ctx.Log.Errorf("failed to pre check task verifier: %w", err)
			return fmt.Errorf("failed to pre check task verifier: %w", err)
		}

		if isRecord {
			continue
		}

		isDone, err := verifier.Verify(ctx)
		if err != nil {
			ctx.Log.Errorf("failed to verify task verifier: %w", err)
			return fmt.Errorf("failed to verify task verifier: %w", err)
		}

		if !isDone {
			continue
		}

		err = verifier.Record(ctx)
		if err != nil {
			ctx.Log.Errorf("failed to record task verifier: %w", err)
			return fmt.Errorf("failed to record task verifier: %w", err)
		}
	}

	return nil
}
