package redis_stream_consumer

import (
	"fmt"
	assetBiz "market-service/internal/biz/asset"
	marketBiz "market-service/internal/biz/market"
	userBiz "market-service/internal/biz/user"
	"market-service/internal/conf"
	"market-service/internal/data/base"
	"market-service/internal/pkg/common"
	"market-service/internal/pkg/util"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-redis/redis/v8"
	"github.com/shopspring/decimal"
)

// VolumeWeeklyLeaderboardHandler Volume Weekly Leaderboard消费者处理器
type VolumeWeeklyLeaderboardHandler struct {
	*baseStreamConsumer // 复用基础消费者功能

	log        log.Logger
	userBiz    *userBiz.UserHandler
	assetBiz   *assetBiz.AssetHandler
	marketBiz  *marketBiz.MarketHandler
	confCustom *conf.Custom
	repo       base.UsercenterInfra
}

func NewVolumeWeeklyLeaderboardHandler(
	log log.Logger,
	userBiz *userBiz.UserHandler,
	assetBiz *assetBiz.AssetHandler,
	marketBiz *marketBiz.MarketHandler,
	confCustom *conf.Custom,
	repo base.UsercenterInfra,
) *VolumeWeeklyLeaderboardHandler {
	handler := &VolumeWeeklyLeaderboardHandler{
		log:        log,
		userBiz:    userBiz,
		assetBiz:   assetBiz,
		marketBiz:  marketBiz,
		confCustom: confCustom,
		repo:       repo,
	}
	handler.baseStreamConsumer = newBaseStreamConsumer(handler, repo, log)
	return handler
}

func (h *VolumeWeeklyLeaderboardHandler) GetConfig() StreamConsumerConfig {
	return StreamConsumerConfig{
		Name:         "volume_weekly_leaderboard_consumer",
		GroupName:    "volume-weekly-leaderboard-group",
		ConsumerName: "volume-weekly-leaderboard-consumer",
		StreamKeys: []string{
			userBiz.UserTradeStreamKey,
		},
		BatchSize:     10,
		BlockDuration: 5 * time.Second,
	}
}

func (h *VolumeWeeklyLeaderboardHandler) ProcessMessage(ctx common.Ctx, streamKey string, msg redis.XMessage) error {
	switch streamKey {
	case userBiz.UserTradeStreamKey:
		return h.processUserTradeMessage(ctx, msg)
	default:
		return fmt.Errorf("unknown stream key: %s", streamKey)
	}
}

func (h *VolumeWeeklyLeaderboardHandler) processUserTradeMessage(ctx common.Ctx, msg redis.XMessage) error {
	userTradeStreamMsg := &userBiz.UserTradeStreamMsg{}
	err := userTradeStreamMsg.ParseUserTradeMessage(msg)
	if err != nil {
		return fmt.Errorf("failed to parse user trade message: %w", err)
	}

	ctx.Log.Infof("Processing volume all time leaderboard: UID=%s, userTradeStreamMsg: %+v",
		userTradeStreamMsg.UID, userTradeStreamMsg)

	// 解析交易金额
	amountIn, err := decimal.NewFromString(userTradeStreamMsg.AmountIn)
	if err != nil {
		return fmt.Errorf("failed to parse amount_in: %w", err)
	}
	amountOut, err := decimal.NewFromString(userTradeStreamMsg.AmountOut)
	if err != nil {
		return fmt.Errorf("failed to parse amount_out: %w", err)
	}

	tradeVolume := amountIn
	if userTradeStreamMsg.Side == assetBiz.OrderSideSell {
		tradeVolume = amountOut
	}

	weekStr := util.GetWeekStartTimeStr(userTradeStreamMsg.Timestamp)
	leaderboardKey := fmt.Sprintf(assetBiz.VolumeWeeklyLeaderboard, userTradeStreamMsg.BaseTokenType, weekStr)
	err = h.assetBiz.UpdateLeaderboard(ctx, leaderboardKey, userTradeStreamMsg.UID, tradeVolume.InexactFloat64())
	if err != nil {
		ctx.Log.Errorf("failed to update volume weekly leaderboard: %w", err)
		return err
	}
	ctx.Log.Infof("Updated volume weekly leaderboard: key=%s, uid=%s, score=%s", leaderboardKey, userTradeStreamMsg.UID, tradeVolume.String())
	return nil
}
