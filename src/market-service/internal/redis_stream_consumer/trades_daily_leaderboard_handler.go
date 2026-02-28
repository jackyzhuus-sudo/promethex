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
)

// TradesDailyLeaderboardHandler Trades Daily Leaderboard消费者处理器
type TradesDailyLeaderboardHandler struct {
	*baseStreamConsumer // 复用基础消费者功能

	log        log.Logger
	userBiz    *userBiz.UserHandler
	assetBiz   *assetBiz.AssetHandler
	marketBiz  *marketBiz.MarketHandler
	confCustom *conf.Custom
	repo       base.UsercenterInfra
}

func NewTradesDailyLeaderboardHandler(
	log log.Logger,
	userBiz *userBiz.UserHandler,
	assetBiz *assetBiz.AssetHandler,
	marketBiz *marketBiz.MarketHandler,
	confCustom *conf.Custom,
	repo base.UsercenterInfra,
) *TradesDailyLeaderboardHandler {
	handler := &TradesDailyLeaderboardHandler{
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

func (h *TradesDailyLeaderboardHandler) GetConfig() StreamConsumerConfig {
	return StreamConsumerConfig{
		Name:         "trades_daily_leaderboard_consumer",
		GroupName:    "trades-daily-leaderboard-group",
		ConsumerName: "trades-daily-leaderboard-consumer",
		StreamKeys: []string{
			userBiz.UserTradeStreamKey,
		},
		BatchSize:     10,
		BlockDuration: 5 * time.Second,
	}
}

func (h *TradesDailyLeaderboardHandler) ProcessMessage(ctx common.Ctx, streamKey string, msg redis.XMessage) error {
	switch streamKey {
	case userBiz.UserTradeStreamKey:
		return h.processUserTradeMessage(ctx, msg)
	default:
		return fmt.Errorf("unknown stream key: %s", streamKey)
	}
}

func (h *TradesDailyLeaderboardHandler) processUserTradeMessage(ctx common.Ctx, msg redis.XMessage) error {
	userTradeStreamMsg := &userBiz.UserTradeStreamMsg{}
	err := userTradeStreamMsg.ParseUserTradeMessage(msg)
	if err != nil {
		return fmt.Errorf("failed to parse user trade message: %w", err)
	}

	ctx.Log.Infof("Processing trades all time leaderboard: UID=%s, userTradeStreamMsg: %+v",
		userTradeStreamMsg.UID, userTradeStreamMsg)

	dayStr := util.GetDayStartTimeStr(userTradeStreamMsg.Timestamp)
	leaderboardKey := fmt.Sprintf(assetBiz.TradesDailyLeaderboard, userTradeStreamMsg.BaseTokenAddress, dayStr)
	err = h.assetBiz.UpdateLeaderboard(ctx, leaderboardKey, userTradeStreamMsg.UID, 1.0)
	if err != nil {
		ctx.Log.Errorf("failed to update trades all time leaderboard: %w", err)
		return err
	}

	ctx.Log.Infof("Updated trades daily leaderboard: key=%s, uid=%s, score=1.0", leaderboardKey, userTradeStreamMsg.UID)

	return nil
}
