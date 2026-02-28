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

// PnlWeeklyLeaderboardHandler Pnl Weekly Leaderboard消费者处理器
type PnlWeeklyLeaderboardHandler struct {
	*baseStreamConsumer // 复用基础消费者功能

	log        log.Logger
	userBiz    *userBiz.UserHandler
	assetBiz   *assetBiz.AssetHandler
	marketBiz  *marketBiz.MarketHandler
	confCustom *conf.Custom
	repo       base.UsercenterInfra
}

func NewPnlWeeklyLeaderboardHandler(
	log log.Logger,
	userBiz *userBiz.UserHandler,
	assetBiz *assetBiz.AssetHandler,
	marketBiz *marketBiz.MarketHandler,
	confCustom *conf.Custom,
	repo base.UsercenterInfra,
) *PnlWeeklyLeaderboardHandler {
	handler := &PnlWeeklyLeaderboardHandler{
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

func (h *PnlWeeklyLeaderboardHandler) GetConfig() StreamConsumerConfig {
	return StreamConsumerConfig{
		Name:         "pnl_weekly_leaderboard_consumer",
		GroupName:    "pnl-weekly-leaderboard-group",
		ConsumerName: "pnl-weekly-leaderboard-consumer",
		StreamKeys: []string{
			userBiz.UserTradeStreamKey,
		},
		BatchSize:     10,
		BlockDuration: 5 * time.Second,
	}
}

func (h *PnlWeeklyLeaderboardHandler) ProcessMessage(ctx common.Ctx, streamKey string, msg redis.XMessage) error {
	switch streamKey {
	case userBiz.UserTradeStreamKey:
		return h.processUserTradeMessage(ctx, msg)
	default:
		return fmt.Errorf("unknown stream key: %s", streamKey)
	}
}

func (h *PnlWeeklyLeaderboardHandler) processUserTradeMessage(ctx common.Ctx, msg redis.XMessage) error {
	userTradeStreamMsg := &userBiz.UserTradeStreamMsg{}
	err := userTradeStreamMsg.ParseUserTradeMessage(msg)
	if err != nil {
		return fmt.Errorf("failed to parse user trade message: %w", err)
	}

	ctx.Log.Infof("Processing pnl weekly leaderboard: UID=%s, userTradeStreamMsg: %+v",
		userTradeStreamMsg.UID, userTradeStreamMsg)

	// 只在卖时更新PnL排行榜（计算已实现盈亏）
	if userTradeStreamMsg.Side != assetBiz.OrderSideSell {
		return nil
	}

	// 计算已实现盈亏
	// 获取该用户的该代币的平均买入价格
	userTokenBalances, err := h.assetBiz.GetUserTokenBalance(ctx, &assetBiz.UserTokenBalanceQuery{
		UID:          userTradeStreamMsg.UID,
		TokenAddress: userTradeStreamMsg.OptionAddress,
		Type:         assetBiz.TypeUserTokenBalanceOption,
	})
	if err != nil {
		return fmt.Errorf("failed to get user token balance: %w", err)
	}

	if len(userTokenBalances) == 0 {
		ctx.Log.Warnf("no user token balance found for UID=%s, token=%s", userTradeStreamMsg.UID, userTradeStreamMsg.OptionAddress)
		return nil
	}

	userTokenBalance := userTokenBalances[0]

	// 计算已实现盈亏 = (卖出价格 - 平均买入价格) * 卖出数量
	amountIn, err := decimal.NewFromString(userTradeStreamMsg.AmountIn)
	if err != nil {
		return fmt.Errorf("failed to parse amount_in: %w", err)
	}

	amountOut, err := decimal.NewFromString(userTradeStreamMsg.AmountOut)
	if err != nil {
		return fmt.Errorf("failed to parse amount_out: %w", err)
	}

	sellPrice := amountOut.Div(amountIn).Mul(decimal.New(1, int32(userTokenBalance.Decimal)))

	realizedPnl := sellPrice.Sub(userTokenBalance.AvgBuyPrice).Mul(amountIn).Div(decimal.New(1, int32(userTokenBalance.Decimal)))

	weekStr := util.GetWeekStartTimeStr(userTradeStreamMsg.Timestamp)
	leaderboardKey := fmt.Sprintf(assetBiz.PnlWeeklyLeaderboard, userTradeStreamMsg.BaseTokenAddress, weekStr)
	err = h.assetBiz.UpdateLeaderboard(ctx, leaderboardKey, userTradeStreamMsg.UID, realizedPnl.InexactFloat64())
	if err != nil {
		ctx.Log.Errorf("failed to update pnl weekly leaderboard: %w", err)
		return err
	}

	ctx.Log.Infof("Updated pnl weekly leaderboard: key=%s, uid=%s, score=%s", leaderboardKey, userTradeStreamMsg.UID, realizedPnl.String())

	return nil
}
