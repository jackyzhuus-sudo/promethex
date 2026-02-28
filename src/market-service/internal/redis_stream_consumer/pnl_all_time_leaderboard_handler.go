package redis_stream_consumer

import (
	"fmt"
	assetBiz "market-service/internal/biz/asset"
	marketBiz "market-service/internal/biz/market"
	userBiz "market-service/internal/biz/user"
	"market-service/internal/conf"
	"market-service/internal/data/base"
	"market-service/internal/pkg/common"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-redis/redis/v8"
	"github.com/shopspring/decimal"
)

// PnlAllTimeLeaderboardHandler Pnl All Time Leaderboard消费者（合成版本）
type PnlAllTimeLeaderboardHandler struct {
	*baseStreamConsumer // 复用基础消费者功能

	// 业务逻辑依赖
	userBiz    *userBiz.UserHandler
	assetBiz   *assetBiz.AssetHandler
	marketBiz  *marketBiz.MarketHandler
	confCustom *conf.Custom
}

func NewPnlAllTimeLeaderboardHandler(
	repo base.UsercenterInfra,
	log log.Logger,
	userBiz *userBiz.UserHandler,
	assetBiz *assetBiz.AssetHandler,
	marketBiz *marketBiz.MarketHandler,
	confCustom *conf.Custom,
) *PnlAllTimeLeaderboardHandler {
	consumer := &PnlAllTimeLeaderboardHandler{
		userBiz:    userBiz,
		assetBiz:   assetBiz,
		marketBiz:  marketBiz,
		confCustom: confCustom,
	}

	consumer.baseStreamConsumer = newBaseStreamConsumer(consumer, repo, log)
	return consumer
}

// 实现MessageHandler接口
func (c *PnlAllTimeLeaderboardHandler) GetConfig() StreamConsumerConfig {
	return StreamConsumerConfig{
		Name:         "pnl_all_time_leaderboard_consumer",
		GroupName:    "pnl-all-time-leaderboard-group",
		ConsumerName: "pnl-all-time-leaderboard-consumer",
		StreamKeys: []string{
			userBiz.UserTradeStreamKey,
		},
		BatchSize:     10,
		BlockDuration: 5 * time.Second,
	}
}

func (c *PnlAllTimeLeaderboardHandler) ProcessMessage(ctx common.Ctx, streamKey string, msg redis.XMessage) error {
	switch streamKey {
	case userBiz.UserTradeStreamKey:
		return c.processUserTradeMessage(ctx, msg)
	default:
		return fmt.Errorf("unknown stream key: %s", streamKey)
	}
}

func (c *PnlAllTimeLeaderboardHandler) processUserTradeMessage(ctx common.Ctx, msg redis.XMessage) error {
	userTradeStreamMsg := &userBiz.UserTradeStreamMsg{}
	err := userTradeStreamMsg.ParseUserTradeMessage(msg)
	if err != nil {
		return fmt.Errorf("failed to parse user trade message: %w", err)
	}

	ctx.Log.Infof("Processing pnl all time leaderboard: UID=%s, userTradeStreamMsg: %+v",
		userTradeStreamMsg.UID, userTradeStreamMsg)

	// 只在卖单时更新PnL排行榜（计算已实现盈亏）
	if userTradeStreamMsg.Side != assetBiz.OrderSideSell {
		return nil
	}

	// 计算已实现盈亏
	// 获取该用户的该代币的平均买入价格
	userTokenBalances, err := c.assetBiz.GetUserTokenBalance(ctx, &assetBiz.UserTokenBalanceQuery{
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
	leaderboardKey := fmt.Sprintf(assetBiz.PnlAllTimeLeaderboard, userTradeStreamMsg.BaseTokenType)
	err = c.assetBiz.UpdateLeaderboard(ctx, leaderboardKey, userTradeStreamMsg.UID, realizedPnl.InexactFloat64())
	if err != nil {
		ctx.Log.Errorf("failed to update pnl all time leaderboard: %w", err)
		return err
	}

	ctx.Log.Infof("Updated pnl all time leaderboard: key=%s, uid=%s, score=%s", leaderboardKey, userTradeStreamMsg.UID, realizedPnl.String())

	return nil
}
