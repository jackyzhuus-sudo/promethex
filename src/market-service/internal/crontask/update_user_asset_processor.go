package crontask

import (
	"context"
	"fmt"
	assetBiz "market-service/internal/biz/asset"
	"market-service/internal/biz/base"
	marketBiz "market-service/internal/biz/market"
	userBiz "market-service/internal/biz/user"
	"market-service/internal/conf"
	"market-service/internal/pkg/alarm"
	"market-service/internal/pkg/common"
	"time"

	"github.com/go-kratos/kratos/v2/log"
)

type UpdateUserAssetProcessor struct {
	log           log.Logger
	userHandler   *userBiz.UserHandler
	marketHandler *marketBiz.MarketHandler
	assetHandler  *assetBiz.AssetHandler
	confCustom    *conf.Custom
}

// NewUpdateUserAssetProcessor
func NewUpdateUserAssetProcessor(
	logger log.Logger,
	userHandler *userBiz.UserHandler,
	marketHandler *marketBiz.MarketHandler,
	assetHandler *assetBiz.AssetHandler,
	confCustom *conf.Custom,
) *UpdateUserAssetProcessor {
	return &UpdateUserAssetProcessor{
		log:           logger,
		userHandler:   userHandler,
		marketHandler: marketHandler,
		assetHandler:  assetHandler,
		confCustom:    confCustom,
	}
}

// Name 返回任务名称
func (p *UpdateUserAssetProcessor) Name() string {
	return "update_user_asset_processor"
}

func (p *UpdateUserAssetProcessor) Timeout() time.Duration {
	return 1 * time.Hour
}

// Run 执行任务
func (p *UpdateUserAssetProcessor) Run(ctx context.Context) error {
	c := common.NewBaseCtx(ctx, p.log)
	c.Log.Infof("update_user_asset_processor processing...")

	pageSize := 100
	page := 1
	for {
		users, _, err := p.userHandler.GetUsersWithTotal(c, &userBiz.UserQuery{
			BaseQuery: base.BaseQuery{
				Order:  "id asc",
				Limit:  int32(pageSize),
				Offset: int32(pageSize * (page - 1)),
			},
		})
		if err != nil {
			c.Log.Errorf("GetUsersWithTotal failed, err: %v", err)
			alarm.Lark.Send(fmt.Sprintf("update_user_asset_processor GetUsersWithTotal failed, err: %v", err))
			return err
		}

		// 处理这一批用户的资产
		if err := p.processUserAssets(c, users); err != nil {
			c.Log.Errorf("processUserAssets failed, err: %v", err)
			alarm.Lark.Send(fmt.Sprintf("update_user_asset_processor processUserAssets failed, err: %v", err))
			return err
		}

		// 如果获取到的用户数小于页大小，说明是最后一页了
		if len(users) < pageSize {
			break
		}

		page++
	}

	c.Log.Infof("update_user_asset_processor processing done")
	return nil
}

func (p *UpdateUserAssetProcessor) processUserAssets(c common.Ctx, users []*userBiz.UserEntity) error {
	if len(users) == 0 {
		return nil
	}

	userPointsAssetValues := make([]*assetBiz.UserAssetValueEntity, 0, len(users))
	userUsdcAssetValues := make([]*assetBiz.UserAssetValueEntity, 0, len(users))
	for _, user := range users {
		// 计算points资产
		pointsAssetValue, err := p.assetHandler.CalculateUserAssetValue(c, user.UID, p.confCustom.AssetTokens.Points.Address, assetBiz.BaseTokenTypePoints)
		if err != nil {
			c.Log.Errorf("CalculateUserAssetValue for points failed, uid: %s, err: %v", user.UID, err)
			return err
		}
		userPointsAssetValues = append(userPointsAssetValues, pointsAssetValue)

		// 计算usdc资产
		usdcAssetValue, err := p.assetHandler.CalculateUserAssetValue(c, user.UID, p.confCustom.AssetTokens.Usdc.Address, assetBiz.BaseTokenTypeUsdc)
		if err != nil {
			c.Log.Errorf("CalculateUserAssetValue for usdc failed, uid: %s, err: %v", user.UID, err)
			return err
		}
		userUsdcAssetValues = append(userUsdcAssetValues, usdcAssetValue)
	}

	// 批量创建/更新用户资产记录
	if len(userPointsAssetValues) > 0 {
		err := p.assetHandler.BatchCreateUserAssetValue(c, userPointsAssetValues)
		if err != nil {
			c.Log.Errorf("BatchCreateUserAssetValue failed, err: %v", err)
			return err
		}
	}

	if len(userUsdcAssetValues) > 0 {
		err := p.assetHandler.BatchCreateUserAssetValue(c, userUsdcAssetValues)
		if err != nil {
			c.Log.Errorf("BatchCreateUserAssetValue failed, err: %v", err)
			return err
		}
	}

	return nil
}
