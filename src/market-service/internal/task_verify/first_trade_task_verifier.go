package task_verify

import (
	assetBiz "market-service/internal/biz/asset"
	"market-service/internal/biz/base"
	"market-service/internal/pkg/common"
	"time"
)

type FirstTradeTaskVerifier struct {
	BasicTaskVerifier
}

func (v *FirstTradeTaskVerifier) Verify(ctx common.Ctx) (bool, error) {

	orders, err := v.AssetHandler.GetOrders(ctx, &assetBiz.OrderQuery{
		UID: v.UserEntity.UID,
		BaseQuery: base.BaseQuery{
			Limit:  1,
			Offset: 0,
			Order:  "created_at asc",
		},
	})
	if err != nil {
		return false, err
	}
	if len(orders) == 0 {
		return false, nil
	}

	order := orders[0]

	// 检查第一笔订单是否在用户注册后7天内
	userCreatedAt := v.UserEntity.CreatedAt
	validUntil := userCreatedAt.Add(7 * 24 * time.Hour) // 注册时间后7天

	// 如果订单创建时间超过了7天期限，则无效
	if order.CreatedAt.After(validUntil) {
		return false, nil
	}

	return true, nil
}
