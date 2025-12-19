package task_verify

import (
	assetBiz "market-service/internal/biz/asset"
	"market-service/internal/biz/base"
	"market-service/internal/pkg/common"
	"time"
)

type DailyFiveMarketTradesTaskVerifier struct {
	BasicTaskVerifier
}

func (verifier *DailyFiveMarketTradesTaskVerifier) Verify(ctx common.Ctx) (bool, error) {
	loc, _ := time.LoadLocation("Asia/Shanghai") // 加载UTC+8时区
	queryTime := time.Now().In(loc)
	if verifier.Timestamp > 0 {
		queryTime = time.Unix(verifier.Timestamp, 0).In(loc)
	}
	startTime := time.Date(queryTime.Year(), queryTime.Month(), queryTime.Day(), 0, 0, 0, 0, loc).Format("2006-01-02 15:04:05.000 +0800")
	endTime := time.Date(queryTime.Year(), queryTime.Month(), queryTime.Day(), 23, 59, 59, 999999999, loc).Format("2006-01-02 15:04:05.000 +0800")
	ctx.Log.Infof("queryTime: %v, startTime: %v, endTime: %v", queryTime, startTime, endTime)

	query := &assetBiz.OrderQuery{
		UID:       verifier.UserEntity.UID,
		StartTime: startTime,
		EndTime:   endTime,
		BaseQuery: base.BaseQuery{
			Limit: 5,
		},
	}

	markets, err := verifier.AssetHandler.GetOrdersDistinctMarkets(ctx, query)
	if err != nil {
		return false, err
	}
	if len(markets) < 5 {
		return false, nil
	}
	return true, nil
}
