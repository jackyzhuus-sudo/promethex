package bayes_http

import (
	"market-backend/internal/conf"
	"market-backend/internal/data"

	bayespb "market-proto/proto/market-backend/v1"

	"github.com/go-kratos/kratos/v2/log"
)

type BayesHttpService struct {
	bayespb.BayesHttpServer
	data   *data.Data
	logger log.Logger
	custom *conf.Custom
}

func NewBayesHttpService(data *data.Data, cfgData *conf.Data, custom *conf.Custom, logger log.Logger) *BayesHttpService {

	// 创建SSE处理器

	return &BayesHttpService{
		data:   data,
		logger: logger,
		custom: custom,
	}
}
