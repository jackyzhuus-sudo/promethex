package http_api

import (
	"market-backend/internal/conf"
	"market-backend/internal/data"

	apipb "market-proto/proto/market-backend/v1"

	"github.com/go-kratos/kratos/v2/log"
)

type HttpApiService struct {
	apipb.HttpApiServer
	data   *data.Data
	logger log.Logger
	custom *conf.Custom
}

func NewHttpApiService(data *data.Data, cfgData *conf.Data, custom *conf.Custom, logger log.Logger) *HttpApiService {

	// 创建SSE处理器

	return &HttpApiService{
		data:   data,
		logger: logger,
		custom: custom,
	}
}
