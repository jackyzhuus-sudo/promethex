package service

import (
	marketcenter "market-proto/proto/market-service/marketcenter/v1"
	usercenter "market-proto/proto/market-service/usercenter/v1"
	assetBiz "market-service/internal/biz/asset"
	communityBiz "market-service/internal/biz/community"
	marketBiz "market-service/internal/biz/market"
	taskBiz "market-service/internal/biz/task"
	userBiz "market-service/internal/biz/user"
	"market-service/internal/conf"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/wire"
)

// ProviderSet is service providers.
var ProviderSet = wire.NewSet(NewBayesService)

type BayesService struct {
	usercenter.UnimplementedUsercenterServer
	marketcenter.UnimplementedMarketcenterServer
	confCustom *conf.Custom

	userHandler      *userBiz.UserHandler
	communityHandler *communityBiz.CommunityHandler
	taskHandler      *taskBiz.TaskHandler

	marketHandler *marketBiz.MarketHandler
	assetHandler  *assetBiz.AssetHandler
	log           log.Logger
}

func NewBayesService(userHandler *userBiz.UserHandler, marketHandler *marketBiz.MarketHandler, taskHandler *taskBiz.TaskHandler, assetHandler *assetBiz.AssetHandler, communityHandler *communityBiz.CommunityHandler, confCustom *conf.Custom, logger log.Logger) *BayesService {
	return &BayesService{userHandler: userHandler, marketHandler: marketHandler, taskHandler: taskHandler, assetHandler: assetHandler, communityHandler: communityHandler, confCustom: confCustom, log: logger}
}
