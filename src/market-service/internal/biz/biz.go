package biz

import (
	"market-service/internal/biz/asset"
	"market-service/internal/biz/community"
	"market-service/internal/biz/market"
	"market-service/internal/biz/task"
	"market-service/internal/biz/user"

	"github.com/google/wire"
)

// ProviderSet is biz providers.
var ProviderSet = wire.NewSet(user.NewUserHandler, community.NewCommunityHandler, market.NewMarketHandler, asset.NewAssetHandler, task.NewTaskHandler)
