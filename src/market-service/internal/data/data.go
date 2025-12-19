package data

import (
	assetData "market-service/internal/data/asset"
	baseData "market-service/internal/data/base"
	communityData "market-service/internal/data/community"
	marketData "market-service/internal/data/market"
	taskData "market-service/internal/data/task"
	userData "market-service/internal/data/user"

	"github.com/google/wire"
)

// ProviderSet is data providers.
var ProviderSet = wire.NewSet(
	baseData.NewData, baseData.NewUsercenterInfra, baseData.NewMarketcenterInfra,
	userData.NewUserRepo, communityData.NewCommunityRepo, marketData.NewMarketRepo, assetData.NewAssetRepo, taskData.NewTaskRepo,
)
