package data

import (
	"github.com/google/wire"
)

// ProviderSet 是data providers
var ProviderSet = wire.NewSet(NewRedisClient, NewRpcClient, NewArbClient, NewDbClient)
