package service

import (
	"market-backend/internal/service/http_api"
	"market-backend/internal/service/sse_api"

	"github.com/google/wire"
)

// ProviderSet is service providers.
var ProviderSet = wire.NewSet(http_api.NewHttpApiService, sse_api.NewSseApiService)
