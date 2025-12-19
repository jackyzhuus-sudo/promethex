package service

import (
	"market-backend/internal/service/bayes_http"
	"market-backend/internal/service/bayes_sse"

	"github.com/google/wire"
)

// ProviderSet is service providers.
var ProviderSet = wire.NewSet(bayes_http.NewBayesHttpService, bayes_sse.NewBayesSseService)
