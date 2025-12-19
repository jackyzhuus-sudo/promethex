//go:build wireinject
// +build wireinject

// The build tag makes sure the stub is not built in the final build.

package main

import (
	"market-backend/internal/alarm"
	"market-backend/internal/conf"
	"market-backend/internal/data"
	"market-backend/internal/server"
	"market-backend/internal/service"
	"market-backend/internal/sse"

	"github.com/go-kratos/kratos/v2"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/wire"
)

// wireApp init kratos application.
func wireApp(*conf.Server, *conf.Data, *conf.Custom, log.Logger) (*kratos.App, func(), error) {
	panic(wire.Build(server.ProviderSet, data.ProviderSet, service.ProviderSet, alarm.ProviderSet, sse.ProviderSet, newApp))
}
