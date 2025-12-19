//go:build wireinject
// +build wireinject

// The build tag makes sure the stub is not built in the final build.

package main

import (
	"market-service/internal/biz"
	"market-service/internal/conf"
	"market-service/internal/crontask"
	"market-service/internal/data"
	"market-service/internal/redis_stream_consumer"
	"market-service/internal/server"
	"market-service/internal/service"

	"github.com/go-kratos/kratos/v2"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/wire"
)

// wireApp init kratos application.
func wireApp(*conf.Server, *conf.Data, *conf.Custom, log.Logger) (*kratos.App, func(), error) {
	panic(wire.Build(server.ProviderSet, data.ProviderSet, biz.ProviderSet, crontask.ProviderSet, service.ProviderSet, redis_stream_consumer.ProviderSet, newApp))
}
