//go:build wireinject
// +build wireinject

// The build tag makes sure the stub is not built in the final build.

package main

import (
	"block-listener/internal/biz"
	"block-listener/internal/conf"
	"block-listener/internal/contract"
	"block-listener/internal/data"
	"block-listener/internal/server"

	"github.com/go-kratos/kratos/v2"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/wire"
)

// wireApp 通过依赖注入组装应用
func wireApp(*conf.Bootstrap, *conf.Custom, log.Logger) (*kratos.App, func(), error) {
	panic(wire.Build(contract.ProviderSet, server.ProviderSet, data.ProviderSet, biz.ProviderSet, newApp))
}
