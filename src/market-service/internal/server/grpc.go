package server

import (
	marketcenter "market-proto/proto/market-service/marketcenter/v1"
	usercenter "market-proto/proto/market-service/usercenter/v1"
	"market-service/internal/conf"
	"market-service/internal/service"
	"market-service/middleware"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/middleware/recovery"
	"github.com/go-kratos/kratos/v2/transport/grpc"
)

// NewGRPCServer new a gRPC server.
func NewGRPCServer(c *conf.Server, bayesService *service.BayesService, logger log.Logger) *grpc.Server {
	var opts = []grpc.ServerOption{
		grpc.Middleware(
			recovery.Recovery(),
			middleware.ErrCause(),
			middleware.Logging(logger),
		),
	}
	if c.Grpc.Network != "" {
		opts = append(opts, grpc.Network(c.Grpc.Network))
	}
	if c.Grpc.Addr != "" {
		opts = append(opts, grpc.Address(c.Grpc.Addr))
	}
	if c.Grpc.Timeout != nil {
		opts = append(opts, grpc.Timeout(c.Grpc.Timeout.AsDuration()))
	}

	srv := grpc.NewServer(opts...)
	usercenter.RegisterUsercenterServer(srv, bayesService)
	marketcenter.RegisterMarketcenterServer(srv, bayesService)
	return srv
}
