package data

import (
	"block-listener/internal/conf"
	"block-listener/pkg/common"
	"context"
	"encoding/json"
	"time"

	marketcenterPb "market-proto/proto/market-service/marketcenter/v1"
	usercenterPb "market-proto/proto/market-service/usercenter/v1"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/middleware"
	"github.com/go-kratos/kratos/v2/middleware/recovery"
	"github.com/go-kratos/kratos/v2/transport"
	"github.com/go-kratos/kratos/v2/transport/grpc"
	"github.com/google/uuid"
)

type RpcClient struct {
	MarketcenterClient *MarketcenterClient
	UsercenterClient   *UsercenterClient
}

type MarketcenterClient struct {
	marketcenterPb.MarketcenterClient
}

type UsercenterClient struct {
	usercenterPb.UsercenterClient
}

func NewRpcClient(conf *conf.Bootstrap, logger log.Logger) (*RpcClient, error) {
	marketcenterClient, err := newMarketcenterClient(conf.Data.MarketcenterRpc.Timeout.AsDuration(), conf.Data.MarketcenterRpc.Addr, logger)
	if err != nil {
		return nil, err
	}

	usercenterClient, err := newUsercenterClient(conf.Data.UsercenterRpc.Timeout.AsDuration(), conf.Data.UsercenterRpc.Addr, logger)
	if err != nil {
		return nil, err
	}

	return &RpcClient{MarketcenterClient: marketcenterClient, UsercenterClient: usercenterClient}, nil
}

func newMarketcenterClient(timeout time.Duration, addr string, logger log.Logger) (*MarketcenterClient, error) {

	conn, err := grpc.DialInsecure(
		context.Background(),
		grpc.WithTimeout(timeout),
		grpc.WithEndpoint(addr),
		grpc.WithMiddleware(
			recovery.Recovery(),
			TraceClient(),
			LogClient(logger),
		),
	)
	if err != nil {
		return nil, err
	}

	return &MarketcenterClient{
		MarketcenterClient: marketcenterPb.NewMarketcenterClient(conn),
	}, nil
}

func newUsercenterClient(timeout time.Duration, addr string, logger log.Logger) (*UsercenterClient, error) {
	conn, err := grpc.DialInsecure(
		context.Background(),
		grpc.WithTimeout(timeout),
		grpc.WithEndpoint(addr),
		grpc.WithMiddleware(
			recovery.Recovery(),
			TraceClient(),
			LogClient(logger),
		),
	)
	if err != nil {
		return nil, err
	}

	return &UsercenterClient{
		UsercenterClient: usercenterPb.NewUsercenterClient(conn),
	}, nil
}

func TraceClient() middleware.Middleware {
	return func(handler middleware.Handler) middleware.Handler {
		return func(ctx context.Context, req interface{}) (reply interface{}, err error) {
			if tr, ok := transport.FromClientContext(ctx); ok {
				// 从context获取trace-id
				if traceID := ctx.Value("traceId"); traceID != nil {
					tr.RequestHeader().Set("traceId", traceID.(string))
				} else {
					tr.RequestHeader().Set("traceId", uuid.New().String())
				}
			}
			return handler(ctx, req)
		}
	}
}

func LogClient(logger log.Logger) middleware.Middleware {
	return func(handler middleware.Handler) middleware.Handler {
		return func(ctx context.Context, req interface{}) (reply interface{}, err error) {
			// 获取服务名称
			var serviceName string
			if tr, ok := transport.FromClientContext(ctx); ok {
				serviceName = tr.Operation()
			}

			c := common.NewBaseCtx(ctx, logger)
			// 打印请求信息
			reqBytes, _ := json.Marshal(req)
			reqStr := string(reqBytes)
			if len(reqStr) > 3000 {
				reqStr = reqStr[:3000] + "......"
			}
			c.Log.Infof("GRPC CALL [%s] request: [%s]", serviceName, reqStr)

			reply, err = handler(ctx, req)

			if err != nil {
				c.Log.Errorf("GRPC CALL [%s] response error: [%+v]", serviceName, err)
			} else {
				rspBytes, _ := json.Marshal(reply)
				rspStr := string(rspBytes)
				if len(rspStr) > 3000 {
					rspStr = rspStr[:3000] + "......"
				}
				c.Log.Infof("GRPC CALL [%s] response: [%s]", serviceName, rspStr)
			}

			return reply, err
		}
	}
}
