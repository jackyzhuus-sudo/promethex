package data

import (
	"context"
	"encoding/json"
	"time"

	"market-backend/internal/conf"
	"market-backend/internal/pkg/util"
	marketcenterpb "market-proto/proto/market-service/marketcenter/v1"
	usercenterpb "market-proto/proto/market-service/usercenter/v1"

	"github.com/go-kratos/kratos/v2/errors"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/middleware"
	"github.com/go-kratos/kratos/v2/middleware/recovery"
	"github.com/go-kratos/kratos/v2/transport"
	"github.com/go-kratos/kratos/v2/transport/grpc"
	"github.com/google/uuid"
	gstatus "google.golang.org/grpc/status"
)

// ErrorMapping 定义错误消息到错误码的映射
var ErrorMapping = map[string]struct {
	Code   int
	Reason string
}{
	"repeat like or unlike": {42009, "REPEAT_LIKE_OR_UNLIKE"},
	// 可以添加更多错误映射
}

type RpcClient struct {
	MarketcenterClient *MarketcenterClient
	UsercenterClient   *UsercenterClient
}

type MarketcenterClient struct {
	marketcenterpb.MarketcenterClient
}

type UsercenterClient struct {
	usercenterpb.UsercenterClient
}

func NewRpcClient(conf *conf.Data, logger log.Logger) (*RpcClient, error) {
	marketcenterClient, err := newMarketcenterClient(conf.Rpc.MarketcenterRpc.Timeout.AsDuration(), conf.Rpc.MarketcenterRpc.Addr, logger)
	if err != nil {
		return nil, err
	}

	usercenterClient, err := newUsercenterClient(conf.Rpc.UsercenterRpc.Timeout.AsDuration(), conf.Rpc.UsercenterRpc.Addr, logger)
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
		MarketcenterClient: marketcenterpb.NewMarketcenterClient(conn),
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
		UsercenterClient: usercenterpb.NewUsercenterClient(conn),
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

			c := util.NewBaseCtx(ctx, logger)
			// 打印请求信息
			reqBytes, _ := json.Marshal(req)
			reqStr := string(reqBytes)
			if len(reqStr) > 300 {
				reqStr = reqStr[:300] + "......"
			}
			c.Log.Infof("GRPC CALL [%s] request: [%s]", serviceName, reqStr)

			reply, err = handler(ctx, req)

			if err != nil {
				if se := errors.FromError(err); se != nil {
					c.Log.Infof("GRPC CALL [%s] error - Code: %d, Reason: %s, Message: %s, Cause: %+v, Metadata: %+v",
						serviceName, se.Code, se.Reason, se.Message, se.Unwrap, se.Metadata)
				} else if st, ok := gstatus.FromError(err); ok {
					c.Log.Infof("GRPC CALL [%s] gRPC error - Code: %d, Message: %s",
						serviceName, st.Code(), st.Message())
				} else {
					c.Log.Errorf("GRPC CALL [%s] unhandled error - Type: %T, Error: %+v", serviceName, err, err)
				}
			} else {
				rspBytes, _ := json.Marshal(reply)
				rspStr := string(rspBytes)
				if len(rspStr) > 300 {
					rspStr = rspStr[:300] + "......"
				}
				c.Log.Infof("GRPC CALL [%s] response: [%s]", serviceName, rspStr)
			}

			return reply, err
		}
	}
}
