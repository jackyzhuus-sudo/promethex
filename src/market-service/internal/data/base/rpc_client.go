package base

import (
	"context"
	recommendationPb "market-proto/proto/recommendation/v1"
	"market-service/internal/conf"
	"time"

	"github.com/go-kratos/kratos/v2/middleware"
	"github.com/go-kratos/kratos/v2/middleware/recovery"
	"github.com/go-kratos/kratos/v2/transport"
	"github.com/go-kratos/kratos/v2/transport/grpc"
	"github.com/google/uuid"
)

type RpcClient struct {
	RecommendationClient *RecommendationClient
}

type RecommendationClient struct {
	recommendationPb.RecommendationHttpClient
}

func newRpcClient(c *conf.Data) *RpcClient {
	recommendationClient, err := newRecommendationClient(c.Rpc.RecommendationRpc.Timeout.AsDuration(), c.Rpc.RecommendationRpc.Addr)
	if err != nil {
		panic(err)
	}
	return &RpcClient{
		RecommendationClient: recommendationClient,
	}
}

func newRecommendationClient(timeout time.Duration, addr string) (*RecommendationClient, error) {

	conn, err := grpc.DialInsecure(
		context.Background(),
		grpc.WithTimeout(timeout),
		grpc.WithEndpoint(addr),
		grpc.WithMiddleware(
			recovery.Recovery(),
			TraceClient(),
		),
	)
	if err != nil {
		return nil, err
	}

	return &RecommendationClient{
		RecommendationHttpClient: recommendationPb.NewRecommendationHttpClient(conn),
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
