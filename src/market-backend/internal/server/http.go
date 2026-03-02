package server

import (
	"context"
	"market-backend/internal/alarm"
	"market-backend/internal/conf"
	"market-backend/internal/data"
	"market-backend/internal/pkg/middleware"
	"market-backend/internal/service/http_api"
	"market-backend/internal/service/sse_api"
	apipb "market-proto/proto/market-backend/v1"
	netHttp "net/http"
	"time"

	"github.com/go-kratos/aegis/ratelimit/bbr"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/middleware/ratelimit"
	"github.com/go-kratos/kratos/v2/middleware/recovery"
	"github.com/go-kratos/kratos/v2/transport/http"
	"github.com/gorilla/handlers"
)


// NewHTTPServer new an HTTP server.
func NewHTTPServer(c *conf.Server, cfgCustom *conf.Custom, cfgData *conf.Data, httpApiService *http_api.HttpApiService, sseApiService *sse_api.SseApiService, logger log.Logger, data *data.Data, larkAlarm *alarm.LarkAlarm) *http.Server {
	// 创建自定义编解码器
	codec := middleware.NewCustomCodec()

	var opts = []http.ServerOption{
		http.Middleware(
			recovery.Recovery(),
			// 添加限流中间件
			ratelimit.Server(
				ratelimit.WithLimiter(bbr.NewLimiter()),
			),
			middleware.TraceId(logger),
			middleware.CommonParams(logger, cfgCustom.AssetTokens.Usdc.Address, cfgCustom.AssetTokens.Points.Address), // 公共参数提取中间件
			middleware.Alarm(larkAlarm),
			middleware.Logging(logger),
			middleware.Auth(cfgData, cfgCustom, logger, data),
		),
		http.RequestDecoder(codec.RequestDecoder()),
		http.ResponseEncoder(codec.ResponseEncoder()),
		http.ErrorEncoder(middleware.ErrorEncoder()),
	}

	// CORS filter — explicit origin whitelist (wildcard + credentials violates CORS spec)
	opts = append(opts, http.Filter(
		handlers.CORS(
			handlers.AllowedOrigins([]string{
				"https://app.promethex.market",
				"https://promethex-market-production.promethex.workers.dev",
				"https://promethex-market-v2-staging.promethex.workers.dev",
				"https://promethex-market-staging.promethex.workers.dev",
				"http://localhost:5173",
				"http://localhost:5174",
				"http://localhost:5175",
			}),
			handlers.AllowedMethods([]string{"GET", "POST", "OPTIONS"}),
			handlers.AllowedHeaders([]string{"Content-Type", "Authorization", "sentry-trace", "baggage"}),
			handlers.AllowCredentials(),
		),
	))
	if c.Http.Network != "" {
		opts = append(opts, http.Network(c.Http.Network))
	}
	if c.Http.Addr != "" {
		opts = append(opts, http.Address(c.Http.Addr))
	}
	if c.Http.Timeout != nil {
		opts = append(opts, http.Timeout(c.Http.Timeout.AsDuration()))
	}
	srv := http.NewServer(opts...)

	// 注册protobuf生成的路由
	apipb.RegisterHttpApiHTTPServer(srv, httpApiService)
	apipb.RegisterSseApiHTTPServer(srv, sseApiService)

	// Health check endpoint (bypasses middleware)
	srv.HandleFunc("/healthz", func(w netHttp.ResponseWriter, r *netHttp.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(netHttp.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})

	// SSE 连接使用自定义处理器，只接受 POST 请求，设置较长的超时时间
	srv.HandleFunc("/api/v1/sse/connect", func(w netHttp.ResponseWriter, r *netHttp.Request) {
		// 只允许 POST 方法
		if r.Method != netHttp.MethodPost {
			netHttp.Error(w, "Method not allowed", netHttp.StatusMethodNotAllowed)
			w.Header().Set("Allow", "POST")
			return
		}

		// 为 SSE 连接设置 10 分钟的超时时间
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()
		r = r.WithContext(ctx)
		sseApiService.HandleConnection(w, r)
	})

	return srv
}
