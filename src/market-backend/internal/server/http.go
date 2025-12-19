package server

import (
	"context"
	"market-backend/internal/alarm"
	"market-backend/internal/conf"
	"market-backend/internal/data"
	"market-backend/internal/pkg/middleware"
	"market-backend/internal/service/bayes_http"
	"market-backend/internal/service/bayes_sse"
	bayespb "market-proto/proto/market-backend/v1"
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
func NewHTTPServer(c *conf.Server, cfgCustom *conf.Custom, cfgData *conf.Data, bayesHttpService *bayes_http.BayesHttpService, bayesSseService *bayes_sse.BayesSseService, logger log.Logger, data *data.Data, larkAlarm *alarm.LarkAlarm) *http.Server {
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
			middleware.CommonParams(logger), // 公共参数提取中间件
			middleware.Alarm(larkAlarm),
			middleware.Logging(logger),
			middleware.Auth(cfgData, cfgCustom, logger, data),
		),
		http.RequestDecoder(codec.RequestDecoder()),
		http.ResponseEncoder(codec.ResponseEncoder()),
		http.ErrorEncoder(middleware.ErrorEncoder()),
	}

	opts = append(opts, http.Filter(handlers.CORS(
		handlers.AllowedOrigins([]string{"*"}),
		handlers.AllowedMethods([]string{"GET", "POST"}),
		handlers.AllowedHeaders([]string{"Content-Type", "Authorization", "sentry-trace", "baggage"}),
		handlers.AllowCredentials(),
	)))
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
	bayespb.RegisterBayesHttpHTTPServer(srv, bayesHttpService)
	bayespb.RegisterBayesSseHTTPServer(srv, bayesSseService)

	// SSE 连接使用自定义处理器，只接受 POST 请求，设置较长的超时时间
	srv.HandleFunc("/bayes/sse/connect", func(w netHttp.ResponseWriter, r *netHttp.Request) {
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
		bayesSseService.HandleConnection(w, r)
	})

	return srv
}
