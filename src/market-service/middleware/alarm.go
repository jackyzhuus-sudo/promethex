package middleware

import (
	"context"
	"fmt"
	"market-service/internal/pkg/alarm"
	"time"

	"github.com/go-kratos/kratos/v2/errors"
	"github.com/go-kratos/kratos/v2/middleware"
	"github.com/go-kratos/kratos/v2/transport"
)

func Alarm() middleware.Middleware {
	return func(handler middleware.Handler) middleware.Handler {
		return func(ctx context.Context, req interface{}) (reply interface{}, err error) {
			var (
				code      int32
				operation string
				msg       string
			)
			startTime := time.Now()
			info, ok := transport.FromServerContext(ctx)
			if ok {
				operation = info.Operation()
			}

			reply, err = handler(ctx, req)

			if se := errors.FromError(err); se != nil {
				code = se.Code
			}
			if err != nil {
				msg = err.Error()
			}
			traceId, _ := ctx.Value("traceId").(string)
			if code != 0 {
				alarmMsg := fmt.Sprintf(
					"[market-service] 接口报错, 接口：%s, 入参：[%s], 返回码：%d, 错误详情：%s, 接口耗时：%s, traceId: [%s]",
					operation, extractArgs(req), code, msg, fmt.Sprintf("%v%s", time.Since(startTime).Seconds(), "s"), traceId)

				alarm.Lark.Send(alarmMsg)
			}

			return
		}
	}
}
