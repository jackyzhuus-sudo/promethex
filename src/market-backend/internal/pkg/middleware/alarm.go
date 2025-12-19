package middleware

import (
	"context"
	"fmt"
	"market-backend/internal/alarm"
	"market-backend/internal/pkg"
	"strconv"
	"time"

	"github.com/go-kratos/kratos/v2/middleware"
	"github.com/go-kratos/kratos/v2/transport"
)

func Alarm(larkAlarm *alarm.LarkAlarm) middleware.Middleware {
	return func(handler middleware.Handler) middleware.Handler {
		return func(ctx context.Context, req interface{}) (reply interface{}, err error) {
			var (
				code      int32
				operation string
				msg       string
				reason    string
				cause     string
			)
			startTime := time.Now()
			info, ok := transport.FromServerContext(ctx)
			if ok {
				operation = info.Operation()
			}

			reply, err = handler(ctx, req)

			if se := pkg.LocalFromError(err); se != nil {
				code = se.Code
				msg = se.Message
				reason = se.Reason
				bizCode := 0
				if metadata := se.GetMetadata(); metadata != nil {
					if bizCodeStr, ok := metadata["bizCode"]; ok {
						bizCode, _ = strconv.Atoi(bizCodeStr)
					}
				}

				errorType := (bizCode / 10) % 10
				traceId, _ := ctx.Value("traceId").(string)
				if errorType >= 2 {
					alarmMsg := fmt.Sprintf(
						"[market-backend] 接口报错, 接口：%s, 入参：[%s], 返回码：%d, 业务码：%d, 错误原因：%s, 错误详情：%s, 错误信息：%s, 接口耗时：%s, traceId: [%s]",
						operation, extractArgs(req), code, bizCode, reason, msg, cause, fmt.Sprintf("%v%s", time.Since(startTime).Seconds(), "s"), traceId)

					larkAlarm.SendLarkAlarm(alarmMsg)
				}

			}

			return
		}
	}
}
