package middleware

import (
	"context"
	"strconv"

	"github.com/go-kratos/kratos/v2/errors"
	"github.com/go-kratos/kratos/v2/middleware"
)

func ErrCause() middleware.Middleware {
	return func(handler middleware.Handler) middleware.Handler {
		return func(ctx context.Context, req interface{}) (reply interface{}, err error) {
			reply, err = handler(ctx, req)
			if err != nil {
				if se := errors.FromError(err); se != nil {
					code := se.Code

					// 初始化metadata map
					metadata := map[string]string{
						"bizCode": strconv.Itoa(int(code)),
					}

					// 添加cause信息
					cause := se.Unwrap()
					if cause != nil {
						metadata["cause"] = cause.Error()
					}

					// 正确使用WithMetadata，接收返回值
					se = se.WithMetadata(metadata)
					err = se
					return
				}
			}
			return
		}
	}
}
