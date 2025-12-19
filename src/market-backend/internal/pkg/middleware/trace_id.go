package middleware

import (
	"context"

	"github.com/go-kratos/kratos/v2/errors"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/middleware"
	"github.com/go-kratos/kratos/v2/transport"
	"github.com/google/uuid"
)

func TraceId(logger log.Logger) middleware.Middleware {
	return func(handler middleware.Handler) middleware.Handler {
		return func(ctx context.Context, req interface{}) (reply interface{}, err error) {
			tr, ok := transport.FromServerContext(ctx)
			if !ok {
				return nil, errors.Unauthorized("AUTH_ERROR", "invalid context")
			}
			traceId := tr.RequestHeader().Get("X-Trace-ID")
			if traceId == "" {
				traceId = uuid.New().String()
			}
			ctx = context.WithValue(ctx, "traceId", traceId)
			tr.ReplyHeader().Set("X-Trace-ID", traceId)
			return handler(ctx, req)
		}
	}
}
