package middleware

import (
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/go-kratos/kratos/v2/errors"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/middleware"
	"github.com/go-kratos/kratos/v2/transport"
	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
	"golang.org/x/net/context"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

// Logging is an server logging middleware.
func Logging(logger log.Logger) middleware.Middleware {
	return func(handler middleware.Handler) middleware.Handler {
		return func(ctx context.Context, req interface{}) (reply interface{}, err error) {
			var (
				code      int32
				kind      string
				operation string
				reason    string
				msg       string
				rsp       string
				ip        string
			)

			var traceId string
			if md, ok := metadata.FromIncomingContext(ctx); ok {
				if traceIDs := md.Get("traceId"); len(traceIDs) > 0 {
					traceId = traceIDs[0]
				}

			}
			if traceId == "" {
				traceId = uuid.New().String()
			}
			spanId := uuid.New().String()

			ctx = context.WithValue(ctx, "server", "SERVER-GRPC")
			ctx = context.WithValue(ctx, "traceId", traceId)
			ctx = context.WithValue(ctx, "spanId", spanId)

			startTime := time.Now()
			info, ok := transport.FromServerContext(ctx)
			if ok {
				kind = info.Kind().String()
				operation = info.Operation()
				if kind == "http" {
					ip = info.RequestHeader().Get("RemoteAddr")
				} else if kind == "grpc" {
					ip, _ = getClientIP(ctx)
				}
			}

			reqStr := extractArgs(req)
			if strings.Contains(operation, "UploadFile") {
				if len(reqStr) > 200 {
					reqStr = "upload-file req - " + reqStr[:200] + "......"
				}
			}
			_ = log.WithContext(ctx, logger).Log(log.LevelInfo,
				"traceId", traceId,
				"spanId", spanId,
				"operation", operation,
				"ip", ip,
				"request", reqStr,
			)
			// 前
			reply, err = handler(ctx, req)
			// 后

			if se := errors.FromError(err); se != nil {
				code = se.Code
				msg = se.Message
				reason = se.Reason
			}

			level, _ := extractError(err)

			// 处理回包日志
			replyByte, _ := jsoniter.Marshal(reply)
			rsp = string(replyByte)
			if strings.Contains(operation, "DownloadFile") {
				if len(rsp) > 200 {
					rsp = "download-file rsp - " + rsp[:200] + "......"
				}
			}

			_ = log.WithContext(ctx, logger).Log(level,
				// "kind", "server",
				// "component", kind,
				"traceId", traceId,
				"spanId", spanId,
				"operation", operation,
				"code", code,
				"reason", reason,
				"message", msg,
				"response", rsp,
				"cost", fmt.Sprintf("%v%s", time.Since(startTime).Seconds(), "s"),
			)
			return
		}
	}

}

// extractArgs returns the string of the req
func extractArgs(req interface{}) string {
	if stringer, ok := req.(fmt.Stringer); ok {
		return stringer.String()
	}
	return fmt.Sprintf("%+v", req)
}

// extractError returns the string of the error
func extractError(err error) (log.Level, string) {
	if err != nil {
		return log.LevelError, fmt.Sprintf("%+v", err)
	}
	return log.LevelInfo, ""
}

func getClientIP(ctx context.Context) (string, error) {
	pr, ok := peer.FromContext(ctx)
	if !ok {
		return "", fmt.Errorf("[getClinetIP] invoke FromContext() failed")
	}
	if pr.Addr == net.Addr(nil) {
		return "", fmt.Errorf("[getClientIP] peer.Addr is nil")
	}
	addSlice := strings.Split(pr.Addr.String(), ":")
	return addSlice[0], nil
}
