package middleware

import (
	"context"
	"market-backend/internal/pkg/util"
	"net"
	"strings"

	kratosHttp "github.com/go-kratos/kratos/v2/transport/http"

	"github.com/go-kratos/kratos/v2/errors"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/middleware"
	"github.com/go-kratos/kratos/v2/transport"
	"google.golang.org/grpc/peer"
)

// CommonParams 公共参数提取中间件
func CommonParams(logger log.Logger) middleware.Middleware {
	return func(handler middleware.Handler) middleware.Handler {
		return func(ctx context.Context, req interface{}) (interface{}, error) {
			tr, ok := transport.FromServerContext(ctx)
			if !ok {
				return nil, errors.Unauthorized("TRANSPORT_ERROR", "invalid transport context")
			}

			// 提取并注入公共参数到上下文
			enrichedCtx := extractAndInjectCommonParams(ctx, tr, logger)

			return handler(enrichedCtx, req)
		}
	}
}

// extractAndInjectCommonParams 提取公共参数并注入到上下文中
func extractAndInjectCommonParams(ctx context.Context, tr transport.Transporter, logger log.Logger) context.Context {
	newCtx := ctx

	// 1. 提取客户端IP地址
	clientIP := extractClientIP(ctx, tr)
	if clientIP != "" {
		newCtx = context.WithValue(newCtx, util.ClientIPKey, clientIP)
	}

	// 2. 提取 User-Agent
	userAgent := tr.RequestHeader().Get("User-Agent")
	if userAgent == "" {
		userAgent = tr.RequestHeader().Get("user-agent")
	}
	newCtx = context.WithValue(newCtx, util.UserAgentKey, userAgent)

	// 3. 提取语言设置
	acceptLang := tr.RequestHeader().Get("Accept-Language")
	if acceptLang == "" {
		acceptLang = tr.RequestHeader().Get("accept-language")
	}
	newCtx = context.WithValue(newCtx, util.AcceptLangKey, acceptLang)

	// 4. 提取自定义应用参数（从header中）
	// App版本
	appVersion := getHeaderValue(tr, []string{"X-App-Version", "x-app-version", "App-Version"})
	newCtx = context.WithValue(newCtx, util.AppVersionKey, appVersion)

	// 设备类型
	deviceType := getHeaderValue(tr, []string{"X-Device-Type", "x-device-type", "Device-Type"})
	if deviceType == "" {
		// 根据User-Agent判断设备类型
		deviceType = detectDeviceType(userAgent)
	}
	newCtx = context.WithValue(newCtx, util.DeviceTypeKey, deviceType)

	newCtx = extractURLParams(newCtx, tr)

	return newCtx
}

// extractClientIP 提取客户端IP地址
func extractClientIP(ctx context.Context, tr transport.Transporter) string {
	var clientIP string

	if tr.Kind().String() == "http" {
		// HTTP请求：优先从header中获取真实IP
		clientIP = getHeaderValue(tr, []string{
			"X-Forwarded-For",
			"x-forwarded-for",
			"X-Real-IP",
			"x-real-ip",
			"CF-Connecting-IP", // Cloudflare
			"True-Client-IP",   // Akamai
		})

		// 处理X-Forwarded-For可能包含多个IP的情况
		if clientIP != "" {
			ips := strings.Split(clientIP, ",")
			clientIP = strings.TrimSpace(ips[0])
		}

		// 如果没有从header获取到，尝试从RemoteAddr获取
		if clientIP == "" {
			clientIP = tr.RequestHeader().Get("RemoteAddr")
		}
	} else if tr.Kind().String() == "grpc" {
		// gRPC请求：从peer中获取
		if pr, ok := peer.FromContext(ctx); ok && pr.Addr != nil {
			addSlice := strings.Split(pr.Addr.String(), ":")
			if len(addSlice) > 0 {
				clientIP = addSlice[0]
			}
		}
	}

	// 验证IP地址格式
	if clientIP != "" && net.ParseIP(clientIP) == nil {
		clientIP = ""
	}

	return clientIP
}

// getHeaderValue 从多个可能的header名称中获取值
func getHeaderValue(tr transport.Transporter, headerNames []string) string {
	for _, name := range headerNames {
		value := tr.RequestHeader().Get(name)
		if value != "" {
			return value
		}
	}
	return ""
}

// detectDeviceType 根据User-Agent检测设备类型
func detectDeviceType(userAgent string) string {
	if userAgent == "" {
		return "unknown"
	}

	ua := strings.ToLower(userAgent)

	// 移动设备检测
	mobileKeywords := []string{"mobile", "android", "iphone", "ipad", "ipod", "blackberry", "windows phone"}
	for _, keyword := range mobileKeywords {
		if strings.Contains(ua, keyword) {
			if strings.Contains(ua, "ipad") {
				return "tablet"
			}
			if strings.Contains(ua, "android") && strings.Contains(ua, "tablet") {
				return "tablet"
			}
			return "mobile"
		}
	}

	// 桌面浏览器检测
	desktopKeywords := []string{"windows", "macintosh", "linux", "x11"}
	for _, keyword := range desktopKeywords {
		if strings.Contains(ua, keyword) {
			return "desktop"
		}
	}

	return "unknown"
}

// extractURLParams 从URL参数中提取额外信息
func extractURLParams(ctx context.Context, tr transport.Transporter) context.Context {
	if httpTr, ok := tr.(kratosHttp.Transporter); ok {
		req := httpTr.Request()
		queryParams := req.URL.Query()
		if addr := queryParams.Get("baseTokenAddress"); addr != "" {
			ctx = context.WithValue(ctx, util.BaseTokenAddressKey, strings.ToLower(addr))
		}
	}
	return ctx
}
