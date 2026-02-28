package util

import (
	"context"
	usercenterpb "market-proto/proto/market-service/usercenter/v1"

	"github.com/go-kratos/kratos/v2/log"
)

type CtxKey string

const (
	UidKey        CtxKey = "uid"
	UserInfoKey   CtxKey = "user_info"
	DidKey        CtxKey = "did"
	EoaAddressKey CtxKey = "eoa_address"
	// 公共参数 keys
	ClientIPKey   CtxKey = "client_ip"
	UserAgentKey  CtxKey = "user_agent"
	AcceptLangKey CtxKey = "accept_language"
	DeviceTypeKey CtxKey = "device_type"
	AppVersionKey CtxKey = "app_version"
	BaseTokenKey        CtxKey = "base_token"
	BaseTokenAddressKey CtxKey = "base_token_address"
)

type Ctx struct {
	Ctx context.Context
	Log *log.Helper
}

// NewBaseCtx 创建带有业务属性的ctx
func NewBaseCtx(ctx context.Context, logger log.Logger) Ctx {
	traceId, _ := ctx.Value("traceId").(string)
	return Ctx{
		Ctx: ctx,
		Log: log.NewHelper(log.With(logger, "traceId", traceId)),
	}
}

func GetUidFromCtx(ctx context.Context) string {
	uid, ok := ctx.Value(UidKey).(string)
	if !ok {
		return ""
	}
	return uid
}

func GetUserInfoFromCtx(ctx context.Context) *usercenterpb.GetUserInfoReply {
	userInfo, ok := ctx.Value(UserInfoKey).(*usercenterpb.GetUserInfoReply)
	if !ok {
		return nil
	}
	return userInfo
}

func GetDidFromCtx(ctx context.Context) string {
	did, ok := ctx.Value(DidKey).(string)
	if !ok {
		return ""
	}
	return did
}

func GetEoaAddressFromCtx(ctx context.Context) string {
	eoaAddress, ok := ctx.Value(EoaAddressKey).(string)
	if !ok {
		return ""
	}
	return eoaAddress
}

// 获取公共参数的函数
func GetClientIPFromCtx(ctx context.Context) string {
	ip, ok := ctx.Value(ClientIPKey).(string)
	if !ok {
		return ""
	}
	return ip
}

func GetUserAgentFromCtx(ctx context.Context) string {
	ua, ok := ctx.Value(UserAgentKey).(string)
	if !ok {
		return ""
	}
	return ua
}

func GetAcceptLanguageFromCtx(ctx context.Context) string {
	lang, ok := ctx.Value(AcceptLangKey).(string)
	if !ok {
		return ""
	}
	return lang
}

func GetDeviceTypeFromCtx(ctx context.Context) string {
	device, ok := ctx.Value(DeviceTypeKey).(string)
	if !ok {
		return ""
	}
	return device
}

func GetBaseTokenFromCtx(ctx context.Context) int {
	baseToken, ok := ctx.Value(BaseTokenKey).(int)
	if !ok {
		return 0
	}
	return baseToken
}

func GetBaseTokenAddressFromCtx(ctx context.Context) string {
	addr, _ := ctx.Value(BaseTokenAddressKey).(string)
	return addr
}

