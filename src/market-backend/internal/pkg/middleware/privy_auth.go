package middleware

import (
	"context"
	"fmt"
	"market-backend/internal/conf"
	"market-backend/internal/data"
	"market-backend/internal/pkg/util"
	usercenterpb "market-proto/proto/market-service/usercenter/v1"
	"net/http"
	"strings"
	"time"

	stdErr "errors"

	"github.com/go-kratos/kratos/v2/errors"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/golang-jwt/jwt/v5"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	pb "market-proto/proto/market-backend/v1"

	"github.com/go-kratos/kratos/v2/middleware"
	"github.com/go-kratos/kratos/v2/transport"
)

type PrivyClaims struct {
	jwt.RegisteredClaims
	// AppId      string `json:"aud,omitempty"`
	// Expiration uint64 `json:"exp,omitempty"`
	// Issuer     string `json:"iss,omitempty"`
	// UserId     string `json:"sub,omitempty"`
}

// This method will be used to check the token's claims later
func (c *PrivyClaims) Valid(appId string) error {
	if len(c.Audience) == 0 {
		return stdErr.New("aud claim is invalid")
	}

	aud := c.Audience[0]
	if aud != appId {
		return stdErr.New("aud claim must be your Privy App ID.")
	}
	if c.Issuer != "privy.io" {
		return stdErr.New("iss claim must be 'privy.io'")
	}
	if c.ExpiresAt.Unix() < time.Now().Unix() {
		return stdErr.New("Token is expired.")
	}

	return nil
}

func Auth(cfgData *conf.Data, cfgCustom *conf.Custom, logger log.Logger, data *data.Data) middleware.Middleware {
	return func(handler middleware.Handler) middleware.Handler {
		return func(ctx context.Context, req interface{}) (interface{}, error) {
			tr, ok := transport.FromServerContext(ctx)
			if !ok {
				return nil, errors.Unauthorized("AUTH_ERROR", "invalid context")
			}

			needAuthFlag := needAuth(tr.Operation())
			isLoginOp := tr.Operation() == "/bayes.v1.BayesHttp/Login"

			authHeader := tr.RequestHeader().Get("Authorization")

			enrichedCtx, _, err := AuthenticateWithToken(ctx, authHeader, cfgData, cfgCustom, data, isLoginOp)

			if err != nil {
				if needAuthFlag {
					return nil, err
				}
				return handler(ctx, req)
			}

			return handler(enrichedCtx, req)
		}
	}
}

func privyValid(authToken string, verificationKey string, appId string) (*PrivyClaims, error) {

	token, err := jwt.ParseWithClaims(authToken, &PrivyClaims{}, func(token *jwt.Token) (interface{}, error) {
		if token.Method.Alg() != "ES256" {
			return nil, fmt.Errorf("Unexpected JWT signing method=%v", token.Header["alg"])
		}
		// https://pkg.go.dev/github.com/dgrijalva/jwt-go#ParseECPublicKeyFromPEM
		ec, err := jwt.ParseECPublicKeyFromPEM([]byte(verificationKey))
		if err != nil {
			log.Errorf("ParseECPublicKeyFromPEM error: %v", err)
			return nil, fmt.Errorf("ParseECPublicKeyFromPEM error: %v", err)
		}
		return ec, nil
	})
	if err != nil {
		return nil, errors.Unauthorized("AUTH_ERROR", "JWT signature is invalid.")
	}

	privyClaim, ok := token.Claims.(*PrivyClaims)
	if !ok {
		return nil, errors.Unauthorized("AUTH_ERROR", "JWT does not have all the necessary claims.")
	}

	err = privyClaim.Valid(appId)
	if err != nil {
		log.Errorf("privyClaim.Valid error: %v", err)
		return nil, errors.Unauthorized("AUTH_ERROR", "JWT signature is invalid.")
	}

	return privyClaim, nil
}

// AuthenticateWithToken 通用的token认证函数
func AuthenticateWithToken(ctx context.Context, authHeader string, cfgData *conf.Data, cfgCustom *conf.Custom, data *data.Data, isLoginOp bool) (context.Context, string, error) {
	if !cfgCustom.GetNeedAuth() {
		ctx = context.WithValue(ctx, util.UidKey, "test_user")
		ctx = context.WithValue(ctx, util.DidKey, "did:privy:test_user")
		return ctx, "test_user", nil
	}

	if authHeader == "" {
		return nil, "", errors.Unauthorized("AUTH_ERROR", "Authorization header required")
	}

	log := log.NewHelper(log.DefaultLogger)
	enrichedCtx, err := privyTryEnrichContextWithUserInfo(ctx, authHeader, cfgData, data, isLoginOp, log)
	if err != nil {
		return nil, "", err
	}

	uid := util.GetUidFromCtx(enrichedCtx)
	return enrichedCtx, uid, nil
}

// AuthenticateFromHTTPRequest 从HTTP请求中进行认证（供外部使用）
func AuthenticateFromHTTPRequest(r *http.Request, cfgData *conf.Data, cfgCustom *conf.Custom, data *data.Data) (context.Context, string, error) {
	authHeader := r.Header.Get("Authorization")
	return AuthenticateWithToken(r.Context(), authHeader, cfgData, cfgCustom, data, false)
}

// tryEnrichContextWithUserInfo 尝试解析 token 并用用户信息丰富上下文
// 如果成功返回增强的上下文，失败返回错误
func privyTryEnrichContextWithUserInfo(
	ctx context.Context,
	authHeader string,
	cfgData *conf.Data,
	data *data.Data,
	isLoginOp bool,
	log *log.Helper,
) (context.Context, error) {
	// 1. 获取 token
	authToken, err := getToken(authHeader)
	if err != nil {
		return nil, errors.Unauthorized("AUTH_ERROR", "get token error")
	}

	verificationKey := cfgData.Privy.VerificationKey
	appId := cfgData.Privy.AppId

	privyClaim, err := privyValid(authToken, verificationKey, appId)
	if err != nil {
		return nil, err
	}

	privyDid := privyClaim.Subject

	newCtx := context.WithValue(ctx, util.DidKey, privyDid)

	if !isLoginOp {
		userInfo, err := data.RpcClient.UsercenterClient.GetUserInfo(newCtx, &usercenterpb.GetUserInfoRequest{
			Issuer: privyDid,
		})
		if err != nil {
			return newCtx, errors.InternalServer("AUTH_ERROR", "get user info error")
		}

		if userInfo.Uid == "" {
			return newCtx, errors.Unauthorized("AUTH_ERROR", "user not found")
		}

		// 将用户信息添加到上下文
		newCtx = context.WithValue(newCtx, util.UidKey, userInfo.Uid)
		newCtx = context.WithValue(newCtx, util.UserInfoKey, userInfo)
	}

	return newCtx, nil
}

func getToken(authorization string) (string, error) {
	authHeader := authorization
	auths := strings.SplitN(authHeader, " ", 2)
	if len(auths) != 2 || !strings.EqualFold(auths[0], "Bearer") {
		return "", stdErr.New("auth token params error")
	}
	token := auths[1]
	return token, nil
}

// 检查是否需要鉴权
func needAuth(method string) bool {
	// 解析方法全名
	descriptor, err := protoregistry.GlobalFiles.
		FindDescriptorByName(protoreflect.FullName(GrpcPathToProtoMethod(method)))
	if err != nil {
		log.Errorf("find descriptor error: %+v", err)
		return true // 默认需要鉴权
	}

	if m, ok := descriptor.(protoreflect.MethodDescriptor); ok {
		opts := m.Options()
		if opts != nil {
			// 检查no_auth选项
			return proto.GetExtension(opts, pb.E_NeedAuth).(bool)
		}
	}
	return true
}

func GrpcPathToProtoMethod(grpcPath string) string {
	// 去掉开头的斜杠
	if strings.HasPrefix(grpcPath, "/") {
		grpcPath = grpcPath[1:]
	}

	// 将路径中的斜杠替换为点
	parts := strings.Split(grpcPath, "/")
	if len(parts) != 2 {
		// 格式不符合预期，返回原始路径
		return grpcPath
	}

	// 格式应该是 "{package}.{service}/{method}"
	servicePath := parts[0] // 例如 "bayes.v1.BayesHttp"
	method := parts[1]      // 例如 "Login"

	// 组合成 protoreflect 需要的格式: "{package}.{service}.{method}"
	return servicePath + "." + method
}
