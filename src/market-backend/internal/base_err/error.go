package base_err

import (
	apipb "market-proto/proto/market-backend/v1"

	"github.com/go-kratos/kratos/v2/errors"
)

var (
	ErrInternal = errors.New(int(apipb.ErrorCode_INTERNAL), "INTERNAL ERROR", "internal error")
	ErrNetwork  = errors.New(int(apipb.ErrorCode_NETWORK), "NETWORK ERROR", "network error")
	ErrDatabase = errors.New(int(apipb.ErrorCode_DATABASE), "DATABASE ERROR", "database error")
	ErrRedis    = errors.New(int(apipb.ErrorCode_REDIS), "REDIS ERROR", "redis error")
	ErrRpc      = errors.New(int(apipb.ErrorCode_RPC), "RPC ERROR", "rpc error")
)
