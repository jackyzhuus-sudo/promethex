package base_err

import (
	bayespb "market-proto/proto/market-backend/v1"

	"github.com/go-kratos/kratos/v2/errors"
)

var (
	ErrInternal = errors.New(int(bayespb.ErrorCode_INTERNAL), "INTERNAL ERROR", "internal error")
	ErrNetwork  = errors.New(int(bayespb.ErrorCode_NETWORK), "NETWORK ERROR", "network error")
	ErrDatabase = errors.New(int(bayespb.ErrorCode_DATABASE), "DATABASE ERROR", "database error")
	ErrRedis    = errors.New(int(bayespb.ErrorCode_REDIS), "REDIS ERROR", "redis error")
	ErrRpc      = errors.New(int(bayespb.ErrorCode_RPC), "RPC ERROR", "rpc error")
)
