package common

import (
	"context"

	"github.com/go-kratos/kratos/v2/log"
	"gorm.io/gorm"
)

type Ctx struct {
	Ctx context.Context
	Log *log.Helper
}

// NewBaseCtx 创建带有业务属性的ctx
func NewBaseCtx(ctx context.Context, logger log.Logger) Ctx {
	traceId, _ := ctx.Value("traceId").(string)
	spanId, _ := ctx.Value("spanId").(string)
	server, _ := ctx.Value("server").(string)

	return Ctx{
		Ctx: ctx,
		Log: log.NewHelper(log.With(logger, "server", server, "traceId", traceId, "spanId", spanId)),
	}
}

func CloneBaseCtx(ctx Ctx, logger log.Logger) Ctx {
	traceId, _ := ctx.Ctx.Value("traceId").(string)
	spanId, _ := ctx.Ctx.Value("spanId").(string)
	server, _ := ctx.Ctx.Value("server").(string)
	newCtx := context.Background()
	newCtx = context.WithValue(newCtx, "traceId", traceId)
	newCtx = context.WithValue(newCtx, "spanId", spanId)
	newCtx = context.WithValue(newCtx, "server", server)
	return Ctx{
		Ctx: newCtx,
		Log: log.NewHelper(log.With(logger, "server", server, "traceId", traceId, "spanId", spanId)),
	}
}

func GetTraceId(ctx context.Context) string {
	traceId, _ := ctx.Value("traceId").(string)
	return traceId
}

// GetDB .
func GetDB(ctx context.Context, db *gorm.DB) *gorm.DB {
	return db.WithContext(ctx)
}
