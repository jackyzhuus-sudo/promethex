package base

import (
	"market-service/internal/pkg/common"
	"time"

	"github.com/go-redis/redis/v8"
	"gorm.io/gorm"
)

type RepoInterface interface {
	// db
	Create(common.Ctx, interface{}) error
	Modify(common.Ctx, interface{}) error
	ModifyByMap(common.Ctx, interface{}, map[string]interface{}) error
	Delete(common.Ctx, interface{}) error
	ExecTx(common.Ctx, func(common.Ctx, *gorm.DB) error) error
	// redis锁
	AcquireLock(common.Ctx, string, time.Duration) (string, bool, error)
	ReleaseLock(common.Ctx, string, string) error
	ReleaseLockDirect(common.Ctx, string) error
	// redis stream
	StreamAddMessage(ctx common.Ctx, streamKey string, data interface{}) (string, error)
	StreamAddMessageWithID(ctx common.Ctx, streamKey string, messageID string, data interface{}) (string, error)
	StreamCreateGroup(ctx common.Ctx, streamKey string, groupName string, startID string) error
	StreamReadGroup(ctx common.Ctx, streamKey string, groupName string, consumerName string, count int64) ([]redis.XStream, error)
	StreamReadGroupBlocking(ctx common.Ctx, streamKey string, groupName string, consumerName string, count int64, blockDuration time.Duration) ([]redis.XStream, error)
	StreamReadGroupBlockingMultiple(ctx common.Ctx, streamKeys []string, groupName string, consumerName string, count int64, blockDuration time.Duration) ([]redis.XStream, error)
	StreamReadPendingMessages(ctx common.Ctx, streamKey string, groupName string, consumerName string, count int64, blockDuration time.Duration) ([]redis.XStream, error)
	StreamReadPendingMessagesMultiple(ctx common.Ctx, streamKeys []string, groupName string, consumerName string, count int64, blockDuration time.Duration) ([]redis.XStream, error)
	StreamAckMessage(ctx common.Ctx, streamKey string, groupName string, messageIDs ...string) (int64, error)
	StreamPendingMessages(ctx common.Ctx, streamKey string, groupName string) (*redis.XPending, error)

	PublishJSON(ctx common.Ctx, channel string, message interface{}) error

	ZIncrBy(ctx common.Ctx, key string, increment float64, member string) (float64, error)
	ZRevRangeWithScores(ctx common.Ctx, key string, start, stop int64) ([]redis.Z, error)
	ZCount(ctx common.Ctx, key, min, max string) (int64, error)
}
