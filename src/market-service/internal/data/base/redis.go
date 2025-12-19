package base

import (
	"context"
	"encoding/json"
	"market-service/internal/conf"
	"market-service/internal/pkg/common"
	"market-service/internal/pkg/util"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
)

type Redis struct {
	Usercenter   *redis.Client
	Marketcenter *redis.Client
}

func newRedis(c *conf.Data, logger log.Logger) *Redis {
	usercenterConf := c.Redis.Usercenter
	marketcenterConf := c.Redis.Marketcenter
	return &Redis{
		Usercenter:   newRedisandConnect(usercenterConf, logger),
		Marketcenter: newRedisandConnect(marketcenterConf, logger),
	}
}

// newRedisandConnect 初始化redis
func newRedisandConnect(conf *conf.Data_RedisData_Redis, logger log.Logger) *redis.Client {
	rdb := redis.NewClient(&redis.Options{
		Addr:         conf.Addr,
		Password:     conf.Password,
		DB:           int(conf.Db),
		DialTimeout:  conf.DialTimeout.AsDuration(),
		WriteTimeout: conf.WriteTimeout.AsDuration(),
		ReadTimeout:  conf.ReadTimeout.AsDuration(),
	})
	result, err := rdb.Ping(context.Background()).Result()
	if err != nil {
		log.Fatal(errors.Wrap(err, "redis connect error"))
	} else {
		log.Infof("redis ping success: %v", result)
	}
	// hook := NewRedisHook(logger)
	// rdb.AddHook(hook)
	return rdb
}

func closeRedis(rdb *redis.Client) {
	rdb.Close()
}

type RedisHook struct {
	logger log.Logger
}

func NewRedisHook(logger log.Logger) *RedisHook {
	return &RedisHook{
		logger: logger,
	}
}

func (h *RedisHook) BeforeProcess(ctx context.Context, cmd redis.Cmder) (context.Context, error) {
	c := common.NewBaseCtx(ctx, h.logger)
	c.Log.Infow("Redis command starting",
		"command", cmd.Name(),
		"args", cmd.Args(),
	)
	return ctx, nil
}

func (h *RedisHook) AfterProcess(ctx context.Context, cmd redis.Cmder) error {
	c := common.NewBaseCtx(ctx, h.logger)

	if err := cmd.Err(); err != nil {
		c.Log.Warnw("Redis command failed",
			"command", cmd.Name(),
			"args", cmd.Args(),
			"error", err.Error(),
		)
		return err
	}

	c.Log.Infow("Redis command completed",
		"command", cmd.Name(),
		"args", cmd.Args(),
		"result", cmd.String(),
	)
	return nil
}

func (h *RedisHook) BeforeProcessPipeline(ctx context.Context, cmds []redis.Cmder) (context.Context, error) {
	return ctx, nil
}

func (h *RedisHook) AfterProcessPipeline(ctx context.Context, cmds []redis.Cmder) error {
	return nil
}

func (r Infra) AcquireLock(ctx common.Ctx, lockKey string, expiration time.Duration) (string, bool, error) {
	lockID := util.GenerateUUID()
	result, err := r.redis.SetNX(ctx.Ctx, lockKey, lockID, expiration).Result()
	if err != nil {
		return "", false, err
	}

	return lockID, result, nil
}

func (r Infra) ReleaseLock(ctx common.Ctx, lockKey string, lockID string) error {
	luaScript := `
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("DEL", KEYS[1])
		else
			return 0
		end
	`
	result, err := r.redis.Eval(ctx.Ctx, luaScript, []string{lockKey}, lockID).Result()
	if err != nil {
		return err
	}

	// 检查 Lua 脚本执行结果
	if result == int64(0) {
		return errors.New("release lock failed")
	}
	return nil
}

// ReleaseLockDirect 直接释放锁，不检查lockID是否匹配
func (r Infra) ReleaseLockDirect(ctx common.Ctx, lockKey string) error {
	err := r.redis.Del(ctx.Ctx, lockKey).Err()
	if err != nil {
		return err
	}
	return nil
}

// Publish 发布消息到指定频道
func (r Infra) Publish(ctx common.Ctx, channel string, message interface{}) error {
	_, err := r.redis.Publish(ctx.Ctx, channel, message).Result()
	if err != nil {
		ctx.Log.Errorf("Redis Publish error [channel: %s][message: %v][error: %v]", channel, message, err)
		return err
	}
	return nil
}

// PublishJSON 发布 JSON 格式的消息到指定频道
func (r Infra) PublishJSON(ctx common.Ctx, channel string, data interface{}) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		ctx.Log.Errorf("PublishJSON marshal error [channel: %s][data: %v][error: %v]", channel, data, err)
		return err
	}

	_, err = r.redis.Publish(ctx.Ctx, channel, jsonData).Result()
	if err != nil {
		ctx.Log.Errorf("PublishJSON error [channel: %s][data: %v][error: %v]", channel, data, err)
		return err
	}
	return nil
}

// ZAdd 添加一个或多个成员到有序集合，或者更新已存在成员的分数
func (r Infra) ZAdd(ctx common.Ctx, key string, members ...*redis.Z) (int64, error) {
	result, err := r.redis.ZAdd(ctx.Ctx, key, members...).Result()
	if err != nil {
		ctx.Log.Errorf("ZAdd error [key: %s][members: %v][error: %v]", key, members, err)
		return 0, err
	}
	return result, nil
}

// ZIncrBy 为有序集合的成员增加分数
func (r Infra) ZIncrBy(ctx common.Ctx, key string, increment float64, member string) (float64, error) {
	result, err := r.redis.ZIncrBy(ctx.Ctx, key, increment, member).Result()
	if err != nil {
		ctx.Log.Errorf("ZIncrBy error [key: %s][increment: %f][member: %s][error: %v]", key, increment, member, err)
		return 0, err
	}
	return result, nil
}

// ZRange 获取有序集合指定范围内的成员（按分数从低到高）
func (r Infra) ZRange(ctx common.Ctx, key string, start, stop int64) ([]string, error) {
	result, err := r.redis.ZRange(ctx.Ctx, key, start, stop).Result()
	if err != nil {
		ctx.Log.Errorf("ZRange error [key: %s][start: %d][stop: %d][error: %v]", key, start, stop, err)
		return nil, err
	}
	return result, nil
}

// ZRevRange 获取有序集合指定范围内的成员（按分数从高到低）
func (r Infra) ZRevRange(ctx common.Ctx, key string, start, stop int64) ([]string, error) {
	result, err := r.redis.ZRevRange(ctx.Ctx, key, start, stop).Result()
	if err != nil {
		ctx.Log.Errorf("ZRevRange error [key: %s][start: %d][stop: %d][error: %v]", key, start, stop, err)
		return nil, err
	}
	return result, nil
}

// ZRangeWithScores 获取有序集合指定范围内的成员和分数（按分数从低到高）
func (r Infra) ZRangeWithScores(ctx common.Ctx, key string, start, stop int64) ([]redis.Z, error) {
	result, err := r.redis.ZRangeWithScores(ctx.Ctx, key, start, stop).Result()
	if err != nil {
		ctx.Log.Errorf("ZRangeWithScores error [key: %s][start: %d][stop: %d][error: %v]", key, start, stop, err)
		return nil, err
	}
	return result, nil
}

// ZRevRangeWithScores 获取有序集合指定范围内的成员和分数（按分数从高到低）
func (r Infra) ZRevRangeWithScores(ctx common.Ctx, key string, start, stop int64) ([]redis.Z, error) {
	result, err := r.redis.ZRevRangeWithScores(ctx.Ctx, key, start, stop).Result()
	if err != nil {
		ctx.Log.Errorf("ZRevRangeWithScores error [key: %s][start: %d][stop: %d][error: %v]", key, start, stop, err)
		return nil, err
	}
	return result, nil
}

// ZRank 获取有序集合中成员的排名（按分数从低到高，排名从0开始）
func (r Infra) ZRank(ctx common.Ctx, key, member string) (int64, error) {
	result, err := r.redis.ZRank(ctx.Ctx, key, member).Result()
	if err != nil {
		if err == redis.Nil {
			return -1, nil
		}
		ctx.Log.Errorf("ZRank error [key: %s][member: %s][error: %v]", key, member, err)
		return -1, err
	}
	return result, nil
}

// ZRevRank 获取有序集合中成员的排名（按分数从高到低，排名从0开始）
func (r Infra) ZRevRank(ctx common.Ctx, key, member string) (int64, error) {
	result, err := r.redis.ZRevRank(ctx.Ctx, key, member).Result()
	if err != nil {
		if err == redis.Nil {
			return -1, nil
		}
		ctx.Log.Errorf("ZRevRank error [key: %s][member: %s][error: %v]", key, member, err)
		return -1, err
	}
	return result, nil
}

// ZScore 获取有序集合中成员的分数
func (r Infra) ZScore(ctx common.Ctx, key, member string) (float64, error) {
	result, err := r.redis.ZScore(ctx.Ctx, key, member).Result()
	if err != nil {
		if err == redis.Nil {
			return 0, nil
		}
		ctx.Log.Errorf("ZScore error [key: %s][member: %s][error: %v]", key, member, err)
		return 0, err
	}
	return result, nil
}

// ZCount 获取有序集合中指定分数范围内的成员数量
func (r Infra) ZCount(ctx common.Ctx, key, min, max string) (int64, error) {
	result, err := r.redis.ZCount(ctx.Ctx, key, min, max).Result()
	if err != nil {
		ctx.Log.Errorf("ZCount error [key: %s][min: %s][max: %s][error: %v]", key, min, max, err)
		return 0, err
	}
	return result, nil
}

// ZRemRangeByRank 移除有序集合中指定排名范围内的所有成员
func (r Infra) ZRemRangeByRank(ctx common.Ctx, key string, start, stop int64) (int64, error) {
	result, err := r.redis.ZRemRangeByRank(ctx.Ctx, key, start, stop).Result()
	if err != nil {
		ctx.Log.Errorf("ZRemRangeByRank error [key: %s][start: %d][stop: %d][error: %v]", key, start, stop, err)
		return 0, err
	}
	return result, nil
}
