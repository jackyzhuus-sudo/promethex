package data

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"block-listener/internal/conf"
	"block-listener/pkg/common"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
)

const (
	// Redis中存储最后处理区块高度的键
	lastProcessedBlockKey = "block-listener:last-processed-block"
)

// RedisClient 是Redis客户端封装
type RedisClient struct {
	client *redis.Client
	log    *log.Helper
}

// NewRedisClient 创建新的Redis客户端
func NewRedisClient(bc *conf.Bootstrap, logger log.Logger) (*RedisClient, error) {
	log := log.NewHelper(logger)
	log.Infof("初始化Redis客户端: %s", bc.Data.Redis.Addr)

	client := redis.NewClient(&redis.Options{
		Addr:         bc.Data.Redis.Addr,
		Password:     bc.Data.Redis.Password,
		DB:           int(bc.Data.Redis.Db),
		ReadTimeout:  bc.Data.Redis.ReadTimeout.AsDuration(),
		WriteTimeout: bc.Data.Redis.WriteTimeout.AsDuration(),
	})

	// 测试连接，添加超时
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if _, err := client.Ping(ctx).Result(); err != nil {
		log.Errorf("Redis连接失败: %v", err)
		return nil, fmt.Errorf("连接Redis失败: %v", err)
	}
	// hook := NewRedisHook(logger)
	// client.AddHook(hook)
	log.Info("Redis连接成功")
	return &RedisClient{
		client: client,
		log:    log,
	}, nil
}

// GetLastProcessedBlock 获取最后处理的区块高度
func (r *RedisClient) GetLastProcessedBlock(ctx context.Context) (uint64, error) {
	val, err := r.client.Get(ctx, lastProcessedBlockKey).Result()
	if err == redis.Nil {
		return 0, nil // 键不存在，返回0
	}
	if err != nil {
		return 0, err
	}

	return strconv.ParseUint(val, 10, 64)
}

// UpdateLastProcessedBlock 更新最后处理的区块高度
func (r *RedisClient) UpdateLastProcessedBlock(ctx context.Context, blockNum uint64) error {
	return r.client.Set(ctx, lastProcessedBlockKey, blockNum, 0).Err()
}

// GetOptionTokenAddress 获取选项代币地址
func (r *RedisClient) GetOptionTokenAddress(ctx context.Context, marketAddr string, optionIndex uint64) (string, error) {
	return r.client.Get(ctx, fmt.Sprintf("option-%s-%d", marketAddr, optionIndex)).Result()
}

// GetOptionTokenAddresses 批量获取选项代币地址
func (r *RedisClient) MGetOptionTokenAddresses(ctx context.Context, keys []string) ([]string, error) {
	vals, err := r.client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}

	valsStr := make([]string, len(vals))
	for i, val := range vals {
		if val == nil {
			valsStr[i] = ""
		} else {
			valsStr[i] = val.(string)
		}
	}

	return valsStr, nil
}

func (r *RedisClient) AcquireLock(ctx context.Context, lockKey string, expiration time.Duration) (string, bool, error) {
	lockID := uuid.New().String()
	result, err := r.client.SetNX(ctx, lockKey, lockID, expiration).Result()
	if err != nil {
		return "", false, err
	}

	return lockID, result, nil
}

func (r *RedisClient) ReleaseLock(ctx context.Context, lockKey string, lockID string) error {
	luaScript := `
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("DEL", KEYS[1])
		else
			return 0
		end
	`
	result, err := r.client.Eval(ctx, luaScript, []string{lockKey}, lockID).Result()
	if err != nil {
		return err
	}

	if result == int64(0) {
		return errors.New("release lock failed")
	}
	return nil
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
