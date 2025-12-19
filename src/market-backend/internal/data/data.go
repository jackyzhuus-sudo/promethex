package data

import (
	"context"
	"encoding/json"
	"market-backend/internal/conf"
	"net/http"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/google/wire"
	"github.com/pkg/errors"
)

var ProviderSet = wire.NewSet(NewData, NewRpcClient)

type Data struct {
	RpcClient    *RpcClient
	RedisClient  *redis.Client
	PrivyClient  *PrivyClient
	OpenAIClient *OpenAI
}

type PrivyClient struct {
	HttpClient      http.Client
	VerificationKey string
	AppId           string
	AppSecret       string
}

func NewData(cfgData *conf.Data, custom *conf.Custom, logger log.Logger) *Data {
	rpcClient, err := NewRpcClient(cfgData, logger)
	if err != nil {
		log.Fatalf("failed to create rpc client error: %v", err)
	}
	redisClient := newRedisandConnect(cfgData.Redis, logger)
	privyClient := newPrivyClient(cfgData.Privy)
	openaiClient := newOpenAIClient(cfgData.Openai)
	return &Data{RpcClient: rpcClient, RedisClient: redisClient, PrivyClient: privyClient, OpenAIClient: openaiClient}
}

func newPrivyClient(cfgPrivy *conf.Data_Privy) *PrivyClient {
	return &PrivyClient{
		VerificationKey: cfgPrivy.VerificationKey,
		AppId:           cfgPrivy.AppId,
		AppSecret:       cfgPrivy.AppSecret,
		HttpClient: http.Client{
			Timeout: 10 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        10,
				MaxIdleConnsPerHost: 5,
				MaxConnsPerHost:     10,
				IdleConnTimeout:     90 * time.Second,
				TLSHandshakeTimeout: 2 * time.Second,
			},
		},
	}
}

// newOpenAIClient 创建 OpenAI 客户端实例
func newOpenAIClient(cfgOpenAI *conf.Data_OpenAI) *OpenAI {
	return newOpenAiWithBase(cfgOpenAI)
}

// newRedisandConnect 初始化redis
func newRedisandConnect(conf *conf.Data_Redis, logger log.Logger) *redis.Client {
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

func (r *Data) AcquireLock(ctx context.Context, lockKey string, expiration time.Duration) (string, bool, error) {
	lockID := uuid.New().String()
	result, err := r.RedisClient.SetNX(ctx, lockKey, lockID, expiration).Result()
	if err != nil {
		return "", false, err
	}

	return lockID, result, nil
}

func (r *Data) ReleaseLock(ctx context.Context, lockKey string, lockID string) error {
	luaScript := `
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("DEL", KEYS[1])
		else
			return 0
		end
	`
	result, err := r.RedisClient.Eval(ctx, luaScript, []string{lockKey}, lockID).Result()
	if err != nil {
		return err
	}

	if result == int64(0) {
		return errors.New("release lock failed")
	}
	return nil
}

// SetJSON 设置JSON格式的缓存
func (d *Data) SetJSONToCache(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	jsonData, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return d.RedisClient.Set(ctx, key, jsonData, expiration).Err()
}

// GetJSON 获取JSON格式的缓存并反序列化
func (d *Data) GetJSONFromCache(ctx context.Context, key string, dest interface{}) error {
	result, err := d.RedisClient.Get(ctx, key).Result()
	if err == redis.Nil {
		return nil // 缓存不存在
	}
	if err != nil {
		return err
	}
	return json.Unmarshal([]byte(result), dest)
}
