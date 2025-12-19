package base

import (
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-redis/redis/v8"
	"gorm.io/gorm"
)

// 封装所有底层对db redis mq rpc的最小操作
type Infra struct {
	db            *gorm.DB
	redis         *redis.Client
	alchemyClient *AlchemyClient
	s3            *S3Client
	rpcClient     *RpcClient
	// mq
	// rpc client
}

type UsercenterInfra struct {
	Infra
	Log *log.Helper
}

type MarketcenterInfra struct {
	Infra
	Log *log.Helper
}

func newInfra(db *gorm.DB, redis *redis.Client, alchemyClient *AlchemyClient, s3 *S3Client, rpcClient *RpcClient) Infra {
	return Infra{
		db:            db,
		redis:         redis,
		alchemyClient: alchemyClient,
		s3:            s3,
		rpcClient:     rpcClient,
	}
}

func NewUsercenterInfra(data *Data, logger log.Logger) UsercenterInfra {
	return UsercenterInfra{
		newInfra(data.Db.Usercenter, data.Redis.Usercenter, data.AlchemyHttpCliet, data.S3Cli, data.Rpc),
		log.NewHelper(logger),
	}
}

func NewMarketcenterInfra(data *Data, logger log.Logger) MarketcenterInfra {
	return MarketcenterInfra{
		newInfra(data.Db.Marketcenter, data.Redis.Marketcenter, data.AlchemyHttpCliet, data.S3Cli, data.Rpc),
		log.NewHelper(logger),
	}
}

// 获取器
func (r Infra) GetDb() *gorm.DB {
	return r.db
}

func (r Infra) GetRedis() *redis.Client {
	return r.redis
}

func (r Infra) GetAlchemyClient() *AlchemyClient {
	return r.alchemyClient
}

func (r Infra) GetS3() *S3Client {
	return r.s3
}

func (r Infra) GetRpcClient() *RpcClient {
	return r.rpcClient
}
