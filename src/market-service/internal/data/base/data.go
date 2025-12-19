package base

import (
	"market-service/internal/conf"

	"github.com/go-kratos/kratos/v2/log"
)

// Data .
type Data struct {
	Db               *Db
	Redis            *Redis
	AlchemyHttpCliet *AlchemyClient
	S3Cli            *S3Client
	Rpc              *RpcClient
}

// NewData
func NewData(c *conf.Data, logger log.Logger) (*Data, func(), error) {
	db := newPostgresql(c, logger)
	redis := newRedis(c, logger)
	alchemyClient := newAlchemyClient(c)
	s3Client := newS3Client(c)
	rpcClient := newRpcClient(c)

	cleanup := func() {
		log.NewHelper(logger).Info("closing the data resources")
		closePostgresql(db.Usercenter)
		closePostgresql(db.Marketcenter)
		closeRedis(redis.Usercenter)
		closeRedis(redis.Marketcenter)
	}

	return &Data{
		Db:               db,
		Redis:            redis,
		AlchemyHttpCliet: alchemyClient,
		S3Cli:            s3Client,
		Rpc:              rpcClient,
	}, cleanup, nil
}
