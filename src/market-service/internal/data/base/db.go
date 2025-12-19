package base

import (
	"context"
	"market-service/internal/conf"
	"market-service/internal/pkg/common"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormLogger "gorm.io/gorm/logger"
)

type Db struct {
	Usercenter   *gorm.DB
	Marketcenter *gorm.DB
}

// 数据库相关函数
func newPostgresql(c *conf.Data, logger log.Logger) *Db {
	usercenter := newPostgresqlAndConnect(c.Postgresql.Usercenter, logger)
	marketcenter := newPostgresqlAndConnect(c.Postgresql.Marketcenter, logger)

	return &Db{
		Usercenter:   usercenter,
		Marketcenter: marketcenter,
	}
}

func newPostgresqlAndConnect(conf *conf.Data_PostgresqlData_Postgresql, logger log.Logger) *gorm.DB {
	// 数据库链接
	db, err := gorm.Open(
		postgres.Open(conf.Dsn),
		&gorm.Config{
			Logger: NewGormLogger(logger).LogMode(gormLogger.Info),
		})
	if err != nil {
		log.Fatal(err)
	}

	sqlDb, err := db.DB()
	if err != nil {
		log.Fatal(err)
	}
	// 设置连接池
	// 空闲
	sqlDb.SetMaxIdleConns(int(conf.MaxIdleConns))
	// 打开
	sqlDb.SetMaxOpenConns(int(conf.MaxOpenConns))
	// 超时
	sqlDb.SetConnMaxLifetime(conf.ConnMaxLifetime.AsDuration())

	return db
}

func closePostgresql(db *gorm.DB) {
	sqlDb, err := db.DB()
	if err != nil {
		log.Fatal(err)
	}
	sqlDb.Close()
}

type GormLogger struct {
	logger *log.Helper
}

func NewGormLogger(logger log.Logger) *GormLogger {
	return &GormLogger{
		logger: log.NewHelper(logger),
	}
}

func (l *GormLogger) LogMode(level gormLogger.LogLevel) gormLogger.Interface {
	return l
}

func (l *GormLogger) Info(ctx context.Context, msg string, data ...interface{}) {
	server, _ := ctx.Value("server").(string)
	traceId, _ := ctx.Value("traceId").(string)
	spanId, _ := ctx.Value("spanId").(string)
	l.logger.WithContext(ctx).Infof(msg, append([]interface{}{
		"server", server,
		"trace_id", traceId,
		"span_id", spanId,
	}, data...)...)
}

func (l *GormLogger) Warn(ctx context.Context, msg string, data ...interface{}) {
	server, _ := ctx.Value("server").(string)
	traceId, _ := ctx.Value("traceId").(string)
	spanId, _ := ctx.Value("spanId").(string)
	l.logger.WithContext(ctx).Warnf(msg, append([]interface{}{
		"server", server,
		"trace_id", traceId,
		"span_id", spanId,
	}, data...)...)
}

func (l *GormLogger) Error(ctx context.Context, msg string, data ...interface{}) {
	server, _ := ctx.Value("server").(string)
	traceId, _ := ctx.Value("traceId").(string)
	spanId, _ := ctx.Value("spanId").(string)
	l.logger.WithContext(ctx).Errorf(msg, append([]interface{}{
		"server", server,
		"trace_id", traceId,
		"span_id", spanId,
	}, data...)...)
}

func (l *GormLogger) Trace(ctx context.Context, begin time.Time, fc func() (sql string, rowsAffected int64), err error) {
	elapsed := time.Since(begin)
	sql, rows := fc()

	traceId, _ := ctx.Value("traceId").(string)
	spanId, _ := ctx.Value("spanId").(string)
	server, _ := ctx.Value("server").(string)
	l.logger.WithContext(ctx).Infow(
		"msg", "SQL",
		"server", server,
		"trace_id", traceId,
		"span_id", spanId,
		"sql", sql,
		"rows", rows,
		"elapsed", elapsed,
	)
}

// 公共CRUD方法
// Create 公共的添加数据方法
func (r Infra) Create(ctx common.Ctx, model interface{}) error {
	err := common.GetDB(ctx.Ctx, r.GetDb()).Create(model).Error
	if err != nil {
		ctx.Log.Errorf("common create error [data: %v][error: %v]", model, err)
		return err
	}
	return nil
}

// Modify 公共的更新数据方法，注意需要有primarykey
func (r Infra) Modify(ctx common.Ctx, model interface{}) (err error) {
	err = common.GetDB(ctx.Ctx, r.GetDb()).Model(model).Updates(model).Error
	if err != nil {
		ctx.Log.Errorf("modify error [data: %v][error: %+v]", model, err)
		return err
	}
	return nil
}

func (r Infra) ModifyByMap(ctx common.Ctx, model interface{}, updateMap map[string]interface{}) (err error) {
	db := common.GetDB(ctx.Ctx, r.GetDb())
	err = db.Model(model).Updates(updateMap).Error
	if err != nil {
		ctx.Log.Errorf("ModifyByMap error [model: %v][updatemap: %v][error: %+v]", model, updateMap, err)
		return err
	}
	return nil
}

// Delete 公共的delete方法，注意需要有primarykey
func (r Infra) Delete(ctx common.Ctx, model interface{}) (err error) {
	err = common.GetDB(ctx.Ctx, r.GetDb()).Delete(model).Error
	return err
}

// 事务相关
func (r Infra) ExecTx(ctx common.Ctx, fn func(ctx common.Ctx, db *gorm.DB) error) error {
	tx := common.GetDB(ctx.Ctx, r.GetDb()).Begin()
	if tx.Error != nil {
		return tx.Error
	}

	err := fn(ctx, tx)
	if err != nil {
		ctx.Log.Errorf("exectx error: [%+v]", err)
		tx.Rollback()
		return err
	}

	return tx.Commit().Error
}
