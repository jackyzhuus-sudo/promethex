package data

import (
	"block-listener/internal/conf"
	"block-listener/internal/model"
	"context"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormLogger "gorm.io/gorm/logger"
)

type DbClient struct {
	db  *gorm.DB
	log *log.Helper
}

// NewDbClient 初始化数据库
func NewDbClient(bc *conf.Bootstrap, logger log.Logger) *DbClient {
	// 数据库链接
	conf := bc.Data.Db
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

	err = db.AutoMigrate()
	if err != nil {
		log.Fatal(err)
	}

	return &DbClient{
		db:  db,
		log: log.NewHelper(logger),
	}
}

func (d *DbClient) Create(ctx context.Context, model interface{}) error {
	return d.db.Create(model).Error
}

func (d *DbClient) BatchCreateEventLogs(ctx context.Context, eventLogs []*model.EventLog) error {
	// 批量创建事件日志
	db := d.db.WithContext(ctx)
	if err := db.CreateInBatches(eventLogs, 100).Error; err != nil {
		return err
	}
	return nil
}

func (d *DbClient) GetConfirmedWaitEventLogs(ctx context.Context, endBlockNumber uint64, limit int) ([]*model.EventLog, error) {
	db := d.db.WithContext(ctx)
	var eventLogs []*model.EventLog
	if err := db.Where("status = ?", model.EventLogStatusConfirmedWait).Where("block_number <= ?", endBlockNumber).Order("block_number ASC, tx_index ASC, log_index ASC").Limit(limit).Find(&eventLogs).Error; err != nil {
		return nil, err
	}
	return eventLogs, nil
}

func (d *DbClient) UpdateEventLogStatus(ctx context.Context, eventLog *model.EventLog) error {
	return d.db.WithContext(ctx).Model(&model.EventLog{}).Where("id = ?", eventLog.ID).Update("status", model.EventLogStatusConfirmed).Error
}

func (d *DbClient) UpdateEventLogsStatusSucc(ctx context.Context, ids []uint64) error {
	return d.db.WithContext(ctx).Model(&model.EventLog{}).Where("id IN ?", ids).Update("status", model.EventLogStatusConfirmed).Error
}

func (d *DbClient) UpdateEventLogsStatusFiltered(ctx context.Context, ids []uint64) error {
	return d.db.WithContext(ctx).Model(&model.EventLog{}).Where("id IN ?", ids).Update("status", model.EventLogStatusFiltered).Error
}

func (d *DbClient) UpdateEventLogsStatusFailed(ctx context.Context, ids []uint64) error {
	return d.db.WithContext(ctx).Model(&model.EventLog{}).Where("id IN ?", ids).Update("status", model.EventLogStatusFailed).Error
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
	traceId, _ := ctx.Value("traceId").(string)
	spanId, _ := ctx.Value("spanId").(string)
	l.logger.WithContext(ctx).Infof(msg, append([]interface{}{
		"trace_id", traceId,
		"span_id", spanId,
	}, data...)...)
}

func (l *GormLogger) Warn(ctx context.Context, msg string, data ...interface{}) {
	traceId, _ := ctx.Value("traceId").(string)
	spanId, _ := ctx.Value("spanId").(string)
	l.logger.WithContext(ctx).Warnf(msg, append([]interface{}{
		"trace_id", traceId,
		"span_id", spanId,
	}, data...)...)
}

func (l *GormLogger) Error(ctx context.Context, msg string, data ...interface{}) {
	traceId, _ := ctx.Value("traceId").(string)
	spanId, _ := ctx.Value("spanId").(string)
	l.logger.WithContext(ctx).Errorf(msg, append([]interface{}{
		"trace_id", traceId,
		"span_id", spanId,
	}, data...)...)
}

func (l *GormLogger) Trace(ctx context.Context, begin time.Time, fc func() (sql string, rowsAffected int64), err error) {
	elapsed := time.Since(begin)
	sql, rows := fc()

	traceId, _ := ctx.Value("traceId").(string)
	spanId, _ := ctx.Value("spanId").(string)

	l.logger.WithContext(ctx).Infow(
		"msg", "SQL",
		"trace_id", traceId,
		"span_id", spanId,
		"sql", sql,
		"rows", rows,
		"elapsed", elapsed,
	)
}
