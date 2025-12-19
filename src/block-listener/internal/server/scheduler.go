package server

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"block-listener/internal/biz"
	"block-listener/internal/data"
	"block-listener/pkg/alarm"
	"block-listener/pkg/common"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
)

// Task 定义定时任务接口
type Task interface {
	Name() string
	Run(ctx context.Context) error
	Timeout() time.Duration
	RedisLockTimeOut() time.Duration
}

// SchedulerServer 实现了Kratos的Server接口
type SchedulerServer struct {
	cron        *cron.Cron
	tasks       map[string]Task
	logger      log.Logger
	mu          sync.RWMutex
	redisClient *data.RedisClient
}

// NewSchedulerServer 创建定时任务服务
func NewSchedulerServer(logger log.Logger, queryPriceProcessor *biz.QueryPriceProcessor, redisClient *data.RedisClient) *SchedulerServer {
	logHelper := log.NewHelper(logger)
	srv := &SchedulerServer{
		cron:        cron.New(cron.WithSeconds(), cron.WithChain(CustomRecover(logHelper))),
		tasks:       make(map[string]Task),
		logger:      logger,
		redisClient: redisClient,
	}

	// 注册价格查询处理器任务
	// if err := srv.RegisterTask("0 */30 * * * *", queryPriceProcessor); err != nil {
	// 	log.Fatalf("register query price processor task failed, err: %v", err)
	// }

	logHelper.Infof("registered tasks: %+v", srv.ListTasks())
	return srv
}

// RegisterTask 注册定时任务
func (s *SchedulerServer) RegisterTask(spec string, task Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.tasks[task.Name()]; exists {
		return fmt.Errorf("task %s already exists", task.Name())
	}

	_, err := s.cron.AddFunc(spec, func() {

		ctx := context.Background()
		traceId := uuid.New().String()
		ctx = context.WithValue(ctx, "server", "SCHEDULER-"+task.Name())
		ctx = context.WithValue(ctx, "traceId", traceId)

		c := common.NewBaseCtx(ctx, s.logger)
		lockKey := fmt.Sprintf("scheduler_lock_%s", task.Name())
		lockID, ok, err := s.redisClient.AcquireLock(c.Ctx, lockKey, task.RedisLockTimeOut())
		if err != nil {
			alarm.Lark.Send(fmt.Sprintf("task %s redis acquire lock error, err: %v", task.Name(), err))
			c.Log.Errorf("acquire lock failed, err: %v", err)
			return
		}
		if !ok {
			return
		}
		defer s.redisClient.ReleaseLock(c.Ctx, lockKey, lockID)

		c.Log.Infof("task %s start", task.Name())
		startTime := time.Now()
		if err := task.Run(ctx); err != nil {
			c.Log.Errorf("task %s failed, err: %v", task.Name(), err)
			alarm.Lark.Send(fmt.Sprintf("task %s failed,traceId: %s, err: %v", task.Name(), traceId, err))
		}

		c.Log.Infof("task %s done, cost: %v", task.Name(), time.Since(startTime))
	})

	if err != nil {
		return fmt.Errorf("add task %s failed, err: %v", task.Name(), err)
	}

	s.tasks[task.Name()] = task
	return nil
}

// Start 启动定时任务服务
func (s *SchedulerServer) Start(ctx context.Context) error {
	logHelper := log.NewHelper(s.logger)
	logHelper.Infof("scheduler server start")
	s.cron.Start()
	return nil
}

// Stop 停止定时任务服务
func (s *SchedulerServer) Stop(ctx context.Context) error {
	logHelper := log.NewHelper(s.logger)
	logHelper.Infof("scheduler server stop")
	s.cron.Stop()
	return nil
}

// ListTasks 列出所有已注册的任务
func (s *SchedulerServer) ListTasks() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tasks := make([]string, 0, len(s.tasks))
	for name := range s.tasks {
		tasks = append(tasks, name)
	}
	return tasks
}

func CustomRecover(logger *log.Helper) cron.JobWrapper {
	return func(j cron.Job) cron.Job {
		return cron.FuncJob(func() {
			defer func() {
				if r := recover(); r != nil {
					// 获取堆栈信息
					const size = 64 << 10
					buf := make([]byte, size)
					buf = buf[:runtime.Stack(buf, false)]

					// 转换panic值为error
					err, ok := r.(error)
					if !ok {
						err = fmt.Errorf("%v", r)
					}

					// 记录错误日志
					logger.Errorf("cron job panic: %v\n%s", err, buf)

					// 发送告警
					alarmMsg := fmt.Sprintf("cron jobs panic: %v\n stack: %s", err, string(buf[:1000])) // 截取堆栈前1000字符
					alarm.Lark.Send(alarmMsg)

					// 这里可以添加更多自定义恢复行为
					// 例如: 重试任务、记录到特定数据库等

					time.Sleep(1 * time.Second)
				}
			}()

			// 执行原始任务
			j.Run()
		})
	}
}
