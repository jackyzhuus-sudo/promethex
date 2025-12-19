package server

import (
	"context"
	"fmt"
	"market-service/internal/crontask"
	"market-service/internal/data/base"
	"market-service/internal/pkg/alarm"
	"market-service/internal/pkg/common"
	"runtime"
	"sync"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
)

// Task 定义定时任务接口
type Task interface {
	Name() string
	Run(ctx context.Context) error
	Timeout() time.Duration
}

// SchedulerServer 实现了Kratos的Server接口
type SchedulerServer struct {
	cron            *cron.Cron
	tasks           map[string]Task
	logger          log.Logger
	mu              sync.RWMutex
	usercenterInfra base.UsercenterInfra
}

// NewSchedulerServer 创建定时任务服务
func NewSchedulerServer(logger log.Logger, updateUserAssetProcessor *crontask.UpdateUserAssetProcessor, usercenterInfra base.UsercenterInfra) *SchedulerServer {
	srv := &SchedulerServer{
		cron:            cron.New(cron.WithSeconds(), cron.WithChain(CustomRecover(log.NewHelper(logger)))),
		tasks:           make(map[string]Task),
		logger:          logger,
		usercenterInfra: usercenterInfra,
	}

	// 每天0点和12点更新用户资产
	if err := srv.RegisterTask("0 0 0,12 * * *", updateUserAssetProcessor); err != nil {
		log.Fatalf("register update user asset processor task failed, err: %v", err)
	}

	log.NewHelper(logger).Infof("registered tasks: %+v", srv.ListTasks())
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

		ctx, cancel := context.WithTimeout(context.Background(), task.Timeout())
		traceId := uuid.New().String()
		ctx = context.WithValue(ctx, "server", "SCHEDULER-"+task.Name())
		ctx = context.WithValue(ctx, "traceId", traceId)
		defer cancel()

		c := common.NewBaseCtx(ctx, s.logger)

		lockKey := fmt.Sprintf("scheduler_lock_%s", task.Name())
		lockID, ok, err := s.usercenterInfra.AcquireLock(c, lockKey, task.Timeout())
		if err != nil {
			c.Log.Errorf("acquire lock failed, err: %v", err)
			return
		}
		if !ok {
			return
		}
		defer s.usercenterInfra.ReleaseLock(c, lockKey, lockID)

		c.Log.Infof("task %s start", task.Name())
		startTime := time.Now()
		if err := task.Run(ctx); err != nil {
			c.Log.Errorf("task %s failed, err: %v", task.Name(), err)
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
	log.NewHelper(s.logger).Infof("scheduler server start")
	s.cron.Start()
	return nil
}

// Stop 停止定时任务服务
func (s *SchedulerServer) Stop(ctx context.Context) error {
	log.NewHelper(s.logger).Infof("scheduler server stop")
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
