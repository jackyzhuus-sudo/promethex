package task_verify

import (
	"fmt"
	assetBiz "market-service/internal/biz/asset"
	"market-service/internal/biz/base"
	marketBiz "market-service/internal/biz/market"
	"market-service/internal/biz/task"
	taskBiz "market-service/internal/biz/task"
	userBiz "market-service/internal/biz/user"
	"market-service/internal/pkg/common"
	"market-service/internal/pkg/util"
	"time"
)

type TaskVerifier interface {
	Init(ctx common.Ctx,
		taskHandler *taskBiz.TaskHandler,
		userHandler *userBiz.UserHandler,
		marketHandler *marketBiz.MarketHandler,
		assetHandler *assetBiz.AssetHandler,
		uid string, taskKey string, timestamp int64,
	) error
	PreCheck(ctx common.Ctx) (bool, error)
	Verify(ctx common.Ctx) (bool, error)
	Record(ctx common.Ctx) error
}

func GetTaskVerifier(taskKey string) (TaskVerifier, error) {
	switch taskKey {
	case task.TaskKeyFirstTrade:
		return &FirstTradeTaskVerifier{}, nil
	case task.TaskKeyFirstShare:
		return &FirstShareTaskVerifier{}, nil
	case task.TaskKeyDailyFirstTrade:
		return &DailyFirstTradeTaskVerifier{}, nil
	case task.TaskKeyDailyFiveTrades:
		return &DailyFiveTradesTaskVerifier{}, nil
	case task.TaskKeyDailyFiveMarketTrades:
		return &DailyFiveMarketTradesTaskVerifier{}, nil
	default:
		return nil, fmt.Errorf("task key not supported, taskKey: %s", taskKey)
	}
}

type BasicTaskVerifier struct {
	Timestamp     int64
	TaskHandler   *taskBiz.TaskHandler
	UserHandler   *userBiz.UserHandler
	MarketHandler *marketBiz.MarketHandler
	AssetHandler  *assetBiz.AssetHandler

	UserEntity *userBiz.UserEntity
	TaskEntity *taskBiz.TaskEntity
}

func (verifier *BasicTaskVerifier) Init(ctx common.Ctx,
	taskHandler *taskBiz.TaskHandler,
	userHandler *userBiz.UserHandler,
	marketHandler *marketBiz.MarketHandler,
	assetHandler *assetBiz.AssetHandler,
	uid string, taskKey string, timestamp int64,
) error {

	verifier.TaskHandler = taskHandler
	verifier.UserHandler = userHandler
	verifier.MarketHandler = marketHandler
	verifier.AssetHandler = assetHandler
	verifier.Timestamp = timestamp

	userEntity, err := verifier.UserHandler.GetUserInfo(ctx, &userBiz.UserQuery{
		UID: uid,
	})
	if err != nil {
		return err
	}
	if userEntity == nil || userEntity.UID == "" {
		return fmt.Errorf("user not found, uid: %s", uid)
	}
	verifier.UserEntity = userEntity

	taskEntity, err := verifier.TaskHandler.GetTaskInfo(ctx, &taskBiz.TaskQuery{
		Key: taskKey,
	})
	if err != nil {
		return err
	}
	if taskEntity == nil || taskEntity.Key == "" {
		return fmt.Errorf("task not found, taskKey: %s", taskKey)
	}
	verifier.TaskEntity = taskEntity
	return nil
}

func (verifier *BasicTaskVerifier) PreCheck(ctx common.Ctx) (bool, error) {

	taskQuery := &taskBiz.UserTaskQuery{
		UID:     verifier.UserEntity.UID,
		TaskKey: verifier.TaskEntity.Key,
		BaseQuery: base.BaseQuery{
			Limit: 1,
		},
	}
	if verifier.TaskEntity.Type == taskBiz.TaskTypeDaily {
		loc, _ := time.LoadLocation("Asia/Shanghai") // 加载UTC+8时区
		queryTime := time.Now().In(loc)
		if verifier.Timestamp > 0 {
			queryTime = time.Unix(verifier.Timestamp, 0).In(loc)
		}
		startTime := time.Date(queryTime.Year(), queryTime.Month(), queryTime.Day(), 0, 0, 0, 0, loc).Format("2006-01-02 15:04:05.000 +0800")
		endTime := time.Date(queryTime.Year(), queryTime.Month(), queryTime.Day(), 23, 59, 59, 999999999, loc).Format("2006-01-02 15:04:05.000 +0800")
		ctx.Log.Infof("queryTime: %v, startTime: %v, endTime: %v", queryTime, startTime, endTime)
		taskQuery.StartTime = startTime
		taskQuery.EndTime = endTime
	}
	userTasks, err := verifier.TaskHandler.GetUserTasks(ctx, taskQuery)
	if err != nil {
		return false, err
	}
	if len(userTasks) > 0 {
		return true, nil
	}
	return false, nil
}

func (verifier *BasicTaskVerifier) Verify(ctx common.Ctx) (bool, error) {
	return false, nil
}

func (verifier *BasicTaskVerifier) Record(ctx common.Ctx) error {
	userTaskEntity := &taskBiz.UserTaskEntity{
		UUID:     util.GenerateUUID(),
		UID:      verifier.UserEntity.UID,
		TaskUUID: verifier.TaskEntity.UUID,
		TaskKey:  verifier.TaskEntity.Key,
		Reward:   verifier.TaskEntity.Reward,
	}
	return verifier.TaskHandler.CreateUserTask(ctx, userTaskEntity)
}
