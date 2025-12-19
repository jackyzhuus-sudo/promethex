package task

import (
	"fmt"
	"market-service/internal/biz/base"
	"market-service/internal/conf"
	"market-service/internal/pkg/common"
	"market-service/internal/pkg/util"
	"time"

	userBiz "market-service/internal/biz/user"

	"github.com/go-kratos/kratos/v2/errors"
	"github.com/go-kratos/kratos/v2/log"
)

type TaskRepoInterface interface {
	base.RepoInterface

	CreateTask(ctx common.Ctx, taskEntity *TaskEntity) error
	CreateUserTask(ctx common.Ctx, userTaskEntity *UserTaskEntity) error
	GetTask(ctx common.Ctx, query *TaskQuery) (*TaskEntity, error)
	GetTasks(ctx common.Ctx, query *TaskQuery) ([]*TaskEntity, error)
	GetTasksWithTotal(ctx common.Ctx, query *TaskQuery) ([]*TaskEntity, int64, error)
	GetUserTask(ctx common.Ctx, query *UserTaskQuery) (*UserTaskEntity, error)
	GetUserTasks(ctx common.Ctx, query *UserTaskQuery) ([]*UserTaskEntity, error)
	GetUserTasksWithTotal(ctx common.Ctx, query *UserTaskQuery) ([]*UserTaskEntity, int64, error)

	UpdateUserTask(ctx common.Ctx, uuid string, updateMap map[string]interface{}) error
}

type TaskHandler struct {
	taskRepo   TaskRepoInterface
	log        log.Logger
	confCustom *conf.Custom
}

func NewTaskHandler(taskRepo TaskRepoInterface, logger log.Logger, confCustom *conf.Custom) *TaskHandler {
	return &TaskHandler{taskRepo: taskRepo, log: logger, confCustom: confCustom}
}

func (h *TaskHandler) GetTaskInfo(ctx common.Ctx, query *TaskQuery) (*TaskEntity, error) {
	return h.taskRepo.GetTask(ctx, query)
}

func (h *TaskHandler) GetTasksWithTotal(ctx common.Ctx, query *TaskQuery) ([]*TaskEntity, int64, error) {
	return h.taskRepo.GetTasksWithTotal(ctx, query)
}

func (h *TaskHandler) GetUserTasks(ctx common.Ctx, query *UserTaskQuery) ([]*UserTaskEntity, error) {
	return h.taskRepo.GetUserTasks(ctx, query)
}

func (h *TaskHandler) GetUserTask(ctx common.Ctx, query *UserTaskQuery) (*UserTaskEntity, error) {
	return h.taskRepo.GetUserTask(ctx, query)
}

func (h *TaskHandler) CreateUserTask(ctx common.Ctx, userTaskEntity *UserTaskEntity) error {
	return h.taskRepo.CreateUserTask(ctx, userTaskEntity)
}

func (h *TaskHandler) GetUserTasksByTaskKeys(ctx common.Ctx, uid string, tasks []*TaskEntity) ([]*UserTaskEntity, error) {
	newUserTypeTaskKeys := make([]string, 0)
	dailyTypeTaskKeys := make([]string, 0)
	for _, task := range tasks {
		if task.Type == TaskTypeNewUser {
			newUserTypeTaskKeys = append(newUserTypeTaskKeys, task.Key)
		} else if task.Type == TaskTypeDaily {
			dailyTypeTaskKeys = append(dailyTypeTaskKeys, task.Key)
		}
	}
	userTaskEntities := make([]*UserTaskEntity, 0)
	if len(newUserTypeTaskKeys) > 0 {
		newUserTaskEntities, err := h.taskRepo.GetUserTasks(ctx, &UserTaskQuery{
			UID:      uid,
			TaskKeys: newUserTypeTaskKeys,
			BaseQuery: base.BaseQuery{
				Order: "id asc",
			},
		})
		if err != nil {
			return nil, err
		}
		userTaskEntities = append(userTaskEntities, newUserTaskEntities...)
	}

	if len(dailyTypeTaskKeys) > 0 {
		loc, _ := time.LoadLocation("Asia/Shanghai") // 加载UTC+8时区
		queryTime := time.Now().In(loc)
		startTime := time.Date(queryTime.Year(), queryTime.Month(), queryTime.Day(), 0, 0, 0, 0, loc).Format("2006-01-02 15:04:05.000 +0800")
		endTime := time.Date(queryTime.Year(), queryTime.Month(), queryTime.Day(), 23, 59, 59, 999999999, loc).Format("2006-01-02 15:04:05.000 +0800")
		ctx.Log.Infof("queryTime: %v, startTime: %v, endTime: %v", queryTime, startTime, endTime)
		dailyUserTaskEntities, err := h.taskRepo.GetUserTasks(ctx, &UserTaskQuery{
			UID:       uid,
			TaskKeys:  dailyTypeTaskKeys,
			StartTime: startTime,
			EndTime:   endTime,
			BaseQuery: base.BaseQuery{
				Order: "id asc",
			},
		})
		if err != nil {
			return nil, err
		}
		userTaskEntities = append(userTaskEntities, dailyUserTaskEntities...)
	}
	return userTaskEntities, nil
}

func (h *TaskHandler) ClaimTaskReward(ctx common.Ctx, uid string, taskKey string) (*userBiz.ClaimTaskRewardStreamMsg, error) {

	lockKey := fmt.Sprintf(TaskClaimLockKey, uid)
	lockID, ok, err := h.taskRepo.AcquireLock(ctx, lockKey, 5*time.Second)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New(1, "TASK_CLAIM_LOCK_FAILED", "task claim lock failed")
	}
	defer h.taskRepo.ReleaseLock(ctx, lockKey, lockID)

	taskEntity, err := h.GetTaskInfo(ctx, &TaskQuery{
		Key: taskKey,
	})
	if err != nil {
		return nil, err
	}
	if taskEntity == nil || taskEntity.UUID == "" {
		//
		return nil, errors.New(1, "TASK_NOT_FOUND", "task not found")
	}

	query := &UserTaskQuery{
		UID:     uid,
		TaskKey: taskKey,
	}
	if taskEntity.Type == TaskTypeDaily {
		loc, _ := time.LoadLocation("Asia/Shanghai") // 加载UTC+8时区
		queryTime := time.Now().In(loc)
		startTime := time.Date(queryTime.Year(), queryTime.Month(), queryTime.Day(), 0, 0, 0, 0, loc).Format("2006-01-02 15:04:05.000 +0800")
		endTime := time.Date(queryTime.Year(), queryTime.Month(), queryTime.Day(), 23, 59, 59, 999999999, loc).Format("2006-01-02 15:04:05.000 +0800")
		ctx.Log.Infof("queryTime: %v, startTime: %v, endTime: %v", queryTime, startTime, endTime)
		query.StartTime = startTime
		query.EndTime = endTime
	}
	userTaskEntity, err := h.taskRepo.GetUserTask(ctx, query)
	if err != nil {
		return nil, err
	}
	if userTaskEntity == nil || userTaskEntity.UUID == "" {
		return nil, errors.New(1, "USER_TASK_NOT_DONE", "user task not done")
	}

	if userTaskEntity.Claimed == UserTaskClaimedStatusClaimed {
		return nil, errors.New(1, "ALREADY_CLAIMED", "already claimed")
	}

	err = h.taskRepo.UpdateUserTask(ctx, userTaskEntity.UUID, map[string]interface{}{
		"claimed":    UserTaskClaimedStatusClaimed,
		"claimed_at": time.Now().Unix(),
	})
	if err != nil {
		return nil, err
	}

	msg := &userBiz.ClaimTaskRewardStreamMsg{
		UID:          uid,
		UserTaskUuid: userTaskEntity.UUID,
		TaskKey:      taskKey,
		Reward:       taskEntity.Reward,
		Timestamp:    uint64(time.Now().Unix()),
	}
	return msg, nil
}

func (h *TaskHandler) TaskDone(ctx common.Ctx, uid string, taskKey string) error {

	taskEntity, err := h.GetTaskInfo(ctx, &TaskQuery{
		Key: taskKey,
	})
	if err != nil {
		return err
	}
	if taskEntity == nil || taskEntity.UUID == "" {
		return errors.New(1, "TASK_NOT_FOUND", "task not found")
	}

	userTaskQuery := &UserTaskQuery{
		UID:     uid,
		TaskKey: taskKey,
	}
	if taskEntity.Type == TaskTypeDaily {
		loc, _ := time.LoadLocation("Asia/Shanghai") // 加载UTC+8时区
		queryTime := time.Now().In(loc)
		startTime := time.Date(queryTime.Year(), queryTime.Month(), queryTime.Day(), 0, 0, 0, 0, loc).Format("2006-01-02 15:04:05.000 +0800")
		endTime := time.Date(queryTime.Year(), queryTime.Month(), queryTime.Day(), 23, 59, 59, 999999999, loc).Format("2006-01-02 15:04:05.000 +0800")
		ctx.Log.Infof("queryTime: %v, startTime: %v, endTime: %v", queryTime, startTime, endTime)
		userTaskQuery.StartTime = startTime
		userTaskQuery.EndTime = endTime
	}
	userTaskEntity, err := h.taskRepo.GetUserTask(ctx, userTaskQuery)
	if err != nil {
		return err
	}
	if userTaskEntity != nil && userTaskEntity.UUID != "" {
		return nil
	}

	err = h.CreateUserTask(ctx, &UserTaskEntity{
		UUID:     util.GenerateUUID(),
		UID:      uid,
		TaskKey:  taskKey,
		TaskUUID: taskEntity.UUID,
		Reward:   taskEntity.Reward,
	})
	if err != nil {
		ctx.Log.Errorf("create user task failed, err: %v", err)
		return err
	}
	return nil
}
