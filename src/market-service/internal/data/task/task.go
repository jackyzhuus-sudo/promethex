package task

import (
	taskBiz "market-service/internal/biz/task"
	"market-service/internal/data/base"
	taskModel "market-service/internal/model/marketcenter/task"
	"market-service/internal/pkg/common"

	"gorm.io/gorm"
)

type taskRepo struct {
	base.UsercenterInfra
}

// NewMarketRepo .
func NewTaskRepo(infra base.UsercenterInfra) taskBiz.TaskRepoInterface {
	return &taskRepo{
		infra,
	}
}

func (r *taskRepo) CreateTask(ctx common.Ctx, taskEntity *taskBiz.TaskEntity) error {
	taskModel := &taskModel.Task{}
	taskModel.FromEntity(taskEntity)
	return r.Create(ctx, taskModel)
}

func (r *taskRepo) CreateUserTask(ctx common.Ctx, userTaskEntity *taskBiz.UserTaskEntity) error {
	userTaskModel := &taskModel.UserTask{}
	userTaskModel.FromEntity(userTaskEntity)
	return r.Create(ctx, userTaskModel)
}

func (r *taskRepo) GetTask(ctx common.Ctx, query *taskBiz.TaskQuery) (*taskBiz.TaskEntity, error) {
	db := query.Condition(r.GetDb().WithContext(ctx.Ctx).Model(&taskModel.Task{}), nil)
	task := &taskModel.Task{}
	if err := db.First(&task).Error; err != nil && err != gorm.ErrRecordNotFound {
		ctx.Log.Errorf("GetTask sql failed, err: %v", err)
		return nil, err
	}
	return task.ToEntity(), nil
}

func (r *taskRepo) GetTasks(ctx common.Ctx, query *taskBiz.TaskQuery) ([]*taskBiz.TaskEntity, error) {
	db := query.Condition(r.GetDb().WithContext(ctx.Ctx).Model(&taskModel.Task{}), nil)
	var tasks []*taskModel.Task
	if err := db.Find(&tasks).Error; err != nil {
		ctx.Log.Errorf("GetTasks sql failed, err: %v", err)
		return nil, err
	}
	taskEntities := make([]*taskBiz.TaskEntity, 0)
	for _, task := range tasks {
		taskEntities = append(taskEntities, task.ToEntity())
	}
	return taskEntities, nil
}

func (r *taskRepo) GetTasksWithTotal(ctx common.Ctx, query *taskBiz.TaskQuery) ([]*taskBiz.TaskEntity, int64, error) {
	var total int64
	db := query.Condition(r.GetDb().WithContext(ctx.Ctx).Model(&taskModel.Task{}), &total)
	var tasks []*taskModel.Task
	if err := db.Find(&tasks).Error; err != nil {
		ctx.Log.Errorf("GetTasksWithTotal sql failed, err: %v", err)
		return nil, 0, err
	}
	taskEntities := make([]*taskBiz.TaskEntity, 0)
	for _, task := range tasks {
		taskEntities = append(taskEntities, task.ToEntity())
	}
	return taskEntities, total, nil
}

func (r *taskRepo) GetUserTask(ctx common.Ctx, query *taskBiz.UserTaskQuery) (*taskBiz.UserTaskEntity, error) {
	db := query.Condition(r.GetDb().WithContext(ctx.Ctx).Model(&taskModel.UserTask{}), nil)
	userTask := &taskModel.UserTask{}
	if err := db.First(&userTask).Error; err != nil && err != gorm.ErrRecordNotFound {
		ctx.Log.Errorf("GetUserTask sql failed, err: %v", err)
		return nil, err
	}
	return userTask.ToEntity(), nil
}

func (r *taskRepo) GetUserTasks(ctx common.Ctx, query *taskBiz.UserTaskQuery) ([]*taskBiz.UserTaskEntity, error) {
	db := query.Condition(r.GetDb().WithContext(ctx.Ctx).Model(&taskModel.UserTask{}), nil)
	var userTasks []*taskModel.UserTask
	if err := db.Find(&userTasks).Error; err != nil {
		ctx.Log.Errorf("GetUserTasks sql failed, err: %v", err)
		return nil, err
	}
	userTaskEntities := make([]*taskBiz.UserTaskEntity, 0)
	for _, userTask := range userTasks {
		userTaskEntities = append(userTaskEntities, userTask.ToEntity())
	}
	return userTaskEntities, nil
}

func (r *taskRepo) GetUserTasksWithTotal(ctx common.Ctx, query *taskBiz.UserTaskQuery) ([]*taskBiz.UserTaskEntity, int64, error) {
	var total int64
	db := query.Condition(r.GetDb().WithContext(ctx.Ctx).Model(&taskModel.UserTask{}), &total)
	var userTasks []*taskModel.UserTask
	if err := db.Find(&userTasks).Error; err != nil {
		ctx.Log.Errorf("GetUserTasksWithTotal sql failed, err: %v", err)
		return nil, 0, err
	}
	userTaskEntities := make([]*taskBiz.UserTaskEntity, 0)
	for _, userTask := range userTasks {
		userTaskEntities = append(userTaskEntities, userTask.ToEntity())
	}
	return userTaskEntities, total, nil
}

func (r *taskRepo) UpdateUserTask(ctx common.Ctx, uuid string, updateMap map[string]interface{}) error {
	db := common.GetDB(ctx.Ctx, r.GetDb()).Model(&taskModel.UserTask{}).Where("uuid = ?", uuid).Updates(updateMap)
	if db.Error != nil {
		ctx.Log.Errorf("UpdateUserTask sql failed, err: %v", db.Error)
		return db.Error
	}
	return nil
}
