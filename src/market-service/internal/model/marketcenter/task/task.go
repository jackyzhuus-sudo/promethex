package task

import (
	"market-service/internal/biz/base"
	taskBiz "market-service/internal/biz/task"
	"market-service/internal/model"
)

type Task struct {
	model.BaseModel
	UUID        string `gorm:"column:uuid;type:varchar(36);not null;" comment:"任务UUID"`
	Key         string `gorm:"column:key;type:varchar(128);not null;" comment:"任务Key"`
	IsShow      uint8  `gorm:"column:is_show;type:smallint;not null;default:1" comment:"是否展示 1: 展示 2: 不展示"`
	Type        uint8  `gorm:"column:type;type:smallint;not null;default:1" comment:"任务类型 1: 新手任务（一次性） 2: 每日任务"`
	Name        string `gorm:"column:name;type:varchar(512);not null;default:''" comment:"任务名称"`
	Description string `gorm:"column:description;type:varchar(1024);not null;default:''" comment:"任务描述"`
	PicUrl      string `gorm:"column:pic_url;type:varchar(512);not null;default:''" comment:"任务图片URL"`
	Reward      uint64 `gorm:"column:reward;type:bigint;not null;default:0" comment:"任务奖励"`
	JumpUrl     string `gorm:"column:jump_url;type:varchar(512);not null;default:''" comment:"任务跳转URL"`
}

func (t *Task) TableName() string {
	return "t_task"
}

func (t *Task) ToEntity() *taskBiz.TaskEntity {
	return &taskBiz.TaskEntity{
		BaseEntity: base.BaseEntity{
			Id:        t.ID,
			CreatedAt: t.CreatedAt,
			UpdatedAt: t.UpdatedAt,
		},
		UUID:        t.UUID,
		Key:         t.Key,
		IsShow:      t.IsShow,
		Type:        t.Type,
		Name:        t.Name,
		Description: t.Description,
		PicUrl:      t.PicUrl,
		Reward:      t.Reward,
		JumpUrl:     t.JumpUrl,
	}
}

func (t *Task) FromEntity(entity *taskBiz.TaskEntity) {
	t.UUID = entity.UUID
	t.Key = entity.Key
	t.IsShow = entity.IsShow
	t.Type = entity.Type
	t.Name = entity.Name
	t.Description = entity.Description
	t.PicUrl = entity.PicUrl
	t.Reward = entity.Reward
	t.JumpUrl = entity.JumpUrl
	t.ID = entity.Id
	t.CreatedAt = entity.CreatedAt
	t.UpdatedAt = entity.UpdatedAt
}
