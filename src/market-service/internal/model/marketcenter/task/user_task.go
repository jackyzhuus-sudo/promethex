package task

import (
	"market-service/internal/biz/base"
	taskBiz "market-service/internal/biz/task"
	"market-service/internal/model"
)

type UserTask struct {
	model.BaseModel
	UUID      string `gorm:"column:uuid;type:varchar(36);not null;" comment:"用户任务UUID"`
	UID       string `gorm:"column:uid;type:varchar(24);not null;" comment:"用户ID"`
	TaskUUID  string `gorm:"column:task_uuid;type:varchar(36);not null;" comment:"任务UUID"`
	TaskKey   string `gorm:"column:task_key;type:varchar(128);not null;" comment:"任务Key"`
	Reward    uint64 `gorm:"column:reward;type:bigint;not null;default:0" comment:"任务奖励"`
	Claimed   uint8  `gorm:"column:claimed;type:smallint;not null;default:2" comment:"是否领取 1: 已领取 2: 未领取"`
	ClaimedAt uint64 `gorm:"column:claimed_at;type:bigint;not null;default:0" comment:"领取时间"`
	// TxHash    string `gorm:"column:tx_hash;type:varchar(66);not null;default:''" comment:"mint points tx hash"`
	// Status    uint8  `gorm:"column:status;type:smallint;not null;default:0" comment:"1成功 2失败 3执行中"`
}

func (t *UserTask) TableName() string {
	return "t_user_task"
}

func (t *UserTask) ToEntity() *taskBiz.UserTaskEntity {
	return &taskBiz.UserTaskEntity{
		BaseEntity: base.BaseEntity{
			Id:        t.ID,
			CreatedAt: t.CreatedAt,
			UpdatedAt: t.UpdatedAt,
		},
		UUID:      t.UUID,
		UID:       t.UID,
		TaskUUID:  t.TaskUUID,
		TaskKey:   t.TaskKey,
		Reward:    t.Reward,
		Claimed:   t.Claimed,
		ClaimedAt: t.ClaimedAt,
	}
}

func (t *UserTask) FromEntity(entity *taskBiz.UserTaskEntity) {
	t.ID = entity.Id
	t.CreatedAt = entity.CreatedAt
	t.UpdatedAt = entity.UpdatedAt
	t.UUID = entity.UUID
	t.UID = entity.UID
	t.TaskUUID = entity.TaskUUID
	t.TaskKey = entity.TaskKey
	t.Reward = entity.Reward
	t.Claimed = entity.Claimed
	t.ClaimedAt = entity.ClaimedAt
}
