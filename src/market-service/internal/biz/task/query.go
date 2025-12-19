package task

import (
	"market-service/internal/biz/base"

	"gorm.io/gorm"
)

type TaskQuery struct {
	base.BaseQuery
	Key    string
	UUID   string
	IsShow uint8
	Status uint8
	Type   uint8
}

type UserTaskQuery struct {
	base.BaseQuery
	UUID      string
	UID       string
	TaskUUID  string
	TaskKey   string
	TaskKeys  []string
	Status    uint8
	Claimed   uint8
	TxHash    string
	StartTime string
	EndTime   string
}

func (query *TaskQuery) Condition(db *gorm.DB, total *int64) *gorm.DB {
	if query.Key != "" {
		db = db.Where("key = ?", query.Key)
	}
	if query.UUID != "" {
		db = db.Where("uuid = ?", query.UUID)
	}
	if query.IsShow != 0 {
		db = db.Where("is_show = ?", query.IsShow)
	}
	if query.Status != 0 {
		db = db.Where("status = ?", query.Status)
	}
	if query.Type != 0 {
		db = db.Where("type = ?", query.Type)
	}
	return db
}

func (query *UserTaskQuery) Condition(db *gorm.DB, total *int64) *gorm.DB {
	if query.UUID != "" {
		db = db.Where("uuid = ?", query.UUID)
	}
	if query.UID != "" {
		db = db.Where("uid = ?", query.UID)
	}
	if query.TaskUUID != "" {
		db = db.Where("task_uuid = ?", query.TaskUUID)
	}
	if query.TaskKey != "" {
		db = db.Where("task_key = ?", query.TaskKey)
	}
	if len(query.TaskKeys) > 0 {
		db = db.Where("task_key in ?", query.TaskKeys)
	}
	if query.Status != 0 {
		db = db.Where("status = ?", query.Status)
	}
	if query.Claimed != 0 {
		db = db.Where("claimed = ?", query.Claimed)
	}
	if query.TxHash != "" {
		db = db.Where("tx_hash = ?", query.TxHash)
	}
	if query.StartTime != "" {
		db = db.Where("created_at >= ?", query.StartTime)
	}
	if query.EndTime != "" {
		db = db.Where("created_at <= ?", query.EndTime)
	}
	return db
}
