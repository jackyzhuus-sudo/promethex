package user

import (
	"encoding/json"
	"market-service/internal/model"
)

// 暂时动态只有交易记录 动态表没用 先放着
type UserActivity struct {
	model.BaseModel
	UUID         string          `gorm:"column:uuid;type:varchar(36);uniqueIndex:idx_uuid;not null;comment:uuid"`
	UID          string          `gorm:"column:uid;type:varchar(24);not null;comment:用户uid"`
	ActivityType uint8           `gorm:"column:activity_type;type:smallint;not null;default:0;comment:动态类型 1买入 2卖出 3发布帖子 4发布评论 5点赞"`
	BizJson      json.RawMessage `gorm:"column:biz_json;type:jsonb;not null;comment:业务数据"`
}

func (u *UserActivity) TableName() string {
	return "t_user_activity"
}
