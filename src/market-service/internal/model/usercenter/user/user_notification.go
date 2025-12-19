package user

import (
	"encoding/json"
	"market-service/internal/biz/base"
	"market-service/internal/biz/user"
	"market-service/internal/model"
)

type UserNotification struct {
	model.BaseModel
	UUID          string          `gorm:"column:uuid;type:varchar(36);uniqueIndex:idx_uuid;not null;comment:uuid"`
	UID           string          `gorm:"column:uid;type:varchar(24);not null;comment:用户uid"`
	Type          uint8           `gorm:"column:type;type:smallint;not null;default:0;comment:动态类型 1. 注册收到初始points 2. 完成任务收到points 3. 交易买卖 4. 市场结算 5. 领取结算结果 6. 收到评论/回复 7. post收到点赞 8. 被关注"`
	Category      uint8           `gorm:"column:category;type:smallint;not null;default:0;comment: 分类 1 交易 2 社区"`
	BizJson       json.RawMessage `gorm:"column:biz_json;type:jsonb;not null;comment:具体业务数据"`
	Status        uint8           `gorm:"column:status;type:smallint;not null;default:2;comment:状态 1: 已读 2: 未读"`
	BaseTokenType uint8           `gorm:"column:base_token_type;type:smallint;not null;default:0;comment:代币类型 1: points 2: usdc"`
}

// CREATE UNIQUE INDEX idx_user_post_like_notification ON t_user_notification (uid, (biz_json->>'post_uuid')) WHERE type = 7 AND status = 2;

func (u *UserNotification) TableName() string {
	return "t_user_notification"
}

func (u *UserNotification) ToEntity() *user.UserNotificationEntity {
	return &user.UserNotificationEntity{
		UUID:          u.UUID,
		UID:           u.UID,
		Type:          u.Type,
		BizJson:       u.BizJson,
		Status:        u.Status,
		Category:      u.Category,
		BaseTokenType: u.BaseTokenType,
		BaseEntity: base.BaseEntity{
			Id:        u.ID,
			CreatedAt: u.CreatedAt,
			UpdatedAt: u.UpdatedAt,
		},
	}
}

func (u *UserNotification) FromEntity(entity *user.UserNotificationEntity) {
	u.UUID = entity.UUID
	u.UID = entity.UID
	u.Type = entity.Type
	u.BizJson = entity.BizJson
	u.Category = entity.Category
	u.Status = entity.Status
	u.ID = entity.Id
	u.CreatedAt = entity.CreatedAt
	u.UpdatedAt = entity.UpdatedAt
	u.BaseTokenType = entity.BaseTokenType
}
