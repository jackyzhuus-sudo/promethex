package user

import (
	"market-service/internal/biz/base"
	userBiz "market-service/internal/biz/user"
	"market-service/internal/model"
)

type UserFollow struct {
	model.BaseModel
	UID       string `gorm:"column:uid;type:varchar(24);not null;uniqueIndex:idx_uid_follow_uid"`
	FollowUID string `gorm:"column:follow_uid;type:varchar(24);not null;uniqueIndex:idx_uid_follow_uid"`
	Status    uint8  `gorm:"column:status;type:smallint;not null;default:0" comment:"状态 1正常 2删除"`
}

// TableName 表名
func (u *UserFollow) TableName() string {
	return "t_user_follow"
}

func (u *UserFollow) ToEntity() *userBiz.UserFollowEntity {
	return &userBiz.UserFollowEntity{
		UID:       u.UID,
		FollowUID: u.FollowUID,
		Status:    u.Status,
		BaseEntity: base.BaseEntity{
			Id:        u.ID,
			CreatedAt: u.CreatedAt,
			UpdatedAt: u.UpdatedAt,
		},
	}
}

func (u *UserFollow) FromEntity(entity *userBiz.UserFollowEntity) {
	u.UID = entity.UID
	u.FollowUID = entity.FollowUID
	u.Status = entity.Status
	u.ID = entity.Id
	u.CreatedAt = entity.CreatedAt
	u.UpdatedAt = entity.UpdatedAt
}
