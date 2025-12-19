package community

import (
	"market-service/internal/biz/base"
	communityBiz "market-service/internal/biz/community"
	"market-service/internal/model"
)

// UserLike 用户点赞表
type UserLike struct {
	model.BaseModel
	UID         string `gorm:"column:uid;type:varchar(24);not null;uniqueIndex:idx_uid_content_type"`
	ContentUUID string `gorm:"column:content_uuid;type:varchar(36);not null;uniqueIndex:idx_uid_content_type" comment:"post 或 content的uuid"`
	Type        uint8  `gorm:"column:type;type:smallint;not null;default:0;uniqueIndex:idx_uid_content_type" comment:"喜欢的内容类型 1post 2comment"`
	Status      uint8  `gorm:"column:status;type:smallint;not null;default:0" comment:"状态 1正常 2删除"`
}

// TableName 表名
func (u *UserLike) TableName() string {
	return "t_user_like"
}

func (u *UserLike) ToEntity() *communityBiz.UserLikeEntity {
	return &communityBiz.UserLikeEntity{
		UID:         u.UID,
		ContentUUID: u.ContentUUID,
		Type:        u.Type,
		Status:      u.Status,
		BaseEntity: base.BaseEntity{
			Id:        u.ID,
			CreatedAt: u.CreatedAt,
			UpdatedAt: u.UpdatedAt,
		},
	}
}

func (u *UserLike) FromEntity(entity *communityBiz.UserLikeEntity) {
	u.UID = entity.UID
	u.ContentUUID = entity.ContentUUID
	u.Type = entity.Type
	u.Status = entity.Status
	u.ID = entity.Id
	u.CreatedAt = entity.CreatedAt
	u.UpdatedAt = entity.UpdatedAt
}
