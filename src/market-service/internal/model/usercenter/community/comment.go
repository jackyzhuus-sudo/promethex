package community

import (
	"market-service/internal/biz/base"
	"market-service/internal/biz/community"
	"market-service/internal/model"
)

// Comment 评论表
type Comment struct {
	model.BaseModel
	UUID          string `gorm:"column:uuid;type:varchar(36);uniqueIndex:idx_uuid;not null;"`
	UID           string `gorm:"column:uid;type:varchar(24);not null;default:''"`
	MarketAddress string `gorm:"column:market_address;type:varchar(42);not null;"`
	PostUUID      string `gorm:"column:post_uuid;type:varchar(36);not null;"`
	RootUUID      string `gorm:"column:root_uuid;type:varchar(36);not null;default:''"`
	ParentUUID    string `gorm:"column:parent_uuid;type:varchar(36);not null;default:''"`
	ParentUserUID string `gorm:"column:parent_user_uid;type:varchar(24);not null;default:''"`
	Content       string `gorm:"column:content;type:text;not null;default:''"`
	Status        uint8  `gorm:"column:status;type:smallint;not null;default:1"` // 状态:1正常 2用户删除 3系统删除
	LikeCount     uint64 `gorm:"column:like_count;type:bigint;not null;default:0"`
}

// TableName 表名
func (c *Comment) TableName() string {
	return "t_comment"
}

func (c *Comment) ToEntity() *community.CommentEntity {
	return &community.CommentEntity{
		UUID:          c.UUID,
		UID:           c.UID,
		MarketAddress: c.MarketAddress,
		PostUUID:      c.PostUUID,
		RootUUID:      c.RootUUID,
		ParentUUID:    c.ParentUUID,
		ParentUserUID: c.ParentUserUID,
		Content:       c.Content,
		Status:        c.Status,
		LikeCount:     c.LikeCount,
		BaseEntity: base.BaseEntity{
			Id:        c.ID,
			CreatedAt: c.CreatedAt,
			UpdatedAt: c.UpdatedAt,
		},
	}
}

func (c *Comment) FromEntity(entity *community.CommentEntity) {
	c.UUID = entity.UUID
	c.UID = entity.UID
	c.MarketAddress = entity.MarketAddress
	c.PostUUID = entity.PostUUID
	c.RootUUID = entity.RootUUID
	c.ParentUUID = entity.ParentUUID
	c.ParentUserUID = entity.ParentUserUID
	c.Content = entity.Content
	c.Status = entity.Status
	c.LikeCount = entity.LikeCount
	c.ID = entity.Id
	c.CreatedAt = entity.CreatedAt
	c.UpdatedAt = entity.UpdatedAt
}
