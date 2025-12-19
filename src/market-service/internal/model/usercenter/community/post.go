package community

import (
	"market-service/internal/biz/base"
	"market-service/internal/biz/community"
	"market-service/internal/model"
)

// Post 帖子
type Post struct {
	model.BaseModel
	UUID          string `gorm:"column:uuid;type:varchar(36);uniqueIndex:idx_uuid;not null;"` // UUID
	UID           string `gorm:"column:uid;type:varchar(24);not null;default:''"`             // 用户ID
	MarketAddress string `gorm:"column:market_address;type:varchar(42);not null;default:''"`  // 市场地址
	Title         string `gorm:"column:title;type:varchar(256);not null;default:''"`          // 标题
	Content       string `gorm:"column:content;type:text;not null;default:''"`                // 回答详情
	LikeCount     uint64 `gorm:"column:like_count;type:bigint;not null;default:0"`            // 获得的点赞数
	CommentCount  uint64 `gorm:"column:comment_count;type:bigint;not null;default:0"`         // 评论数
	ViewCount     uint64 `gorm:"column:view_count;type:bigint;not null;default:0"`            // 查看过的人数
	Status        uint8  `gorm:"column:status;type:smallint;not null;default:1"`              // 状态:1正常 2用户删除 3系统删除
}

// TableName 表名
func (p *Post) TableName() string {
	return "t_post"
}

func (p *Post) ToEntity() *community.PostEntity {
	return &community.PostEntity{
		BaseEntity: base.BaseEntity{
			Id:        p.ID,
			CreatedAt: p.CreatedAt,
			UpdatedAt: p.UpdatedAt,
		},
		UUID:          p.UUID,
		UID:           p.UID,
		MarketAddress: p.MarketAddress,
		Title:         p.Title,
		Content:       p.Content,
		LikeCount:     p.LikeCount,
		CommentCount:  p.CommentCount,
		ViewCount:     p.ViewCount,
		Status:        p.Status,
	}
}

func (p *Post) FromEntity(entity *community.PostEntity) {
	p.UUID = entity.UUID
	p.UID = entity.UID
	p.MarketAddress = entity.MarketAddress
	p.Title = entity.Title
	p.Content = entity.Content
	p.LikeCount = entity.LikeCount
	p.CommentCount = entity.CommentCount
	p.ViewCount = entity.ViewCount
	p.Status = entity.Status
	p.ID = entity.Id
	p.CreatedAt = entity.CreatedAt
	p.UpdatedAt = entity.UpdatedAt
}
