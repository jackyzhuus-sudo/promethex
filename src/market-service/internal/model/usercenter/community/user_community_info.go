package community

import (
	"market-service/internal/biz/base"
	"market-service/internal/biz/community"
	"market-service/internal/model"
)

type UserCommunityInfo struct {
	model.BaseModel
	UID          string `gorm:"column:uid;type:varchar(24);uniqueIndex:idx_uid;not null;comment:用户uid"`
	PostCount    int64  `gorm:"column:post_count;type:bigint;not null;default:0;comment:帖子数"`
	CommentCount int64  `gorm:"column:comment_count;type:bigint;not null;default:0;comment:评论数"`
	LikeCount    int64  `gorm:"column:like_count;type:bigint;not null;default:0;comment:点赞数"`
}

func (UserCommunityInfo) TableName() string {
	return "t_user_community_info"
}

func (u *UserCommunityInfo) ToEntity() *community.UserCommunityInfoEntity {
	return &community.UserCommunityInfoEntity{
		UID:          u.UID,
		PostCount:    u.PostCount,
		CommentCount: u.CommentCount,
		LikeCount:    u.LikeCount,
		BaseEntity: base.BaseEntity{
			Id:        u.ID,
			CreatedAt: u.CreatedAt,
			UpdatedAt: u.UpdatedAt,
		},
	}
}

func (u *UserCommunityInfo) FromEntity(entity *community.UserCommunityInfoEntity) {
	u.UID = entity.UID
	u.PostCount = entity.PostCount
	u.CommentCount = entity.CommentCount
	u.LikeCount = entity.LikeCount
	u.ID = entity.Id
	u.CreatedAt = entity.CreatedAt
	u.UpdatedAt = entity.UpdatedAt
}
