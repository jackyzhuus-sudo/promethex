package community

import (
	"market-service/internal/biz/base"
)

const (
	PostStatusNormal  = 1
	PostStatusDeleted = 2
	PostStatusBlocked = 3

	CommentStatusNormal  = 1
	CommentStatusDeleted = 2
	CommentStatusBlocked = 3

	UserLikeTypePost    = 1
	UserLikeTypeComment = 2

	UserLikeStatusNormal  = 1
	UserLikeStatusDeleted = 2
)

type PostEntity struct {
	base.BaseEntity
	UUID          string `json:"uuid"`
	UID           string `json:"uid"`
	MarketAddress string `json:"market_address"`
	Title         string `json:"title"`
	Content       string `json:"content"`
	LikeCount     uint64 `json:"like_count"`
	CommentCount  uint64 `json:"comment_count"`
	ViewCount     uint64 `json:"view_count"`
	Status        uint8  `json:"status"`

	IsLike uint8 `json:"is_like"`
}

type CommentEntity struct {
	base.BaseEntity
	UUID          string `json:"uuid"`
	UID           string `json:"uid"`
	MarketAddress string `json:"market_address"`
	PostUUID      string `json:"post_uuid"`
	RootUUID      string `json:"root_uuid"`
	ParentUUID    string `json:"parent_uuid"`
	ParentUserUID string `json:"parent_user_uid"`
	Content       string `json:"content"`
	LikeCount     uint64 `json:"like_count"`
	Status        uint8  `json:"status"`

	IsLike     uint8  `json:"is_like"`
	ReplyCount uint64 `json:"reply_count"`
}

type UserLikeEntity struct {
	base.BaseEntity
	UID         string `json:"uid"`
	ContentUUID string `json:"content_uuid"`
	Type        uint8  `json:"type"`
	Status      uint8  `json:"status"`
}

type UserCommunityInfoEntity struct {
	base.BaseEntity
	UID          string `json:"uid"`
	PostCount    int64  `json:"post_count"`
	CommentCount int64  `json:"comment_count"`
	LikeCount    int64  `json:"like_count"`
}
