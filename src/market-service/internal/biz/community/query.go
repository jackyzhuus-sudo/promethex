package community

import (
	"market-service/internal/biz/base"

	"gorm.io/gorm"
)

type PostQuery struct {
	base.BaseQuery
	UUID          string // UUID
	UID           string // 用户ID
	MarketAddress string // 市场地址
	Title         string // 标题
	TitleLike     string // 标题模糊查询
	Status        uint8  // 状态
}

type CommentQuery struct {
	base.BaseQuery
	UUID            string // UUID
	UID             string // 用户ID
	PostUUID        string // 帖子ID
	RootUUID        string // 根ID
	ParentUUID      string // 父ID
	RootUUIDIsNil   bool   // 是否要查根ID为空
	ParentUUIDIsNil bool   // 是否要查父ID为空
	Status          uint8  // 状态
}

type UserLikeQuery struct {
	base.BaseQuery
	UID             string   // 用户ID
	ContentUUID     string   // 内容ID
	ContentUUIDList []string // 内容ID列表
	Type            uint8    // 类型
	Status          uint8    // 状态
}

type UserCommunityInfoQuery struct {
	base.BaseQuery
	UID string // 用户ID
}

func (query *PostQuery) Condition(db *gorm.DB, total *int64) *gorm.DB {
	if query.UUID != "" {
		db = db.Where("uuid = ?", query.UUID)
	}
	if query.UID != "" {
		db = db.Where("uid = ?", query.UID)
	}
	if query.MarketAddress != "" {
		db = db.Where("market_address = ?", query.MarketAddress)
	}
	if query.Title != "" {
		db = db.Where("title = ?", query.Title)
	}
	if query.TitleLike != "" {
		db = db.Where("title like ?", query.TitleLike+"%")
	}
	if query.Status != 0 {
		db = db.Where("status = ?", query.Status)
	}

	return query.BaseQuery.Condition(db, total)
}

func (query *CommentQuery) Condition(db *gorm.DB, total *int64) *gorm.DB {
	if query.UUID != "" {
		db = db.Where("uuid = ?", query.UUID)
	}
	if query.UID != "" {
		db = db.Where("uid = ?", query.UID)
	}
	if query.PostUUID != "" {
		db = db.Where("post_uuid = ?", query.PostUUID)
	}
	if query.RootUUID != "" {
		db = db.Where("root_uuid = ?", query.RootUUID)
	}
	if query.ParentUUID != "" {
		db = db.Where("parent_uuid = ?", query.ParentUUID)
	}
	if query.RootUUIDIsNil {
		db = db.Where("root_uuid = ''")
	}
	if query.ParentUUIDIsNil {
		db = db.Where("parent_uuid = ''")
	}
	if query.Status != 0 {
		db = db.Where("status = ?", query.Status)
	}
	return query.BaseQuery.Condition(db, total)
}

func (query *UserLikeQuery) Condition(db *gorm.DB, total *int64) *gorm.DB {
	if query.UID != "" {
		db = db.Where("uid = ?", query.UID)
	}
	if query.ContentUUID != "" {
		db = db.Where("content_uuid = ?", query.ContentUUID)
	}
	if len(query.ContentUUIDList) > 0 {
		db = db.Where("content_uuid in ?", query.ContentUUIDList)
	}
	if query.Type != 0 {
		db = db.Where("type = ?", query.Type)
	}
	if query.Status != 0 {
		db = db.Where("status = ?", query.Status)
	}
	return query.BaseQuery.Condition(db, total)
}

func (query *UserCommunityInfoQuery) Condition(db *gorm.DB, total *int64) *gorm.DB {
	if query.UID != "" {
		db = db.Where("uid = ?", query.UID)
	}
	return query.BaseQuery.Condition(db, total)
}
