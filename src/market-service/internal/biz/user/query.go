package user

import (
	"market-service/internal/biz/base"

	"gorm.io/gorm"
)

// UserQuery 用户查询结构体
type UserQuery struct {
	base.BaseQuery
	UID         string   // 用户ID
	UIDList     []string // 用户ID列表
	AddressList []string // 钱包地址列表
	Address     string   // 钱包地址
	Email       string   // 邮箱
	InviteCode  string   // 邀请码
	InviterUID  string   // 邀请人ID
	Issuer      string   // DID
	Name        string   // 用户名
	NameLike    string   // 用户名模糊查询
	EoaAddress  string   // eoa地址
	Search      string   // 搜索关键词
	Source      int8     // 用户来源

}

type UserFollowQuery struct {
	base.BaseQuery
	UID       string // 用户ID
	FollowUID string // 关注用户ID
	Status    uint8  // 状态
}

type UserNotificationQuery struct {
	base.BaseQuery
	UUID            string   // 通知ID
	UUIDList        []string // 通知ID列表
	Category        uint8    // 通知分类
	Type            uint8    // 通知类型
	UID             string   // 用户ID
	Status          uint8    // 状态
	BizJsonPostUuid string   // 业务数据post_uuid
	BaseTokenType   uint8    // 代币类型
}

func (query *UserNotificationQuery) Condition(db *gorm.DB, total *int64) *gorm.DB {
	if query.UID != "" {
		db = db.Where("uid = ?", query.UID)
	}
	if query.UUID != "" {
		db = db.Where("uuid = ?", query.UUID)
	}
	if len(query.UUIDList) > 0 {
		db = db.Where("uuid IN ?", query.UUIDList)
	}
	if query.Category != 0 {
		db = db.Where("category = ?", query.Category)
	}
	if query.Type != 0 {
		db = db.Where("type = ?", query.Type)
	}
	if query.Status != 0 {
		db = db.Where("status = ?", query.Status)
	}
	if query.BizJsonPostUuid != "" {
		db = db.Where("biz_json->>'post_uuid' = ?", query.BizJsonPostUuid)
	}
	if query.BaseTokenType != 0 {
		db = db.Where("base_token_type = ?", query.BaseTokenType)
	}
	return query.BaseQuery.Condition(db, total)
}

// Condition 用户查询条件
func (query *UserQuery) Condition(db *gorm.DB, total *int64) *gorm.DB {
	if query.UID != "" {
		db = db.Where("uid = ?", query.UID)
	}
	if len(query.UIDList) > 0 {
		db = db.Where("uid IN ?", query.UIDList)
	}
	if len(query.AddressList) > 0 {
		db = db.Where("address IN ?", query.AddressList)
	}
	if query.Address != "" {
		db = db.Where("address = ?", query.Address)
	}
	if query.EoaAddress != "" {
		db = db.Where("eoa_address = ?", query.EoaAddress)
	}
	if query.Email != "" {
		db = db.Where("email = ?", query.Email)
	}
	if query.InviteCode != "" {
		db = db.Where("invite_code = ?", query.InviteCode)
	}
	if query.InviterUID != "" {
		db = db.Where("inviter_uid = ?", query.InviterUID)
	}
	if query.Issuer != "" {
		db = db.Where("issuer = ?", query.Issuer)
	}
	if query.Name != "" {
		db = db.Where("name = ?", query.Name)
	}
	if query.NameLike != "" {
		db = db.Where("name LIKE ?", "%"+query.NameLike+"%")
	}
	if query.Search != "" {
		db = db.Where("name LIKE ?", "%"+query.Search+"%").Or("address = ?", query.Search)
	}
	if query.Source != 0 {
		db = db.Where("source = ?", query.Source)
	}
	return query.BaseQuery.Condition(db, total)
}

// Condition 用户关注查询条件
func (query *UserFollowQuery) Condition(db *gorm.DB, total *int64) *gorm.DB {
	if query.UID != "" {
		db = db.Where("uid = ?", query.UID)
	}
	if query.FollowUID != "" {
		db = db.Where("follow_uid = ?", query.FollowUID)
	}
	if query.Status != 0 {
		db = db.Where("status = ?", query.Status)
	}
	return query.BaseQuery.Condition(db, total)
}
