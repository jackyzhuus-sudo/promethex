package user

import (
	"encoding/json"
	"market-service/internal/biz/base"
	userBiz "market-service/internal/biz/user"
	"market-service/internal/model"
)

type User struct {
	model.BaseModel
	UID                 string          `gorm:"column:uid;type:varchar(24);uniqueIndex:idx_uid;not null;comment:用户uid"`
	EoaAddress          string          `gorm:"column:eoa_address;type:varchar(42);uniqueIndex:idx_eoa_address;not null;"`
	Address             string          `gorm:"column:address;type:varchar(42);uniqueIndex:idx_address;not null;"`
	Email               string          `gorm:"column:email;type:varchar(256)uniqueIndex:idx_email_source;not null;default:''"`
	Avatar              string          `gorm:"column:avatar;type:varchar(256);not null;default:'';comment:用户头像"`
	Description         string          `gorm:"column:description;type:varchar(256);not null;default:'';comment:用户简介"`
	Name                string          `gorm:"column:name;type:varchar(128);not null;default:'';comment:用户昵称"`
	InviteCode          string          `gorm:"column:invite_code;type:varchar(16);uniqueIndex:idx_invite_code;not null;default:'';comment:邀请码"`
	InviterUID          string          `gorm:"column:inviter_uid;type:varchar(24);not null;default:'';comment:邀请人"`
	Issuer              string          `gorm:"column:issuer;type:varchar(64);not null;default:'';comment:did"`
	FollowerCount       int64           `gorm:"column:follower_count;type:bigint;not null;default:0;comment:粉丝数"`
	FollowCount         int64           `gorm:"column:follow_count;type:bigint;not null;default:0;comment:关注数"`
	PostCount           int64           `gorm:"column:post_count;type:bigint;not null;default:0;comment:帖子数"`
	InviteAt            int64           `gorm:"column:invite_at;type:bigint;not null;default:0;comment:邀请时间"`
	EarnedInvitePoints  int64           `gorm:"column:earned_invite_points;type:bigint;not null;default:0;comment:邀请奖励积分"`
	ProvideInvitePoints int64           `gorm:"column:provide_invite_points;type:bigint;not null;default:0;comment:提供的邀请积分"`
	PrivyUserInfo       json.RawMessage `gorm:"column:privy_user_info;type:jsonb;not null;default:'{}'" comment:"privy用户信息"`
	Source              int8            `gorm:"column:source;type:int8;uniqueIndex:idx_email_source;not null;default:1;comment:用户来源 1 magiclink 2 privy"`
}

func (u *User) ToEntity() *userBiz.UserEntity {
	return &userBiz.UserEntity{
		EoaAddress:          u.EoaAddress,
		Address:             u.Address,
		Email:               u.Email,
		UID:                 u.UID,
		Avatar:              u.Avatar,
		Description:         u.Description,
		Name:                u.Name,
		InviteCode:          u.InviteCode,
		InviterUID:          u.InviterUID,
		Issuer:              u.Issuer,
		FollowerCount:       u.FollowerCount,
		FollowCount:         u.FollowCount,
		PostCount:           u.PostCount,
		InviteAt:            u.InviteAt,
		EarnedInvitePoints:  u.EarnedInvitePoints,
		ProvideInvitePoints: u.ProvideInvitePoints,
		PrivyUserInfo:       u.PrivyUserInfo,
		Source:              u.Source,
		BaseEntity: base.BaseEntity{
			Id:        u.ID,
			CreatedAt: u.CreatedAt,
			UpdatedAt: u.UpdatedAt,
		},
	}
}

func (u *User) FromEntity(entity *userBiz.UserEntity) {
	u.EoaAddress = entity.EoaAddress
	u.Address = entity.Address
	u.Email = entity.Email
	u.UID = entity.UID
	u.Avatar = entity.Avatar
	u.Description = entity.Description
	u.Name = entity.Name
	u.InviteCode = entity.InviteCode
	u.InviterUID = entity.InviterUID
	u.Issuer = entity.Issuer
	u.FollowerCount = entity.FollowerCount
	u.FollowCount = entity.FollowCount
	u.PostCount = entity.PostCount
	u.InviteAt = entity.InviteAt
	u.EarnedInvitePoints = entity.EarnedInvitePoints
	u.ProvideInvitePoints = entity.ProvideInvitePoints
	u.ID = entity.Id
	u.CreatedAt = entity.CreatedAt
	u.UpdatedAt = entity.UpdatedAt
	u.PrivyUserInfo = entity.PrivyUserInfo
	u.Source = entity.Source
}

// TableName 指定表名
func (User) TableName() string {
	return "t_user"
}
