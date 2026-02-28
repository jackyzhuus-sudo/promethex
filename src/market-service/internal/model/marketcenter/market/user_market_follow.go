package market

import (
	"market-service/internal/biz/base"
	marketBiz "market-service/internal/biz/market"
	"market-service/internal/model"
)

// UserMarketFollow 用户关注市场表结构
type UserMarketFollow struct {
	model.BaseModel
	UID           string `gorm:"column:uid;type:varchar(24);uniqueIndex:idx_uid_market_address;not null;" comment:"用户ID"`
	MarketAddress string `gorm:"column:market_address;type:varchar(42);uniqueIndex:idx_uid_market_address;not null;" comment:"市场地址"`
	BaseTokenType uint8  `gorm:"column:base_token_type;type:smallint;not null;default:1" comment:"对应的基础代币类型 1: points 2: usdc"`
	Status        uint8  `gorm:"column:status;type:smallint;not null;default:1" comment:"状态"`
}

func (u *UserMarketFollow) ToEntity() *marketBiz.UserMarketFollowEntity {
	return &marketBiz.UserMarketFollowEntity{
		UID:           u.UID,
		MarketAddress: u.MarketAddress,
		BaseTokenType: u.BaseTokenType,
		Status:        u.Status,
		BaseEntity: base.BaseEntity{
			Id:        u.ID,
			CreatedAt: u.CreatedAt,
			UpdatedAt: u.UpdatedAt,
		},
	}
}

func (u *UserMarketFollow) FromEntity(entity *marketBiz.UserMarketFollowEntity) {
	u.UID = entity.UID
	u.MarketAddress = entity.MarketAddress
	u.BaseTokenType = entity.BaseTokenType
	u.Status = entity.Status
	u.ID = entity.Id
	u.CreatedAt = entity.CreatedAt
	u.UpdatedAt = entity.UpdatedAt
}

// TableName 指定表名
func (UserMarketFollow) TableName() string {
	return "t_user_market_follow"
}
