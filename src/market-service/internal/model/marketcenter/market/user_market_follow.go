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
	BaseTokenAddress string `gorm:"column:base_token_address" comment:"基础资产代币合约地址"`
	Status        uint8  `gorm:"column:status;type:smallint;not null;default:1" comment:"状态"`
}

func (u *UserMarketFollow) ToEntity() *marketBiz.UserMarketFollowEntity {
	return &marketBiz.UserMarketFollowEntity{
		UID:           u.UID,
		MarketAddress: u.MarketAddress,
		BaseTokenAddress: u.BaseTokenAddress,
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
	u.BaseTokenAddress = entity.BaseTokenAddress
	u.Status = entity.Status
	u.ID = entity.Id
	u.CreatedAt = entity.CreatedAt
	u.UpdatedAt = entity.UpdatedAt
}

// TableName 指定表名
func (UserMarketFollow) TableName() string {
	return "t_user_market_follow"
}
