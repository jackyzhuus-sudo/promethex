package asset

import (
	assetBiz "market-service/internal/biz/asset"
	"market-service/internal/biz/base"

	"github.com/shopspring/decimal"

	"market-service/internal/model"
)

// UserMarketPosition 用户市场持仓表
type UserMarketPosition struct {
	model.BaseModel
	UID           string          `gorm:"column:uid;type:varchar(24);not null" comment:"用户ID"`
	MarketAddress string          `gorm:"column:market_address;type:varchar(42);not null" comment:"市场地址"`
	BaseTokenType uint8           `gorm:"column:base_token_type;type:smallint;not null;default:1" comment:"对应的基础代币类型 1: points 2: usdc"`
	TotalValue    decimal.Decimal `gorm:"column:total_value;type:NUMERIC;not null;default:0" comment:"总持仓价值"`
	Status        uint8           `gorm:"column:status;type:smallint;not null;default:1" comment:"状态 1持仓中 2市场已结束"`

	_ struct{} `gorm:"uniqueIndex:idx_uid_market_address1;columns:uid,market_address"`
}

func (UserMarketPosition) TableName() string {
	return "t_user_market_position"
}

func (u *UserMarketPosition) ToEntity() *assetBiz.UserMarketPositionEntity {
	return &assetBiz.UserMarketPositionEntity{
		BaseEntity: base.BaseEntity{
			Id:        u.ID,
			CreatedAt: u.CreatedAt,
			UpdatedAt: u.UpdatedAt,
		},
		UID:           u.UID,
		MarketAddress: u.MarketAddress,
		BaseTokenType: u.BaseTokenType,
		TotalValue:    u.TotalValue,
		Status:        u.Status,
	}
}

func (u *UserMarketPosition) FromEntity(entity *assetBiz.UserMarketPositionEntity) {
	u.UID = entity.UID
	u.MarketAddress = entity.MarketAddress
	u.BaseTokenType = entity.BaseTokenType
	u.TotalValue = entity.TotalValue
	u.ID = entity.Id
	u.CreatedAt = entity.CreatedAt
	u.UpdatedAt = entity.UpdatedAt
	u.Status = entity.Status
}
