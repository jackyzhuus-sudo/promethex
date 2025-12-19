package asset

import (
	assetBiz "market-service/internal/biz/asset"
	"market-service/internal/biz/base"
	"market-service/internal/model"

	"github.com/shopspring/decimal"
)

// UserTokenBalance 用户代币余额表结构（持仓）
type UserTokenBalance struct {
	model.BaseModel
	UID           string          `gorm:"column:uid;type:varchar(24);not null;uniqueIndex:idx_uid_token" comment:"用户ID"`
	TokenAddress  string          `gorm:"column:token_address;type:varchar(42);not null;uniqueIndex:idx_uid_token" comment:"代币地址"`
	MarketAddress string          `gorm:"column:market_address;type:varchar(42);not null" comment:"市场地址"`
	Balance       decimal.Decimal `gorm:"column:balance;type:NUMERIC;not null" comment:"余额"`
	Decimal       uint8           `gorm:"column:decimal;type:smallint;not null;default:6" comment:"精度"`
	BlockNumber   uint64          `gorm:"column:block_number;type:bigint;not null;default:0" comment:"区块高度"`
	Type          uint8           `gorm:"column:type;type:smallint;not null;default:1" comment:"类型 1资产代币 2条件代币"`
	BaseTokenType uint8           `gorm:"column:base_token_type;type:smallint;not null;default:1" comment:"如果是条件代币 对应的基础代币类型 1: points 2: usdc"`
	AvgBuyPrice   decimal.Decimal `gorm:"column:avg_buy_price;type:NUMERIC;not null;default:0" comment:"平均买入价格"`
	Status        uint8           `gorm:"column:status;type:smallint;not null;default:1" comment:"状态 1持仓中 2已结束获胜选项 3已结束非获胜选项"`
	IsClaimed     uint8           `gorm:"column:is_claimed;type:smallint;not null;default:2" comment:"非获胜选项 是否已领取 1是 2否"`
}

func (u *UserTokenBalance) ToEntity() *assetBiz.UserTokenBalanceEntity {
	return &assetBiz.UserTokenBalanceEntity{
		BaseEntity: base.BaseEntity{
			Id:        u.ID,
			CreatedAt: u.CreatedAt,
			UpdatedAt: u.UpdatedAt,
		},
		UID:           u.UID,
		TokenAddress:  u.TokenAddress,
		MarketAddress: u.MarketAddress,
		Balance:       u.Balance,
		Decimal:       u.Decimal,
		BlockNumber:   u.BlockNumber,
		Type:          u.Type,
		BaseTokenType: u.BaseTokenType,
		AvgBuyPrice:   u.AvgBuyPrice,
		Status:        u.Status,
		IsClaimed:     u.IsClaimed,
	}
}

func (u *UserTokenBalance) FromEntity(entity *assetBiz.UserTokenBalanceEntity) {
	u.UID = entity.UID
	u.TokenAddress = entity.TokenAddress
	u.MarketAddress = entity.MarketAddress
	u.Balance = entity.Balance
	u.BlockNumber = entity.BlockNumber
	u.Type = entity.Type
	u.BaseTokenType = entity.BaseTokenType
	u.Decimal = entity.Decimal
	u.ID = entity.Id
	u.CreatedAt = entity.CreatedAt
	u.UpdatedAt = entity.UpdatedAt
	u.AvgBuyPrice = entity.AvgBuyPrice
	u.IsClaimed = entity.IsClaimed
}

// TableName 指定表名
func (UserTokenBalance) TableName() string {
	return "t_user_token_balance"
}
