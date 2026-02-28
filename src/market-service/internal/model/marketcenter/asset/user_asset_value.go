package asset

import (
	assetBiz "market-service/internal/biz/asset"
	"market-service/internal/biz/base"
	"market-service/internal/model"
	"time"

	"github.com/shopspring/decimal"
)

// UserAssetValue 用户资产总价值表 时序表
type UserAssetValue struct {
	model.BaseModel
	Time          time.Time       `gorm:"column:time;type:timestamptz;uniqueIndex:uid_asset_time;not null;" comment:"时间"`
	UID           string          `gorm:"column:uid;type:varchar(24);uniqueIndex:uid_asset_time;not null;" comment:"用户ID"`
	AssetAddress  string          `gorm:"column:asset_address;type:varchar(42);uniqueIndex:uid_asset_time;not null;" comment:"资产地址"`
	Value         decimal.Decimal `gorm:"column:value;type:NUMERIC;not null;default:0" comment:"某时刻的某资产总额"`
	Balance       decimal.Decimal `gorm:"column:balance;type:NUMERIC;not null;default:0" comment:"某时刻的某资产余额"`
	Portfolio     decimal.Decimal `gorm:"column:portfolio;type:NUMERIC;not null;default:0" comment:"某时刻的某资产持仓总价值"`
	Pnl           decimal.Decimal `gorm:"column:pnl;type:NUMERIC;not null;default:0" comment:"某时刻的某资产总盈亏"`
	BaseTokenType uint8           `gorm:"column:base_token_type;type:smallint;not null;default:1" comment:"对应的基础代币类型 1: points 2: usdc"`
}

func (u *UserAssetValue) ToEntity() *assetBiz.UserAssetValueEntity {
	return &assetBiz.UserAssetValueEntity{
		BaseEntity: base.BaseEntity{
			Id:        u.ID,
			CreatedAt: u.CreatedAt,
			UpdatedAt: u.UpdatedAt,
		},
		UID:           u.UID,
		AssetAddress:  u.AssetAddress,
		Value:         u.Value,
		Balance:       u.Balance,
		Portfolio:     u.Portfolio,
		Pnl:           u.Pnl,
		BaseTokenType: u.BaseTokenType,
		Time:          u.Time,
	}
}

func (u *UserAssetValue) FromEntity(entity *assetBiz.UserAssetValueEntity) {
	u.UID = entity.UID
	u.AssetAddress = entity.AssetAddress
	u.Value = entity.Value
	u.Balance = entity.Balance
	u.Portfolio = entity.Portfolio
	u.Pnl = entity.Pnl
	u.BaseTokenType = entity.BaseTokenType
	u.Time = entity.Time
	u.ID = entity.Id
	u.CreatedAt = entity.CreatedAt
	u.UpdatedAt = entity.UpdatedAt
}

// TableName 指定表名
func (UserAssetValue) TableName() string {
	return "t_user_asset_value"
}
