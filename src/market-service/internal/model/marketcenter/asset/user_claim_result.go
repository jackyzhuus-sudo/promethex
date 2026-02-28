package asset

import (
	"market-service/internal/biz/asset"
	"market-service/internal/biz/base"
	"market-service/internal/model"

	"github.com/shopspring/decimal"
)

type UserClaimResult struct {
	model.BaseModel
	UUID           string          `gorm:"column:uuid;type:varchar(36);uniqueIndex:idx_uuid;not null;" comment:"唯一标识"`
	UID            string          `gorm:"column:uid;type:varchar(24);not null;" comment:"用户ID"`
	MarketAddress  string          `gorm:"column:market_address;type:varchar(42);not null;" comment:"市场地址"`
	OptionAddress  string          `gorm:"column:option_address;type:varchar(42);not null;" comment:"option地址"`
	Amount         decimal.Decimal `gorm:"column:amount;type:NUMERIC;not null;default:0" comment:"数量"`
	BaseTokenAddress string          `gorm:"column:base_token_address" comment:"基础资产代币合约地址"`
	Status         uint8           `gorm:"column:status;type:smallint;not null;default:3" comment:"1成功2失败3执行中"`
	EventProcessed uint8           `gorm:"column:event_processed;type:smallint;not null;default:2" comment:"事件是否处理过 1: 已处理 2: 未处理"`
	TxHash         string          `gorm:"column:tx_hash;type:varchar(66);uniqueIndex:idx_tx_hash;not null;default:''" comment:"交易上链 tx_hash"`
	OpHash         string          `gorm:"column:op_hash;type:varchar(66);not null;default:''" comment:"userOp hash"`
}

func (UserClaimResult) TableName() string {
	return "t_user_claim_result"
}

func (u *UserClaimResult) ToEntity() *asset.UserClaimResultEntity {
	return &asset.UserClaimResultEntity{
		BaseEntity: base.BaseEntity{
			Id:        u.ID,
			CreatedAt: u.CreatedAt,
			UpdatedAt: u.UpdatedAt,
		},
		UUID:           u.UUID,
		UID:            u.UID,
		MarketAddress:  u.MarketAddress,
		OptionAddress:  u.OptionAddress,
		BaseTokenAddress: u.BaseTokenAddress,
		Status:         u.Status,
		EventProcessed: u.EventProcessed,
		Amount:         u.Amount,
		TxHash:         u.TxHash,
		OpHash:         u.OpHash,
	}
}

func (u *UserClaimResult) FromEntity(entity *asset.UserClaimResultEntity) {
	u.UUID = entity.UUID
	u.UID = entity.UID
	u.MarketAddress = entity.MarketAddress
	u.OptionAddress = entity.OptionAddress
	u.BaseTokenAddress = entity.BaseTokenAddress
	u.Status = entity.Status
	u.TxHash = entity.TxHash
	u.Amount = entity.Amount
	u.OpHash = entity.OpHash
	u.EventProcessed = entity.EventProcessed
	u.ID = entity.Id
	u.CreatedAt = entity.CreatedAt
	u.UpdatedAt = entity.UpdatedAt
}
