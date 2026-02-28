package asset

import (
	assetBiz "market-service/internal/biz/asset"
	"market-service/internal/biz/base"
	"market-service/internal/model"

	"github.com/shopspring/decimal"
)

type UserTransferTokens struct {
	model.BaseModel
	UUID            string          `gorm:"column:uuid;type:varchar(36);uniqueIndex:idx_uuid;not null;" comment:"唯一标识"`
	UID             string          `gorm:"column:uid;type:varchar(24);not null;" comment:"用户ID"`
	ExternalAddress string          `gorm:"column:external_address;type:varchar(42);not null;" comment:"外部地址"`
	TokenAddress    string          `gorm:"column:token_address;type:varchar(42);not null;" comment:"代币地址"`
	Side            uint8           `gorm:"column:side;type:smallint;not null;default:1" comment:"1: 转入deposit 2: 转出withdraw"`
	BaseTokenType   uint8           `gorm:"column:base_token_type;type:smallint;not null;default:1" comment:"对应的基础代币类型 1: points 2: usdc"`
	Amount          decimal.Decimal `gorm:"column:amount;type:NUMERIC;not null;default:0" comment:"数量"`
	Status          uint8           `gorm:"column:status;type:smallint;not null;default:3" comment:"1成功2失败3执行中"`
	EventProcessed  uint8           `gorm:"column:event_processed;type:smallint;not null;default:2" comment:"事件是否处理过 1: 已处理 2: 未处理"`
	TxHash          string          `gorm:"column:tx_hash;type:varchar(66);uniqueIndex:idx_tx_hash;not null;default:''" comment:"交易上链 tx_hash"`
	OpHash          string          `gorm:"column:op_hash;type:varchar(66);not null;default:''" comment:"userOp hash"`
}

func (UserTransferTokens) TableName() string {
	return "t_user_transfer_tokens"
}

func (u *UserTransferTokens) ToEntity() *assetBiz.UserTransferTokensEntity {
	return &assetBiz.UserTransferTokensEntity{
		BaseEntity: base.BaseEntity{
			Id:        u.ID,
			CreatedAt: u.CreatedAt,
			UpdatedAt: u.UpdatedAt,
		},
		UUID:            u.UUID,
		UID:             u.UID,
		ExternalAddress: u.ExternalAddress,
		Side:            u.Side,
		TokenAddress:    u.TokenAddress,
		BaseTokenType:   u.BaseTokenType,
		Amount:          u.Amount,
		Status:          u.Status,
		EventProcessed:  u.EventProcessed,
		TxHash:          u.TxHash,
		OpHash:          u.OpHash,
	}
}

func (u *UserTransferTokens) FromEntity(entity *assetBiz.UserTransferTokensEntity) {
	u.UUID = entity.UUID
	u.UID = entity.UID
	u.ExternalAddress = entity.ExternalAddress
	u.Side = entity.Side
	u.TokenAddress = entity.TokenAddress
	u.BaseTokenType = entity.BaseTokenType
	u.Amount = entity.Amount
	u.Status = entity.Status
	u.EventProcessed = entity.EventProcessed
	u.TxHash = entity.TxHash
	u.OpHash = entity.OpHash
	u.ID = entity.Id
	u.CreatedAt = entity.CreatedAt
	u.UpdatedAt = entity.UpdatedAt
}
