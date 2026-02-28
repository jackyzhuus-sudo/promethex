package asset

import (
	assetBiz "market-service/internal/biz/asset"
	"market-service/internal/biz/base"
	"market-service/internal/model"

	"github.com/shopspring/decimal"
)

type UserMintPoints struct {
	model.BaseModel
	UUID           string          `gorm:"column:uuid;type:varchar(36);uniqueIndex:idx_uuid;not null;" comment:"唯一标识"`
	UID            string          `gorm:"column:uid;type:varchar(24);not null;" comment:"用户ID"`
	TokenAddress   string          `gorm:"column:token_address;type:varchar(42);not null;" comment:"市场地址"`
	BaseTokenAddress string          `gorm:"column:base_token_address" comment:"基础资产代币合约地址"`
	Amount         decimal.Decimal `gorm:"column:amount;type:NUMERIC;not null;default:0" comment:"数量"`
	Status         uint8           `gorm:"column:status;type:smallint;not null;default:3" comment:"1成功2失败3执行中"`
	Source         uint8           `gorm:"column:source;type:smallint;not null;default:0" comment:"1: 初始mint  2: 邀请mint 3: 任务mint"`
	EventProcessed uint8           `gorm:"column:event_processed;type:smallint;not null;default:2" comment:"事件是否处理过 1: 已处理 2: 未处理"`
	TxHash         string          `gorm:"column:tx_hash;type:varchar(66);uniqueIndex:idx_tx_hash;not null;default:''" comment:"交易上链 tx_hash"`
	OpHash         string          `gorm:"column:op_hash;type:varchar(66);not null;default:''" comment:"userOp hash"`
	InviteUID      string          `gorm:"column:invite_uid;type:varchar(24);not null;default:''" comment:"邀请人uid"`
	UserTaskUUID   string          `gorm:"column:user_task_uuid;type:varchar(36);not null;default:''" comment:"用户完成任务记录任务UUID"`
}

func (UserMintPoints) TableName() string {
	return "t_user_mint_points"
}

func (u *UserMintPoints) ToEntity() *assetBiz.UserMintPointsEntity {
	return &assetBiz.UserMintPointsEntity{
		BaseEntity: base.BaseEntity{
			Id:        u.ID,
			CreatedAt: u.CreatedAt,
			UpdatedAt: u.UpdatedAt,
		},
		UUID:           u.UUID,
		UID:            u.UID,
		TokenAddress:   u.TokenAddress,
		BaseTokenAddress: u.BaseTokenAddress,
		Amount:         u.Amount,
		Status:         u.Status,
		EventProcessed: u.EventProcessed,
		Source:         u.Source,
		TxHash:         u.TxHash,
		OpHash:         u.OpHash,
		InviteUID:      u.InviteUID,
		UserTaskUUID:   u.UserTaskUUID,
	}
}

func (u *UserMintPoints) FromEntity(entity *assetBiz.UserMintPointsEntity) {
	u.UUID = entity.UUID
	u.UID = entity.UID
	u.TokenAddress = entity.TokenAddress
	u.BaseTokenAddress = entity.BaseTokenAddress
	u.Amount = entity.Amount
	u.Status = entity.Status
	u.EventProcessed = entity.EventProcessed
	u.Source = entity.Source
	u.TxHash = entity.TxHash
	u.OpHash = entity.OpHash
	u.ID = entity.Id
	u.CreatedAt = entity.CreatedAt
	u.UpdatedAt = entity.UpdatedAt
	u.InviteUID = entity.InviteUID
	u.UserTaskUUID = entity.UserTaskUUID
}
