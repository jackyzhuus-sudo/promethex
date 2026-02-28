package asset

import (
	"encoding/json"
	assetBiz "market-service/internal/biz/asset"
	"market-service/internal/biz/base"
	"market-service/internal/model"
)

// SendTx 交易表结构
type SendTx struct {
	model.BaseModel
	UID           string          `gorm:"column:uid;type:varchar(24);not null;" comment:"用户ID"`
	TxHash        string          `gorm:"column:tx_hash;type:varchar(66);not null;default:''" comment:"交易hash"`
	OpHash        string          `gorm:"column:op_hash;type:varchar(66);not null;default:''" comment:"userOp hash"`
	Status        uint8           `gorm:"column:status;type:smallint;not null;default:0" comment:""`
	Chain         string          `gorm:"column:chain;type:varchar(32);not null;default:'arb'" comment:"链"`
	ErrMsg        string          `gorm:"column:err_msg;type:varchar(64);not null;default:''" comment:"错误信息"`
	RetryCount    uint16          `gorm:"column:retry_count;type:smallint;not null;default:0" comment:"重试次数"`
	Source        uint8           `gorm:"column:source;type:smallint;not null;default:0" comment:"1 交易buy 2 交易sell 3 结算claim 4 积分mint 5 邀请积分mint 6 转入deposit 7 转出withdraw 8 任务积分mint"`
	Type          uint8           `gorm:"column:type;type:smallint;not null;default:0" comment:"1使用userOperation上链(帮用户上链) 2一般交易"`
	UserOperation json.RawMessage `gorm:"column:user_operation;type:jsonb;not null;default:'{}'" comment:"userOperation"`
	BaseTokenAddress string          `gorm:"column:base_token_address" comment:"基础资产代币合约地址"`
}

func (s *SendTx) ToEntity() *assetBiz.SendTxEntity {
	return &assetBiz.SendTxEntity{
		BaseEntity: base.BaseEntity{
			Id:        s.ID,
			CreatedAt: s.CreatedAt,
			UpdatedAt: s.UpdatedAt,
		},
		UID:        s.UID,
		TxHash:     s.TxHash,
		OpHash:     s.OpHash,
		Status:     s.Status,
		Chain:      s.Chain,
		ErrMsg:     s.ErrMsg,
		RetryCount: s.RetryCount,
		Source:     s.Source,
		Type:       s.Type,
		UserOperation: func() *assetBiz.UserOperation {
			var userOperation *assetBiz.UserOperation
			if s.UserOperation != nil {
				err := json.Unmarshal(s.UserOperation, userOperation)
				if err != nil {
					return nil
				}
			}
			return userOperation
		}(),
		BaseTokenAddress: s.BaseTokenAddress,
	}
}

func (s *SendTx) FromEntity(entity *assetBiz.SendTxEntity) {
	s.UID = entity.UID
	s.TxHash = entity.TxHash
	s.OpHash = entity.OpHash
	s.Status = entity.Status
	s.Chain = entity.Chain
	s.ErrMsg = entity.ErrMsg
	s.RetryCount = entity.RetryCount
	s.Source = entity.Source
	s.Type = entity.Type
	s.ID = entity.Id
	s.CreatedAt = entity.CreatedAt
	s.UpdatedAt = entity.UpdatedAt
	s.UserOperation = func() json.RawMessage {
		jsonData, err := json.Marshal(entity.UserOperation)
		if err != nil {
			return nil
		}
		return jsonData
	}()
	s.BaseTokenAddress = entity.BaseTokenAddress
}

// TableName 指定表名
func (SendTx) TableName() string {
	return "t_send_tx"
}
