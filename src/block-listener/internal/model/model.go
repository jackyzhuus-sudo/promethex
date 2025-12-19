package model

import (
	"time"

	"github.com/ethereum/go-ethereum/common"
	ethereum "github.com/ethereum/go-ethereum/core/types"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
)

// BaseModel 基础模型
type BaseModel struct {
	ID        uint      `gorm:"primarykey"`
	CreatedAt time.Time // 存储带时区的时间
	UpdatedAt time.Time // 存储带时区的时间
}

const (
	EventLogStatusConfirmedWait = 1
	EventLogStatusConfirmed     = 2
	EventLogStatusFiltered      = 3
	EventLogStatusFailed        = 4

	TypeFactory       = 1
	TypePrediction    = 2
	TypeErc20Transfer = 3
)

type EventLog struct {
	BaseModel
	Address     string         `gorm:"column:address;type:char(42);not null;comment:合约地址"`
	Topics      pq.StringArray `gorm:"column:topics;type:text[];not null;comment:事件主题"`
	Data        []byte         `gorm:"column:data;type:bytea;not null;comment:事件数据"`
	BlockNumber uint64         `gorm:"column:block_number;type:bigint;not null;comment:区块高度"`
	TxHash      string         `gorm:"column:tx_hash;type:char(66);not null;comment:交易哈希"`
	TxIndex     uint           `gorm:"column:tx_index;type:int;not null;comment:交易索引"`
	BlockHash   string         `gorm:"column:block_hash;type:char(66);not null;comment:区块哈希"`
	LogIndex    uint           `gorm:"column:log_index;type:int;not null;comment:日志索引"`
	Removed     bool           `gorm:"column:removed;type:boolean;not null;default:false;comment:是否被移除"`
	Status      uint           `gorm:"column:status;type:smallint;not null;default:1;comment:状态 1:待处理 2:处理中 3:处理完成"`
	Type        uint           `gorm:"column:type;type:smallint;not null;default:1;comment:事件类型 1:工厂合约 2:预测市场合约 3:erc20代币转移"`
}

func (EventLog) TableName() string {
	return "event_logs"
}

// ToEventLog 将链上日志转换为数据库模型
func (e *EventLog) ToEventLog(chainLog *ethereum.Log) {
	topics := make([]string, len(chainLog.Topics))
	for i, topic := range chainLog.Topics {
		topics[i] = topic.Hex()
	}

	e.Address = chainLog.Address.Hex()
	e.Data = chainLog.Data
	e.Topics = topics
	e.BlockNumber = chainLog.BlockNumber
	e.TxHash = chainLog.TxHash.Hex()
	e.TxIndex = chainLog.TxIndex
	e.BlockHash = chainLog.BlockHash.Hex()
	e.LogIndex = chainLog.Index
	e.Removed = chainLog.Removed
}

// ToChainLog 将数据库模型转换为链上日志
func (e *EventLog) ToChainLog() *ethereum.Log {
	topics := make([]common.Hash, len(e.Topics))
	for i, topic := range e.Topics {
		topics[i] = common.HexToHash(topic)
	}

	return &ethereum.Log{
		Address:     common.HexToAddress(e.Address),
		Data:        e.Data,
		BlockNumber: e.BlockNumber,
		TxHash:      common.HexToHash(e.TxHash),
		TxIndex:     e.TxIndex,
		BlockHash:   common.HexToHash(e.BlockHash),
		Index:       e.LogIndex,
		Removed:     e.Removed,
		Topics:      topics,
	}
}
