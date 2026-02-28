package asset

import (
	"market-service/internal/biz/base"
	"time"

	"github.com/shopspring/decimal"
	"gorm.io/gorm"
)

// UserTokenBalanceQuery 用户代币余额查询结构体
type UserTokenBalanceQuery struct {
	base.BaseQuery
	UID               string          // 用户ID
	UIDList           []string        // 用户ID列表
	MarketAddress     string          // 市场地址
	MarketAddressList []string        // 市场地址列表
	TokenAddress      string          // 代币地址
	TokenAddressList  []string        // 代币地址列表
	Type              uint8           // 类型
	BaseTokenAddress  string          // 基础代币地址
	NoZero            bool            // 不等于0
	Status            uint8           // 状态
	StatusIn          []uint8         // 状态列表
	StatusNotEqual    uint8           // 状态不等于
	MinBalance        decimal.Decimal // 最小余额
}

// UserAssetValueQuery 用户资产总价值查询结构体
type UserAssetValueQuery struct {
	base.BaseQuery
	UID           string    // 用户ID
	AssetAddress     string    // 资产地址
	BaseTokenAddress string    // 基础代币地址
	StartTime        time.Time // 开始时间
	EndTime       time.Time // 结束时间
}

// UserMarketPositionQuery 用户持仓查询结构体
type UserMarketPositionQuery struct {
	base.BaseQuery
	UID           string // 用户ID
	MarketAddress    string // 市场地址
	BaseTokenAddress string // 基础代币地址
}

// OrderQuery 订单查询结构体
type OrderQuery struct {
	base.BaseQuery
	UID            string // 用户ID
	MarketAddress  string // 市场地址
	OptionAddress    string // 条件代币地址
	BaseTokenAddress string // 基础代币地址
	Status           uint8  // 订单状态
	Side           uint8  // 交易方向
	UUID           string // 订单唯一标识
	EventProcessed uint8  // 事件是否处理过
	TxHashList     []string
	StartTime      string // 开始时间
	EndTime        string // 结束时间
}

// SendTxQuery 交易查询结构体
type SendTxQuery struct {
	base.BaseQuery
	UID           string // 用户ID
	TxHash        string // 交易哈希
	Chain         string // 链名称
	Status        uint8  // 交易状态
	Type             uint8  // 交易类型
	BaseTokenAddress string // 基础代币地址
}

// UserClaimResultQuery 用户申领结果查询结构体
type UserClaimResultQuery struct {
	base.BaseQuery
	Uuid           string
	UID            string // 用户ID
	TxHash         string // 交易哈希
	OpHash         string // 操作哈希
	Status           uint8  // 状态
	BaseTokenAddress string // 基础代币地址
	MarketAddress    string // 市场地址
	OptionAddress  string // 条件代币地址
	EventProcessed uint8  // 事件是否处理过
	TxHashList     []string
}

type UserMintPointsQuery struct {
	base.BaseQuery
	Uid            string // 用户ID
	TxHash         string // 交易哈希
	Status         uint8  // 状态
	Source         uint8  // 来源
	EventProcessed uint8  // 事件是否处理过
	TxHashList     []string
	InviteUID      string   // 邀请人ID
	StatusIn       []int    // 状态列表
	UserTaskUuids  []string // 用户任务UUID
}

type UserTransferTokensQuery struct {
	base.BaseQuery
	Uid        string // 用户ID
	TxHash     string // 交易哈希
	Status     uint8  // 状态
	TxHashList []string
}

func (query *UserMintPointsQuery) Condition(db *gorm.DB, total *int64) *gorm.DB {
	if query.Uid != "" {
		db = db.Where("uid = ?", query.Uid)
	}

	if len(query.TxHashList) > 0 {
		db = db.Where("tx_hash IN ?", query.TxHashList)
	}
	if query.Source > 0 {
		db = db.Where("source = ?", query.Source)
	}
	if query.EventProcessed > 0 {
		db = db.Where("event_processed = ?", query.EventProcessed)
	}
	if query.InviteUID != "" {
		db = db.Where("invite_uid = ?", query.InviteUID)
	}
	if query.Status > 0 {
		db = db.Where("status = ?", query.Status)
	}
	if len(query.StatusIn) > 0 {
		db = db.Where("status IN ?", query.StatusIn)
	}
	if len(query.UserTaskUuids) > 0 {
		db = db.Where("user_task_uuid IN ?", query.UserTaskUuids)
	}
	return query.BaseQuery.Condition(db, total)
}

func (query *UserTransferTokensQuery) Condition(db *gorm.DB, total *int64) *gorm.DB {
	if query.Uid != "" {
		db = db.Where("uid = ?", query.Uid)
	}
	if len(query.TxHashList) > 0 {
		db = db.Where("tx_hash IN ?", query.TxHashList)
	}
	return query.BaseQuery.Condition(db, total)
}

// Condition 用户申领结果查询条件
func (query *UserClaimResultQuery) Condition(db *gorm.DB, total *int64) *gorm.DB {
	if query.UID != "" {
		db = db.Where("uid = ?", query.UID)
	}
	if query.Uuid != "" {
		db = db.Where("uuid = ?", query.Uuid)
	}
	if query.TxHash != "" {
		db = db.Where("tx_hash = ?", query.TxHash)
	}
	if query.OpHash != "" {
		db = db.Where("op_hash = ?", query.OpHash)
	}
	if query.Status > 0 {
		db = db.Where("status = ?", query.Status)
	}
	if query.BaseTokenAddress != "" {
		db = db.Where("base_token_address = ?", query.BaseTokenAddress)
	}
	if query.MarketAddress != "" {
		db = db.Where("market_address = ?", query.MarketAddress)
	}
	if query.OptionAddress != "" {
		db = db.Where("option_address = ?", query.OptionAddress)
	}
	if query.EventProcessed > 0 {
		db = db.Where("event_processed = ?", query.EventProcessed)
	}
	if len(query.TxHashList) > 0 {
		db = db.Where("tx_hash IN ?", query.TxHashList)
	}
	return query.BaseQuery.Condition(db, total)
}

// Condition 交易查询条件
func (query *SendTxQuery) Condition(db *gorm.DB, total *int64) *gorm.DB {
	if query.UID != "" {
		db = db.Where("uid = ?", query.UID)
	}
	if query.TxHash != "" {
		db = db.Where("tx_hash = ?", query.TxHash)
	}
	if query.Chain != "" {
		db = db.Where("chain = ?", query.Chain)
	}
	if query.Status > 0 {
		db = db.Where("status = ?", query.Status)
	}
	if query.Type > 0 {
		db = db.Where("type = ?", query.Type)
	}
	if query.BaseTokenAddress != "" {
		db = db.Where("base_token_address = ?", query.BaseTokenAddress)
	}
	return query.BaseQuery.Condition(db, total)
}

// Condition 订单查询条件
func (query *OrderQuery) Condition(db *gorm.DB, total *int64) *gorm.DB {
	if query.UID != "" {
		db = db.Where("uid = ?", query.UID)
	}
	if query.MarketAddress != "" {
		db = db.Where("market_address = ?", query.MarketAddress)
	}
	if query.OptionAddress != "" {
		db = db.Where("option_address = ?", query.OptionAddress)
	}
	if query.BaseTokenAddress != "" {
		db = db.Where("base_token_address = ?", query.BaseTokenAddress)
	}
	if query.Status > 0 {
		db = db.Where("status = ?", query.Status)
	}
	if query.Side > 0 {
		db = db.Where("side = ?", query.Side)
	}
	if query.UUID != "" {
		db = db.Where("uuid = ?", query.UUID)
	}
	if query.EventProcessed > 0 {
		db = db.Where("event_processed = ?", query.EventProcessed)
	}
	if len(query.TxHashList) > 0 {
		db = db.Where("tx_hash IN ?", query.TxHashList)
	}
	if query.StartTime != "" {
		db = db.Where("created_at >= ?", query.StartTime)
	}
	if query.EndTime != "" {
		db = db.Where("created_at <= ?", query.EndTime)
	}
	return query.BaseQuery.Condition(db, total)
}

// Condition 用户代币余额查询条件
func (query *UserTokenBalanceQuery) Condition(db *gorm.DB, total *int64) *gorm.DB {
	if query.UID != "" {
		db = db.Where("uid = ?", query.UID)
	}
	if len(query.UIDList) > 0 {
		db = db.Where("uid IN ?", query.UIDList)
	}
	if query.MarketAddress != "" {
		db = db.Where("market_address = ?", query.MarketAddress)
	}
	if len(query.MarketAddressList) > 0 {
		db = db.Where("market_address IN ?", query.MarketAddressList)
	}
	if query.TokenAddress != "" {
		db = db.Where("token_address = ?", query.TokenAddress)
	}
	if len(query.TokenAddressList) > 0 {
		db = db.Where("token_address IN ?", query.TokenAddressList)
	}
	if query.Type > 0 {
		db = db.Where("type = ?", query.Type)
	}
	if query.BaseTokenAddress != "" {
		db = db.Where("base_token_address = ?", query.BaseTokenAddress)
	}
	if query.NoZero {
		db = db.Where("balance > ?", 0)
	}
	if query.Status > 0 {
		db = db.Where("status = ?", query.Status)
	}
	if len(query.StatusIn) > 0 {
		db = db.Where("status IN ?", query.StatusIn)
	}
	if query.StatusNotEqual > 0 {
		db = db.Where("status != ?", query.StatusNotEqual)
	}
	if !query.MinBalance.IsZero() {
		db = db.Where("balance >= ?", query.MinBalance)
	}
	return query.BaseQuery.Condition(db, total)
}

// Condition 用户资产总价值查询条件
func (query *UserAssetValueQuery) Condition(db *gorm.DB, total *int64) *gorm.DB {
	if query.UID != "" {
		db = db.Where("uid = ?", query.UID)
	}
	if query.AssetAddress != "" {
		db = db.Where("asset_address = ?", query.AssetAddress)
	}
	if query.BaseTokenAddress != "" {
		db = db.Where("base_token_address = ?", query.BaseTokenAddress)
	}
	if !query.StartTime.IsZero() {
		db = db.Where("time >= ?", query.StartTime)
	}
	if !query.EndTime.IsZero() {
		db = db.Where("time <= ?", query.EndTime)
	}
	return query.BaseQuery.Condition(db, total)
}

// Condition 用户市场持仓查询条件
func (query *UserMarketPositionQuery) Condition(db *gorm.DB, total *int64) *gorm.DB {
	if query.UID != "" {
		db = db.Where("uid = ?", query.UID)
	}
	if query.MarketAddress != "" {
		db = db.Where("market_address = ?", query.MarketAddress)
	}
	if query.BaseTokenAddress != "" {
		db = db.Where("base_token_address = ?", query.BaseTokenAddress)
	}
	return query.BaseQuery.Condition(db, total)
}
