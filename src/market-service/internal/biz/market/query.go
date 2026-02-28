package market

import (
	"market-service/internal/biz/base"
	"time"

	"github.com/shopspring/decimal"
	"gorm.io/gorm"
)

// MarketQuery 市场查询结构体
type MarketQuery struct {
	base.BaseQuery
	Address       string   // 地址
	AddressList   []string // 地址列表
	TxHash        string   // 交易哈希
	IsShow        uint8    // 是否显示
	Name          string   // 市场名称
	NameLike      string   // 市场名称模糊查询
	DescLike      string   // 市场描述模糊查询
	Status        uint8    // 市场状态
	Search        string   // 搜索关键词
	Tag           string   // 标签
	TagList       []string // 标签列表
	IsNotDeadline bool     // 是否未到截止时间

	Embedding     []float64 // 向量搜索
	Category         string // 分类
	BaseTokenAddress string // 基础代币地址

	// 新增：用户关注相关字段
	OnlyFollowed bool   // 是否只查询关注的市场
	FollowUID    string // 关注的用户ID

	MinVolume   decimal.Decimal // 最小交易量
	MaxDeadline uint64          // 最大截止时间

	// CTF 筛选
	EventId  string   // 按 NegRisk Event ID 筛选
	EventIds []string // 按多个 Event ID 批量筛选
}

// OptionQuery 条件代币查询结构体
type OptionQuery struct {
	base.BaseQuery
	Address           string   // 地址
	AddressList       []string // 地址列表
	Symbol            string   // 符号
	MarketAddress     string   // 市场地址
	MarketAddressList []string // 市场地址列表
	Name              string   // 代币名称
}

// OptionTokenPriceQuery 条件代币价格查询结构体
type OptionTokenPriceQuery struct {
	base.BaseQuery
	TokenAddress string // 代币地址
	MinTimestamp uint64 // 最小时间戳
	MaxTimestamp uint64 // 最大时间戳
}

// UserMarketFollowQuery 用户关注市场查询结构体
type UserMarketFollowQuery struct {
	base.BaseQuery
	UID               string   // 用户ID
	MarketAddress     string   // 市场地址
	MarketAddressList []string // 市场地址列表
	Status            uint8    // 关注状态
}

type MarketTagQuery struct {
	base.BaseQuery
	Tag string // 标签
}

func (query *MarketTagQuery) Condition(db *gorm.DB, total *int64) *gorm.DB {
	if query.Tag != "" {
		db = db.Where("tag_name = ?", query.Tag)
	}
	return query.BaseQuery.Condition(db, total)
}

// Condition 市场查询条件
func (query *MarketQuery) Condition(db *gorm.DB, total *int64) *gorm.DB {
	// 处理JOIN和表别名
	tablePrefix := ""
	if query.OnlyFollowed && query.FollowUID != "" {
		db = db.Table("t_market m").
			Joins("INNER JOIN t_user_market_follow f ON m.address = f.market_address").
			Where("f.uid = ? AND f.status = ?", query.FollowUID, UserMarketFollowStatusActive)
		tablePrefix = "m."
	}

	// 统一应用所有条件
	db = query.applyConditions(db, tablePrefix)

	return query.BaseQuery.Condition(db, total)
}

// applyConditions 应用所有查询条件
func (query *MarketQuery) applyConditions(db *gorm.DB, prefix string) *gorm.DB {
	if len(query.AddressList) > 0 {
		db = db.Where(prefix+"address IN ?", query.AddressList)
	}
	if query.Address != "" {
		db = db.Where(prefix+"address = ?", query.Address)
	}
	if query.TxHash != "" {
		db = db.Where(prefix+"tx_hash = ?", query.TxHash)
	}
	if query.IsShow > 0 {
		db = db.Where(prefix+"is_show = ?", query.IsShow)
	}
	if query.Name != "" {
		db = db.Where(prefix+"name = ?", query.Name)
	}
	if query.NameLike != "" {
		db = db.Where(prefix+"name LIKE ?", "%"+query.NameLike+"%")
	}
	if query.DescLike != "" {
		db = db.Where(prefix+"description LIKE ?", "%"+query.DescLike+"%")
	}
	if query.Search != "" {
		db = db.Where("("+prefix+"address = ? OR "+prefix+"name iLIKE ? OR "+prefix+"description iLIKE ?)",
			query.Search, "%"+query.Search+"%", "%"+query.Search+"%")
	}
	if query.Tag != "" {
		db = db.Where(prefix + "tags ? '" + query.Tag + "'")
	}
	if query.IsNotDeadline {
		db = db.Where(prefix+"deadline > ?", time.Now().Unix())
	}
	if query.Status > 0 {
		db = db.Where(prefix+"status = ?", query.Status)
	}
	if query.Category != "" {
		db = db.Where(prefix + "categories ? '" + query.Category + "'")
	}
	if query.BaseTokenAddress != "" {
		db = db.Where(prefix+"base_token_address = ?", query.BaseTokenAddress)
	}
	if query.MinVolume.GreaterThan(decimal.Zero) {
		db = db.Where(prefix+"volume >= ?", query.MinVolume)
	}
	if query.MaxDeadline > 0 {
		db = db.Where(prefix+"deadline <= ?", query.MaxDeadline)
	}
	if query.EventId != "" {
		db = db.Where(prefix+"event_id = ?", query.EventId)
	}
	if len(query.EventIds) > 0 {
		db = db.Where(prefix+"event_id IN ?", query.EventIds)
	}
	return db
}

// Condition 条件代币查询条件
func (query *OptionQuery) Condition(db *gorm.DB, total *int64) *gorm.DB {
	if query.Address != "" {
		db = db.Where("address = ?", query.Address)
	}
	if len(query.AddressList) > 0 {
		db = db.Where("address IN ?", query.AddressList)
	}
	if query.Symbol != "" {
		db = db.Where("symbol = ?", query.Symbol)
	}
	if query.MarketAddress != "" {
		db = db.Where("market_address = ?", query.MarketAddress)
	}
	if len(query.MarketAddressList) > 0 {
		db = db.Where("market_address IN ?", query.MarketAddressList)
	}
	if query.Name != "" {
		db = db.Where("name = ?", query.Name)
	}
	return query.BaseQuery.Condition(db, total)
}

func (query *UserMarketFollowQuery) Condition(db *gorm.DB, total *int64) *gorm.DB {
	if query.UID != "" {
		db = db.Where("uid = ?", query.UID)
	}
	if query.MarketAddress != "" {
		db = db.Where("market_address = ?", query.MarketAddress)
	}
	if len(query.MarketAddressList) > 0 {
		db = db.Where("market_address IN ?", query.MarketAddressList)
	}
	if query.Status > 0 {
		db = db.Where("status = ?", query.Status)
	}
	return query.BaseQuery.Condition(db, total)
}

// Condition 条件代币价格查询条件
// PredictionEventQuery 预测事件查询结构体
type PredictionEventQuery struct {
	base.BaseQuery
	DbId    uint   // 按数据库 ID 查询
	EventId string // on-chain NegRisk eventId
	Status  uint8  // 事件状态
}

func (query *PredictionEventQuery) Condition(db *gorm.DB, total *int64) *gorm.DB {
	if query.DbId > 0 {
		db = db.Where("id = ?", query.DbId)
	}
	if query.EventId != "" {
		db = db.Where("event_id = ?", query.EventId)
	}
	if query.Status > 0 {
		db = db.Where("status = ?", query.Status)
	}
	return query.BaseQuery.Condition(db, total)
}

func (query *OptionTokenPriceQuery) Condition(db *gorm.DB, total *int64) *gorm.DB {
	if query.TokenAddress != "" {
		db = db.Where("token_address = ?", query.TokenAddress)
	}
	if query.MinTimestamp > 0 {
		db = db.Where("block_time >= ?", query.MinTimestamp)
	}
	if query.MaxTimestamp > 0 {
		db = db.Where("block_time <= ?", query.MaxTimestamp)
	}
	return query.BaseQuery.Condition(db, total)
}
