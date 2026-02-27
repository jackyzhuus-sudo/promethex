package market

import (
	"market-service/internal/biz/base"
	"time"

	"github.com/shopspring/decimal"
)

var (
	MarketShow = uint8(1)
	MarketHide = uint8(2)

	BaseTokenTypePoints = uint8(1)
	BaseTokenTypeUsdc   = uint8(2)

	UserMarketFollowStatusActive   = uint8(1)
	UserMarketFollowStatusInactive = uint8(2)

	MarketStatusRunnig   = uint8(1)
	MarketStatusSettling = uint8(2)
	MarketStatusDisputed = uint8(3)
	MarketStatusEnd      = uint8(4)

	EventStatusActive   = uint8(1)
	EventStatusResolved = uint8(2)
	EventStatusVoided   = uint8(3)
	EventStatusFrozen   = uint8(4)
)

const (
	S3MarketCategoriesDefaultKey = "Category-List_Default"
	S3MarketCategoriesUsdcKey    = "Category-List_USDC"

	S3MarketBannersDefaultKey = "Banners_Default"
	S3MarketBannersUsdcKey    = "Banners_USDC"

	S3MarketSectionsDefaultKey = "Section-List_Default"
	S3MarketSectionsUsdcKey    = "Section-List_USDC"
)

type MarketEntity struct {
	base.BaseEntity
	Address           string          `json:"address"`
	TxHash            string          `json:"tx_hash"`
	Name              string          `json:"name"`
	Fee               uint32          `json:"fee"`
	TokenType         uint8           `json:"token_type"`
	OracleAddress     string          `json:"oracle_address"`
	InitLiquidity     decimal.Decimal `json:"init_liquidity"`
	PicUrl            string          `json:"pic_url"`
	Deadline          uint64          `json:"deadline"`
	Volume            decimal.Decimal `json:"volume"`
	ParticipantsCount uint64          `json:"participants_count"`
	Result            string          `json:"result"`
	IsShow            uint8           `json:"is_show"`
	AssertionId       []byte          `json:"assertion_id"`
	Description       string          `json:"description"`
	Rules             string          `json:"rules"`
	RulesUrl          string          `json:"rules_url"`
	Status            uint8           `json:"status"`
	Tags              []string        `json:"tags"`
	Categories        []string        `json:"categories"`
	Embedding         []float64       `json:"embedding"`
	BlockNumber       uint64          `json:"block_number"`

	// CTF fields
	EventId          string `json:"event_id"`
	ConditionId      string `json:"condition_id"`
	QuestionId       string `json:"question_id"`
	OutcomeSlotCount int32  `json:"outcome_slot_count"`

	IsFollowed uint8 `json:"is_followed"`

	Options []*OptionEntity `json:"options"`
}

type TagEntity struct {
	base.BaseEntity
	TagName string `json:"tag_name"`
}

type UserMarketFollowEntity struct {
	base.BaseEntity
	UID           string `json:"uid"`
	MarketAddress string `json:"market_address"`
	BaseTokenType uint8  `json:"base_token_type"`
	Status        uint8  `json:"status"`
}

type OptionEntity struct {
	base.BaseEntity
	MarketAddress string `json:"market_address"`
	BaseTokenType uint8  `json:"base_token_type"`
	Address       string `json:"address"`
	Name          string `json:"name"`
	Symbol        string `json:"symbol"`
	Description   string `json:"description"`
	Decimal       uint8  `json:"decimal"`
	Weight        uint32 `json:"weight"`
	Index         uint32 `json:"index"`
	PicUrl        string `json:"pic_url"`
	PositionId    string `json:"position_id"` // ERC1155 token ID

	OptionTokenPrice *OptionTokenPriceEntity `json:"option_token_price"`
}
type OptionTokenPriceEntity struct {
	base.BaseEntity
	TokenAddress  string          `json:"token_address"`
	Price         decimal.Decimal `json:"price"`
	BlockNumber   uint64          `json:"block_number"`
	BlockTime     time.Time       `json:"block_time"`
	BaseTokenType uint8           `json:"base_token_type"`
	Decimals      uint8           `json:"decimals"`
}

// PredictionEventEntity 表示一个 NegRisk Event
type PredictionEventEntity struct {
	base.BaseEntity
	EventId          string `json:"event_id"`
	Title            string `json:"title"`
	OutcomeSlotCount int32  `json:"outcome_slot_count"`
	Collateral       string `json:"collateral"`
	Status           uint8  `json:"status"`
	MetadataHash     string `json:"metadata_hash"`

	Markets []*MarketEntity `json:"markets"` // 子市场列表
}

// TokenPricePoint 表示一个时间点上的所有token价格
type TokenPricePoint struct {
	Timestamp   time.Time         `json:"timestamp" gorm:"column:timestamp"`
	TokenPrices map[string]string `json:"token_prices" gorm:"type:jsonb;column:token_prices"`
}
