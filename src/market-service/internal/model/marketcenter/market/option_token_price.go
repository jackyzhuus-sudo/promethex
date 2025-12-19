package market

import (
	"market-service/internal/biz/base"
	marketBiz "market-service/internal/biz/market"
	"market-service/internal/model"
	"time"

	"github.com/shopspring/decimal"
)

// OptionTokenPrice 代币价格表结构
type OptionTokenPrice struct {
	model.BaseModel
	BlockTime     time.Time       `gorm:"column:block_time;type:timestamptz;not null;" comment:"区块时间"`
	TokenAddress  string          `gorm:"column:token_address;type:varchar(42);uniqueIndex:idx_token_address_block_number;not null;" comment:"条件代币地址"`
	BlockNumber   uint64          `gorm:"column:block_number;type:bigint;uniqueIndex:idx_token_address_block_number;not null;default:0" comment:"区块高度"`
	Price         decimal.Decimal `gorm:"column:price;type:NUMERIC;not null;default:0" comment:"代币价格"`
	Decimals      uint8           `gorm:"column:decimals;type:smallint;not null;default:0" comment:"代币小数位"`
	BaseTokenType uint8           `gorm:"column:base_token_type;type:smallint;not null;default:1" comment:"对应的基础代币类型 1: points 2: usdc"`
}

func (o *OptionTokenPrice) ToEntity() *marketBiz.OptionTokenPriceEntity {
	return &marketBiz.OptionTokenPriceEntity{
		TokenAddress:  o.TokenAddress,
		Price:         o.Price,
		BlockNumber:   o.BlockNumber,
		BlockTime:     o.BlockTime,
		BaseTokenType: o.BaseTokenType,
		Decimals:      o.Decimals,
		BaseEntity: base.BaseEntity{
			Id:        o.ID,
			CreatedAt: o.CreatedAt,
			UpdatedAt: o.UpdatedAt,
		},
	}
}

func (o *OptionTokenPrice) FromEntity(entity *marketBiz.OptionTokenPriceEntity) {
	o.TokenAddress = entity.TokenAddress
	o.Price = entity.Price
	o.BlockNumber = entity.BlockNumber
	o.BlockTime = entity.BlockTime
	o.BaseTokenType = entity.BaseTokenType
	o.Decimals = entity.Decimals
	o.ID = entity.Id
	o.CreatedAt = entity.CreatedAt
	o.UpdatedAt = entity.UpdatedAt
}

// TableName 指定表名
func (OptionTokenPrice) TableName() string {
	return "t_option_token_price"
}
