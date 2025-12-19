package market

import (
	"market-service/internal/biz/base"
	marketBiz "market-service/internal/biz/market"
	"market-service/internal/model"
)

// Option 条件代币表结构
type Option struct {
	model.BaseModel
	Address       string `gorm:"column:address;type:varchar(42);uniqueIndex:idx_address;not null;" comment:"条件代币合约地址"`
	MarketAddress string `gorm:"column:market_address;type:varchar(42);not null;" comment:"属于的市场"`
	Name          string `gorm:"column:name;type:varchar(32);not null;default:''" comment:"代币名"`
	Symbol        string `gorm:"column:symbol;type:varchar(32);not null;default:''" comment:"代币符号标识"`
	Description   string `gorm:"column:description;type:varchar(256);not null;default:''" comment:"选项描述"`
	Decimal       uint8  `gorm:"column:decimal;type:smallint;not null;default:0" comment:"精度"`
	Weight        uint32 `gorm:"column:weight;type:integer;not null;default:0" comment:"权重"`
	Index         uint32 `gorm:"column:index;type:integer;not null;default:0" comment:"在市场的条件中的排序索引"`
	PicUrl        string `gorm:"column:pic_url;type:varchar(256);not null;default:''" comment:"图片url"`
	BaseTokenType uint8  `gorm:"column:base_token_type;type:smallint;not null;default:1" comment:"对应的基础代币类型 1: points 2: usdc"`
}

func (o *Option) ToEntity() *marketBiz.OptionEntity {
	return &marketBiz.OptionEntity{
		BaseEntity: base.BaseEntity{
			Id:        o.ID,
			CreatedAt: o.CreatedAt,
			UpdatedAt: o.UpdatedAt,
		},
		Address:       o.Address,
		MarketAddress: o.MarketAddress,
		Name:          o.Name,
		Symbol:        o.Symbol,
		Description:   o.Description,
		Decimal:       o.Decimal,
		Weight:        o.Weight,
		Index:         o.Index,
		PicUrl:        o.PicUrl,
		BaseTokenType: o.BaseTokenType,
	}
}

func (o *Option) FromEntity(entity *marketBiz.OptionEntity) {
	o.Address = entity.Address
	o.MarketAddress = entity.MarketAddress
	o.Name = entity.Name
	o.Symbol = entity.Symbol
	o.Description = entity.Description
	o.Decimal = entity.Decimal
	o.Weight = entity.Weight
	o.Index = entity.Index
	o.PicUrl = entity.PicUrl
	o.BaseTokenType = entity.BaseTokenType
	o.ID = entity.Id
	o.CreatedAt = entity.CreatedAt
	o.UpdatedAt = entity.UpdatedAt
}

// TableName 指定表名
func (Option) TableName() string {
	return "t_option"
}
