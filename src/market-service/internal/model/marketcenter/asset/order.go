package asset

import (
	assetBiz "market-service/internal/biz/asset"
	"market-service/internal/biz/base"
	"market-service/internal/model"
	"time"

	"github.com/shopspring/decimal"
)

// Order 订单表结构
type Order struct {
	model.BaseModel
	UUID             string          `gorm:"column:uuid;type:varchar(36);uniqueIndex:idx_uuid;not null;" comment:"订单唯一标识"`
	UID              string          `gorm:"column:uid;type:varchar(24);not null;" comment:"用户ID"`
	MarketAddress    string          `gorm:"column:market_address;type:varchar(42);not null;" comment:"市场地址"`
	OptionAddress    string          `gorm:"column:option_address;type:varchar(42);not null;" comment:"交易的条件代币地址"`
	Side             uint8           `gorm:"column:side;type:smallint;not null;default:0" comment:"交易方向 1买 2卖"`
	Price            decimal.Decimal `gorm:"column:price;type:NUMERIC;not null;default:0" comment:"下单时希望的价格"`
	DealPrice        decimal.Decimal `gorm:"column:deal_price;type:NUMERIC;not null;default:0" comment:"成交时价格"`
	Amount           decimal.Decimal `gorm:"column:amount;type:NUMERIC;not null;default:0" comment:"deposit买 amount为投入的基础代币数量 / withdraw卖 amount为投入的条件代币数量"`
	MinReceiveAmount decimal.Decimal `gorm:"column:min_receive_amount;type:NUMERIC;not null;default:0" comment:"deposit买 min_receive_amount为最小接收的条件代币数量 / withdraw卖 min_receive_amount为最小接收的基础代币数量"`
	ReceiveAmount    decimal.Decimal `gorm:"column:receive_amount;type:NUMERIC;not null;default:0" comment:"deposit买 receive_amount为实际接收的条件代币数量 / withdraw卖 receive_amount为实际接收的基础代币数量"`
	Status           uint8           `gorm:"column:status;type:smallint;not null;default:3" comment:"1成功2失败3执行中"`
	EventProcessed   uint8           `gorm:"column:event_processed;type:smallint;not null;default:2" comment:"事件是否处理过 1已处理 2未处理"`
	TxHash           string          `gorm:"column:tx_hash;type:varchar(66);uniqueIndex:idx_tx_hash;not null;default:''" comment:"交易上链 tx_hash"`
	OpHash           string          `gorm:"column:op_hash;type:varchar(66);not null;default:''" comment:"userOp hash"`
	Deadline         time.Time       `gorm:"column:deadline;type:TIMESTAMPTZ;not null;default:0" comment:"截止时间"`
	Pnl              decimal.Decimal `gorm:"column:pnl;type:NUMERIC;not null;default:0" comment:"卖出实现的盈亏 买入不填为0"`
	BaseTokenAddress string          `gorm:"column:base_token_address" comment:"基础资产代币合约地址"`
}

func (o *Order) ToEntity() *assetBiz.OrderEntity {
	return &assetBiz.OrderEntity{
		BaseEntity: base.BaseEntity{
			Id:        o.ID,
			CreatedAt: o.CreatedAt,
			UpdatedAt: o.UpdatedAt,
		},
		UUID:             o.UUID,
		UID:              o.UID,
		MarketAddress:    o.MarketAddress,
		OptionAddress:    o.OptionAddress,
		Side:             o.Side,
		Price:            o.Price,
		DealPrice:        o.DealPrice,
		Amount:           o.Amount,
		MinReceiveAmount: o.MinReceiveAmount,
		ReceiveAmount:    o.ReceiveAmount,
		Status:           o.Status,
		EventProcessed:   o.EventProcessed,
		Pnl:              o.Pnl,
		BaseTokenAddress: o.BaseTokenAddress,
		TxHash:           o.TxHash,
		OpHash:           o.OpHash,
		Deadline:         o.Deadline,
	}
}

func (o *Order) FromEntity(entity *assetBiz.OrderEntity) {
	o.UUID = entity.UUID
	o.UID = entity.UID
	o.MarketAddress = entity.MarketAddress
	o.OptionAddress = entity.OptionAddress
	o.Side = entity.Side
	o.Price = entity.Price
	o.DealPrice = entity.DealPrice
	o.Amount = entity.Amount
	o.MinReceiveAmount = entity.MinReceiveAmount
	o.ReceiveAmount = entity.ReceiveAmount
	o.Status = entity.Status
	o.EventProcessed = entity.EventProcessed
	o.Pnl = entity.Pnl
	o.TxHash = entity.TxHash
	o.OpHash = entity.OpHash
	o.Deadline = entity.Deadline
	o.BaseTokenAddress = entity.BaseTokenAddress
	o.ID = entity.Id
	o.CreatedAt = entity.CreatedAt
	o.UpdatedAt = entity.UpdatedAt
}

// TableName 指定表名
func (Order) TableName() string {
	return "t_order"
}
