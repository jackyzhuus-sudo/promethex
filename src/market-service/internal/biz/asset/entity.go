package asset

import (
	"encoding/json"
	"errors"
	"market-service/internal/biz/base"
	"time"

	"github.com/shopspring/decimal"
)

const (
	SendTxStatusExecSuccess = uint8(1) // 执行成功
	SendTxStatusSendFailed  = uint8(2) // 上链失败
	SendTxStatusSending     = uint8(3) // 发送中

	OrderStatusSuccess = uint8(1)
	OrderStatusFailed  = uint8(2)
	OrderStatusPending = uint8(3)

	UserClaimResultStatusSuccess = uint8(1)
	UserClaimResultStatusFailed  = uint8(2)
	UserClaimResultStatusPending = uint8(3)

	UserMintPointsStatusSuccess = uint8(1)
	UserMintPointsStatusFailed  = uint8(2)
	UserMintPointsStatusPending = uint8(3)

	UserTransferTokensStatusSuccess = uint8(1)
	UserTransferTokensStatusFailed  = uint8(2)
	UserTransferTokensStatusPending = uint8(3)
)

const (
	LeaderboardKey = "leaderboard-%d-%s-%s"

	// 排行榜相关zset  %d -> baseTokenType(1:Points, 2:USDC)   %s -> timestr
	VolumeDailyLeaderboard = "leaderboard-%d-volume-daily-%s"
	TradesDailyLeaderboard = "leaderboard-%d-trades-daily-%s"
	PnlDailyLeaderboard    = "leaderboard-%d-pnl-daily-%s"

	VolumeWeeklyLeaderboard = "leaderboard-%d-volume-weekly-%s"
	TradesWeeklyLeaderboard = "leaderboard-%d-trades-weekly-%s"
	PnlWeeklyLeaderboard    = "leaderboard-%d-pnl-weekly-%s"

	VolumeMonthlyLeaderboard = "leaderboard-%d-volume-monthly-%s"
	TradesMonthlyLeaderboard = "leaderboard-%d-trades-monthly-%s"
	PnlMonthlyLeaderboard    = "leaderboard-%d-pnl-monthly-%s"

	VolumeAllTimeLeaderboard = "leaderboard-%d-volume-all-time"
	TradesAllTimeLeaderboard = "leaderboard-%d-trades-all-time"
	PnlAllTimeLeaderboard    = "leaderboard-%d-pnl-all-time"
)

const (
	TxSourceBuy                    = uint8(1)
	TxSourceSell                   = uint8(2)
	TxSourceUserClaim              = uint8(3)
	TxSourceMintInitPoins          = uint8(4)
	TxSourceMintInviteRewardPoints = uint8(5)
	TxSourceTransferDeposit        = uint8(6)
	TxSourceTransferWithdraw       = uint8(7)
	TxSourceMintTaskRewardPoints   = uint8(8)

	TxTypeUserOperation = uint8(1)
	TxTypeNormal        = uint8(2)

	OrderSideBuy  = uint8(1)
	OrderSideSell = uint8(2)

	ProcessedYes = uint8(1)
	ProcessedNo  = uint8(2)

	TypeUserTokenBalanceBaseAsset = uint8(1)
	TypeUserTokenBalanceOption    = uint8(2)

	UserTokenBalanceStatusHolding = uint8(1)
	UserTokenBalanceStatusEndWin  = uint8(2)
	UserTokenBalanceStatusEndLose = uint8(3)

	UserTokenBalanceIsClaimedNo  = uint8(2)
	UserTokenBalanceIsClaimedYes = uint8(1)

	BaseTokenTypePoints = uint8(1)
	BaseTokenTypeUsdc   = uint8(2)

	UserMarketPositionStatusHolding = uint8(1)
	UserMarketPositionStatusEnd     = uint8(2)

	UserMintPointsSourceNewUser   = uint8(1)
	UserMintPointsSourceInvite    = uint8(2)
	UserMintPointsSourceTaskClaim = uint8(3)

	UserTransferTokensSideDeposit  = uint8(1)
	UserTransferTokensSideWithdraw = uint8(2)

	UserOperationLockKey = "user-operation-send-tx-%s" // uid

	MintPointsLockKey = "mint_points_to_user"
)

var (
	MinPointBalance = decimal.NewFromInt(10000)
	MinUsdcBalance  = decimal.NewFromInt(10000)
)

type OrderEntity struct {
	base.BaseEntity
	UUID             string          `json:"uuid"`
	UID              string          `json:"uid"`
	MarketAddress    string          `json:"market_address"`
	OptionAddress    string          `json:"option_address"`
	Status           uint8           `json:"status"`
	EventProcessed   uint8           `json:"event_processed"`
	Amount           decimal.Decimal `json:"amount"`
	Price            decimal.Decimal `json:"price"`
	MinReceiveAmount decimal.Decimal `json:"min_receive_amount"`
	ReceiveAmount    decimal.Decimal `json:"receive_amount"`
	DealPrice        decimal.Decimal `json:"deal_price"`
	Deadline         time.Time       `json:"deadline"`
	Side             uint8           `json:"side"`
	TxHash           string          `json:"tx_hash"`
	OpHash           string          `json:"op_hash"`
	Pnl              decimal.Decimal `json:"pnl"`
	BaseTokenType    uint8           `json:"base_token_type"`

	Tx *SendTxEntity `json:"send_tx_entity"`
}

type UserClaimResultEntity struct {
	base.BaseEntity
	UUID           string          `json:"uuid"`
	UID            string          `json:"uid"`
	MarketAddress  string          `json:"market_address"`
	OptionAddress  string          `json:"option_address"`
	BaseTokenType  uint8           `json:"base_token_type"`
	Status         uint8           `json:"status"`
	Amount         decimal.Decimal `json:"amount"`
	EventProcessed uint8           `json:"event_processed"`
	TxHash         string          `json:"tx_hash"`
	OpHash         string          `json:"op_hash"`

	Tx *SendTxEntity `json:"send_tx_entity"`
}

type SendTxEntity struct {
	base.BaseEntity
	UID           string `json:"uid"`
	TxHash        string `json:"tx_hash"`
	OpHash        string `json:"op_hash"`
	Source        uint8  `json:"source"`
	Status        uint8  `json:"status"`
	Chain         string `json:"chain"`
	Type          uint8  `json:"type"`
	ErrMsg        string `json:"err_msg"`
	RetryCount    uint16 `json:"retry_count"`
	BaseTokenType uint8  `json:"base_token_type"`

	UserOperation *UserOperation `json:"user_operation"`
}

type UserTokenBalanceEntity struct {
	base.BaseEntity
	UID           string          `json:"uid"`
	TokenAddress  string          `json:"token_address"`
	MarketAddress string          `json:"market_address"`
	Balance       decimal.Decimal `json:"balance"`
	Decimal       uint8           `json:"decimal"`
	Type          uint8           `json:"type"`
	BaseTokenType uint8           `json:"base_token_type"`
	BlockNumber   uint64          `json:"block_number"`
	AvgBuyPrice   decimal.Decimal `json:"avg_buy_price"`
	Status        uint8           `json:"status"`
	IsClaimed     uint8           `json:"is_claimed"`
	TxHash        string          `json:"tx_hash"`

	FromAddress    string          `json:"from_address"`
	ToAddress      string          `json:"to_address"`
	Side           uint8           `json:"side"`
	TransferAmount decimal.Decimal `json:"transfer_amount"`
}

type UserTokenBalanceQueryItem struct {
	UID           string
	MarketAddress string
}

type UserAssetValueEntity struct {
	base.BaseEntity
	UID           string          `json:"uid"`
	AssetAddress  string          `json:"asset_address"`
	Value         decimal.Decimal `json:"value"`
	Balance       decimal.Decimal `json:"balance"`
	Portfolio     decimal.Decimal `json:"portfolio"`
	Pnl           decimal.Decimal `json:"pnl"`
	BaseTokenType uint8           `json:"base_token_type"`
	Time          time.Time       `json:"time"`

	PortfolioPnl decimal.Decimal `json:"portfolio_pnl"`
	SelfPnl      decimal.Decimal `json:"self_pnl"`
}

type UserMarketPositionEntity struct {
	base.BaseEntity
	UID           string          `json:"uid"`
	MarketAddress string          `json:"market_address"`
	BaseTokenType uint8           `json:"base_token_type"`
	TotalValue    decimal.Decimal `json:"total_value"`
	Status        uint8           `json:"status"`
}

// UserOperation 结构体定义
type UserOperation struct {
	// 基础字段
	Sender interface{} `json:"sender,omitempty"` // 合约钱包地址
	Nonce  interface{} `json:"nonce,omitempty"`  // 交易序号
	// InitCode             string `json:"initCode"`             // 合约部署代码
	CallData             interface{} `json:"callData,omitempty"`             // 调用数据
	CallGasLimit         interface{} `json:"callGasLimit,omitempty"`         // 执行gas限制
	VerificationGasLimit interface{} `json:"verificationGasLimit,omitempty"` // 验证gas限制
	PreVerificationGas   interface{} `json:"preVerificationGas,omitempty"`   // 预验证gas
	MaxFeePerGas         interface{} `json:"maxFeePerGas,omitempty"`         // 最大gas价格
	MaxPriorityFeePerGas interface{} `json:"maxPriorityFeePerGas,omitempty"` // 最大优先gas价格
	Signature            interface{} `json:"signature,omitempty"`            // 签名

	// 0.7新增字段
	Factory                       interface{} `json:"factory,omitempty"`                       // 工厂合约地址
	FactoryData                   interface{} `json:"factoryData,omitempty"`                   // 工厂数据
	Paymaster                     interface{} `json:"paymaster,omitempty"`                     // paymaster地址
	PaymasterData                 interface{} `json:"paymasterData,omitempty"`                 // paymaster数据
	PaymasterVerificationGasLimit interface{} `json:"paymasterVerificationGasLimit,omitempty"` // paymaster验证gas限制
	PaymasterPostOpGasLimit       interface{} `json:"paymasterPostOpGasLimit,omitempty"`       // paymaster后处理gas限制
}

type PaymasterData struct {
	PaymasterAddress              string `json:"paymaster_address"`
	PaymasterData                 string `json:"paymaster_data"`
	PaymasterVerificationGasLimit string `json:"paymaster_verification_gas_limit"`
	PreVerificationGas            string `json:"pre_verification_gas"`
	VerificationGasLimit          string `json:"verification_gas_limit"`
	CallGasLimit                  string `json:"call_gas_limit"`
}

// 验证UserOperation
func (op *UserOperation) Validate() error {
	// 验证必填字段
	if op.Sender == nil {
		return errors.New("sender is required")
	}
	if op.Nonce == nil {
		return errors.New("nonce is required")
	}
	if op.CallData == nil {
		return errors.New("callData is required")
	}
	if op.CallGasLimit == nil {
		return errors.New("callGasLimit is required")
	}
	if op.MaxFeePerGas == nil {
		return errors.New("maxFeePerGas is required")
	}
	if op.MaxPriorityFeePerGas == nil {
		return errors.New("maxPriorityFeePerGas is required")
	}
	if op.Signature == nil {
		return errors.New("signature is required")
	}
	return nil
}

// MarshalJSON 自定义JSON序列化方法，确保nil字段和空字符串不参与序列化
func (op *UserOperation) MarshalJSON() ([]byte, error) {
	m := make(map[string]interface{})

	// 检查是否为nil或空字符串的辅助函数
	isEmptyValue := func(v interface{}) bool {
		if v == nil {
			return true
		}
		// 检查是否为空字符串
		if s, ok := v.(string); ok && s == "" {
			return true
		}
		return false
	}

	// 添加基本字段（仅当不为nil且不为空字符串时）
	if !isEmptyValue(op.Sender) {
		m["sender"] = op.Sender
	}
	if !isEmptyValue(op.Nonce) {
		m["nonce"] = op.Nonce
	}
	if !isEmptyValue(op.CallData) {
		m["callData"] = op.CallData
	}
	if !isEmptyValue(op.CallGasLimit) {
		m["callGasLimit"] = op.CallGasLimit
	}
	if !isEmptyValue(op.VerificationGasLimit) {
		m["verificationGasLimit"] = op.VerificationGasLimit
	}
	if !isEmptyValue(op.PreVerificationGas) {
		m["preVerificationGas"] = op.PreVerificationGas
	}
	if !isEmptyValue(op.MaxFeePerGas) {
		m["maxFeePerGas"] = op.MaxFeePerGas
	}
	if !isEmptyValue(op.MaxPriorityFeePerGas) {
		m["maxPriorityFeePerGas"] = op.MaxPriorityFeePerGas
	}
	if !isEmptyValue(op.Signature) {
		m["signature"] = op.Signature
	}

	// 添加可选字段（仅当不为nil且不为空字符串时）
	if !isEmptyValue(op.Factory) {
		m["factory"] = op.Factory
	}
	if !isEmptyValue(op.FactoryData) {
		m["factoryData"] = op.FactoryData
	}
	if !isEmptyValue(op.Paymaster) {
		m["paymaster"] = op.Paymaster
	}
	if !isEmptyValue(op.PaymasterData) {
		m["paymasterData"] = op.PaymasterData
	}
	if !isEmptyValue(op.PaymasterVerificationGasLimit) {
		m["paymasterVerificationGasLimit"] = op.PaymasterVerificationGasLimit
	}
	if !isEmptyValue(op.PaymasterPostOpGasLimit) {
		m["paymasterPostOpGasLimit"] = op.PaymasterPostOpGasLimit
	}

	// 序列化最终的map
	return json.Marshal(m)
}

type MarketValue struct {
	MarketAddress string          `json:"market_address"`
	TotalValue    decimal.Decimal `json:"total_value"`
}

type UserTransferTokensEntity struct {
	base.BaseEntity
	UUID            string          `json:"uuid"`
	UID             string          `json:"uid"`
	TokenAddress    string          `json:"token_address"`
	ExternalAddress string          `json:"external_address"`
	Side            uint8           `json:"side"`
	BaseTokenType   uint8           `json:"base_token_type"`
	Amount          decimal.Decimal `json:"amount"`
	Status          uint8           `json:"status"`
	EventProcessed  uint8           `json:"event_processed"`
	TxHash          string          `json:"tx_hash"`
	OpHash          string          `json:"op_hash"`

	Tx *SendTxEntity `json:"send_tx_entity"`
}

type UserMintPointsEntity struct {
	base.BaseEntity
	UUID           string          `json:"uuid"`
	UID            string          `json:"uid"`
	TokenAddress   string          `json:"token_address"`
	BaseTokenType  uint8           `json:"base_token_type"`
	Amount         decimal.Decimal `json:"amount"`
	Status         uint8           `json:"status"`
	EventProcessed uint8           `json:"event_processed"`
	Source         uint8           `json:"source"`
	TxHash         string          `json:"tx_hash"`
	OpHash         string          `json:"op_hash"`
	InviteUID      string          `json:"invite_uid"`
	UserTaskUUID   string          `json:"user_task_uuid"`

	Tx *SendTxEntity `json:"send_tx_entity"`
}

// TxDataBuy 买入交易数据 (type = 1)
type TxDataBuy struct {
	// 市场地址
	MarketAddress string `json:"marketAddress"`
	// 市场名称
	MarketName string `json:"marketName"`
	// 市场图片
	MarketPicUrl string `json:"marketPicUrl"`
	// 市场描述
	MarketDescription string `json:"marketDescription"`
	// 选项地址
	OptionAddress string `json:"optionAddress"`
	// 选项名称
	OptionName string `json:"optionName"`
	// 选项描述
	OptionDescription string `json:"optionDescription"`
}

// TxDataSell 卖出交易数据 (type = 2)
type TxDataSell struct {
	// 市场地址
	MarketAddress string `json:"marketAddress"`
	// 市场名称
	MarketName string `json:"marketName"`
	// 市场图片
	MarketPicUrl string `json:"marketPicUrl"`
	// 市场描述
	MarketDescription string `json:"marketDescription"`
	// 选项地址
	OptionAddress string `json:"optionAddress"`
	// 选项名称
	OptionName string `json:"optionName"`
	// 选项描述
	OptionDescription string `json:"optionDescription"`
}

// TxDataClaim 申领交易数据 (type = 3)
type TxDataClaim struct {
	// 市场地址
	MarketAddress string `json:"marketAddress"`
	// 市场名称
	MarketName string `json:"marketName"`
	// 市场图片
	MarketPicUrl string `json:"marketPicUrl"`
	// 市场描述
	MarketDescription string `json:"marketDescription"`
	// 选项地址
	OptionAddress string `json:"optionAddress"`
	// 选项名称
	OptionName string `json:"optionName"`
	// 选项描述
	OptionDescription string `json:"optionDescription"`
}

// TxDataMintPoints mint积分交易数据 (type = 4)
type TxDataMintPoints struct {
}

// TxDataMintInvitePoints 邀请积分mint交易数据 (type = 5)
type TxDataMintInvitePoints struct {
	// 被邀请用户uid
	InvitedUid string `json:"invitedUid"`
	// 被邀请用户名称
	InvitedName string `json:"invitedName"`
	// 被邀请用户头像
	InvitedAvator string `json:"invitedAvator"`
}

// TxDataDeposit 充值交易数据 (type = 6)
type TxDataDeposit struct {
	// 转入地址
	Address string `json:"address"`
}

// TxDataWithdraw 提现交易数据 (type = 7)
type TxDataWithdraw struct {
	// 转出地址
	Address string `json:"address"`
}

// TxDataMintTaskRewardPoints 领取任务奖励mint交易数据 (type = 8)
type TxDataMintTaskRewardPoints struct {
	// 任务uuid
	TaskUUID string `json:"taskUuid"`
	// 任务key
	TaskKey string `json:"taskKey"`
	// 任务名称
	TaskName string `json:"taskName"`
	// 任务奖励
	Reward uint64 `json:"reward"`
	// 用户任务uuid
	UserTaskUUID string `json:"userTaskUuid"`
}
