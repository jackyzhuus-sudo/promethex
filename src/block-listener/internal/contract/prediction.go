package contract

import (
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/go-kratos/kratos/v2/log"
)

// 预测市场合约方法名常量 - 根据合约实际定义更新
const (
	// 基本信息查询方法
	MethodDescription = "description"
	MethodOwner       = "owner"
	MethodBaseToken   = "baseToken" // 对应合约中的baseToken
	MethodMAXOPTION   = "MAX_OPTION"
	MethodOracle      = "oracle"
	MethodAssertTime  = "assertTime"

	MethodOptionsAt    = "options" // 函数 options(uint256 index) - 获取指定索引的Option
	MethodOptionsArray = "options" // 函数 options() - 获取整个options数组
	MethodWeightsAt    = "weights" // 函数 weights(uint256 index)
	MethodPrice        = "price"   // 函数 price(uint256 option)
)

// 预测市场状态 - 与合约中的Status枚举对应
type PredictionMarketStatus uint8

// 预测市场合约信息 - 与合约中的字段保持一致
type PredictionMarketInfo struct {
	Address     common.Address   // 预测市场合约地址
	Description string           // 市场描述
	BaseToken   common.Address   // 基础代币地址（在合约中为baseToken）
	OptionCount uint64           // 选项数量
	AssertTime  uint64           // 断言时间
	AssertionId common.Hash      // UMA断言ID
	Options     []common.Address // 选项代币列表
	Oracle      common.Address   // UMA预言机地址
	Weights     []*big.Int       // 选项权重列表
	BlockNumber uint64           // 预测市场创建区块号

	OptionsInfo []*OptionInfo
}

// Prediction合约事件类型
const (
	EventTypeLiquidityAdded    = "LiquidityAdded"
	EventTypeLiquidityRemoved  = "LiquidityRemoved"
	EventTypeSwapped           = "Swapped"
	EventTypeSettling          = "Settling"
	EventTypeClaimed           = "Claimed"
	EventTypeDeposited         = "Deposited"
	EventTypeWithdrawn         = "Withdrawn"
	EventTypeAssertionDisputed = "AssertionDisputed"
	EventTypeAssertionResolved = "AssertionResolved"
	EventTypeFeeSet            = "FeeSet"
	EventTypeOwnerSet          = "OwnerSet"
)

type EventBase struct {
	EventType   string
	Address     common.Address
	Txhash      common.Hash
	BlockNumber uint64
	BlockTime   time.Time
}

// 事件结构体定义
type LiquidityAddedEvent struct {
	EventBase
	User     common.Address
	To       common.Address
	Amount   *big.Int
	LpAmount *big.Int
}

type LiquidityRemovedEvent struct {
	EventBase
	User     common.Address
	Amount   *big.Int
	LpAmount *big.Int
}

type SwappedEvent struct {
	EventBase
	User      common.Address
	OptionIn  uint64
	OptionOut uint64
	AmountIn  *big.Int
	AmountOut *big.Int
}

type SettlingEvent struct {
	EventBase
	FinalOption uint64
	AssertionId []byte
}

type ClaimedEvent struct {
	EventBase
	User   common.Address
	Option uint64
	Amount *big.Int
}

type DepositedEvent struct {
	EventBase
	User      common.Address
	OptionOut uint64
	AmountIn  *big.Int
	AmountOut *big.Int
}

type WithdrawnEvent struct {
	EventBase
	User      common.Address
	OptionIn  uint64
	AmountIn  *big.Int
	AmountOut *big.Int
}

type AssertionDisputedEvent struct {
	EventBase
	AssertionId common.Hash
}

type AssertionResolvedEvent struct {
	EventBase
	AssertionId        common.Hash
	AssertedTruthfully bool
}

type FeeSetEvent struct {
	EventBase
	OldFee *big.Int
	NewFee *big.Int
}

type OwnerSetEvent struct {
	EventBase
	OldOwner common.Address
	NewOwner common.Address
}

type UnknownEvent struct {
	EventBase
	RawData []byte
	Topics  []common.Hash
}

// PredictionContract 用于解析Prediction合约事件
type PredictionContract struct {
	abi                       abi.ABI
	log                       *log.Helper
	predictionEventSignatures map[string]string // 签名哈希 -> 事件名
	predictionEventNames      map[string]string // 事件名 -> 签名哈希
}

// NewPredictionContract 创建新的Prediction事件解析器
func NewPredictionContract(logger log.Logger) (*PredictionContract, error) {
	parser := &PredictionContract{
		log:                       log.NewHelper(logger),
		predictionEventSignatures: make(map[string]string),
		predictionEventNames:      make(map[string]string),
	}

	var err error
	parser.abi, err = abi.JSON(strings.NewReader(PredictionAbiJson))
	if err != nil {
		return nil, fmt.Errorf("解析Prediction ABI失败 [%s]", err.Error())
	}

	// 动态计算事件签名
	for _, event := range parser.abi.Events {
		sigHex := event.ID.Hex()
		parser.predictionEventSignatures[sigHex] = event.Name
		parser.predictionEventNames[event.Name] = sigHex
	}

	return parser, nil
}

func (p *PredictionContract) GetEventSignature(eventType string) string {
	if signature, exists := p.predictionEventNames[eventType]; exists {
		return signature
	}
	return ""
}

// GetEventType 根据事件签名获取事件类型
func (p *PredictionContract) GetEventType(topicHex string) string {
	if eventType, exists := p.predictionEventSignatures[topicHex]; exists {
		return eventType
	}
	return "Unknown"
}

// GetAllEventSignatures 返回所有已注册的事件签名
// 这个方法可以用于批量监听事件时构建过滤条件
func (p *PredictionContract) GetAllEventSignatures() []common.Hash {
	signatures := make([]common.Hash, 0, len(p.predictionEventSignatures))
	for sigHex := range p.predictionEventSignatures {
		signatures = append(signatures, common.HexToHash(sigHex))
	}
	return signatures
}

// GetABI 返回解析器使用的ABI
func (p *PredictionContract) GetABI() abi.ABI {
	return p.abi
}

// ParseEvent 解析Prediction合约事件
func (p *PredictionContract) ParseEvent(log types.Log) (interface{}, error) {

	// 检查日志是否有足够的topics
	if len(log.Topics) == 0 {
		return nil, fmt.Errorf("解析Prediction合约事件失败: topics不足")
	}

	// 获取事件类型
	eventType := p.GetEventType(log.Topics[0].Hex())
	if eventType == "Unknown" {
		return &UnknownEvent{
			EventBase: EventBase{
				EventType:   "Unknown",
				Address:     log.Address,
				Txhash:      log.TxHash,
				BlockNumber: log.BlockNumber,
				BlockTime:   time.Unix(int64(log.BlockNumber), 0),
			},
			RawData: log.Data,
			Topics:  log.Topics,
		}, nil
	}

	// 解析事件数据
	switch eventType {
	case EventTypeLiquidityAdded:
		return p.ParseLiquidityAddedEvent(log)
	case EventTypeLiquidityRemoved:
		return p.ParseLiquidityRemovedEvent(log)
	case EventTypeSwapped:
		return p.ParseSwappedEvent(log)
	case EventTypeSettling:
		return p.ParseSettlingEvent(log)
	case EventTypeClaimed:
		return p.ParseClaimedEvent(log)
	case EventTypeDeposited:
		return p.ParseDepositedEvent(log)
	case EventTypeWithdrawn:
		return p.ParseWithdrawnEvent(log)
	case EventTypeAssertionDisputed:
		return p.ParseAssertionDisputedEvent(log)
	case EventTypeAssertionResolved:
		return p.ParseAssertionResolvedEvent(log)
	case EventTypeFeeSet:
		return p.ParseFeeSetEvent(log)
	case EventTypeOwnerSet:
		return p.ParseOwnerSetEvent(log)
	default:
		return &UnknownEvent{
			EventBase: EventBase{
				EventType:   eventType,
				Address:     log.Address,
				Txhash:      log.TxHash,
				BlockNumber: log.BlockNumber,
				BlockTime:   time.Unix(int64(log.BlockNumber), 0),
			},
			RawData: log.Data,
			Topics:  log.Topics,
		}, nil
	}
}

// 以下是各事件的解析函数

func (p *PredictionContract) ParseLiquidityAddedEvent(log types.Log) (*LiquidityAddedEvent, error) {
	// LiquidityAdded事件格式：LiquidityAdded(address user, address to, uint256 amount, uint256 lpAmount)
	// 所有参数都是非indexed，都在data中
	if len(log.Data) < 128 { // 四个参数：两个地址(32字节)+两个uint256(32字节) = 128字节
		return nil, fmt.Errorf("解析LiquidityAdded事件失败: data不足")
	}

	// 从data中解析所有参数
	user := common.BytesToAddress(log.Data[0:32])
	to := common.BytesToAddress(log.Data[32:64])
	amount := new(big.Int).SetBytes(log.Data[64:96])
	lpAmount := new(big.Int).SetBytes(log.Data[96:128])

	return &LiquidityAddedEvent{
		EventBase: EventBase{
			EventType:   EventTypeLiquidityAdded,
			Address:     log.Address,
			Txhash:      log.TxHash,
			BlockNumber: log.BlockNumber,
			BlockTime:   time.Unix(int64(log.BlockNumber), 0),
		},
		User:     user,
		To:       to,
		Amount:   amount,
		LpAmount: lpAmount,
	}, nil
}

func (p *PredictionContract) ParseLiquidityRemovedEvent(log types.Log) (*LiquidityRemovedEvent, error) {
	// LiquidityRemoved事件格式：LiquidityRemoved(address user, uint256 amount, uint256 lpAmount)
	// 所有参数都是非indexed，都在data中
	if len(log.Data) < 96 { // 一个地址(32字节)+两个uint256(32字节) = 96字节
		return nil, fmt.Errorf("解析LiquidityRemoved事件失败: data不足")
	}

	// 从data中解析所有参数
	user := common.BytesToAddress(log.Data[0:32])
	amount := new(big.Int).SetBytes(log.Data[32:64])
	lpAmount := new(big.Int).SetBytes(log.Data[64:96])

	return &LiquidityRemovedEvent{
		EventBase: EventBase{
			EventType:   EventTypeLiquidityRemoved,
			Address:     log.Address,
			Txhash:      log.TxHash,
			BlockNumber: log.BlockNumber,
			BlockTime:   time.Unix(int64(log.BlockNumber), 0),
		},
		User:     user,
		Amount:   amount,
		LpAmount: lpAmount,
	}, nil
}

func (p *PredictionContract) ParseSwappedEvent(log types.Log) (*SwappedEvent, error) {
	// Swapped事件格式：Swapped(address user, uint256 optionIn, uint256 optionOut, uint256 amountIn, uint256 amountOut)
	// 所有参数都是非indexed，都在data中
	if len(log.Data) < 160 { // 一个地址(32字节)+四个uint256(32字节) = 160字节
		return nil, fmt.Errorf("解析Swapped事件失败: data不足")
	}

	// 从data中解析所有参数
	user := common.BytesToAddress(log.Data[0:32])
	optionInBig := new(big.Int).SetBytes(log.Data[32:64])
	optionOutBig := new(big.Int).SetBytes(log.Data[64:96])
	amountIn := new(big.Int).SetBytes(log.Data[96:128])
	amountOut := new(big.Int).SetBytes(log.Data[128:160])

	// 确保大整数可以安全转换为uint64
	var optionIn, optionOut uint64
	if optionInBig.IsUint64() && optionOutBig.IsUint64() {
		optionIn = optionInBig.Uint64()
		optionOut = optionOutBig.Uint64()
	} else {
		return nil, fmt.Errorf("解析Swapped事件失败: optionIn或optionOut超出uint64范围")
	}

	return &SwappedEvent{
		EventBase: EventBase{
			EventType:   EventTypeSwapped,
			Address:     log.Address,
			Txhash:      log.TxHash,
			BlockNumber: log.BlockNumber,
			BlockTime:   time.Unix(int64(log.BlockNumber), 0),
		},
		User:      user,
		OptionIn:  optionIn,
		OptionOut: optionOut,
		AmountIn:  amountIn,
		AmountOut: amountOut,
	}, nil
}

func (p *PredictionContract) ParseSettlingEvent(log types.Log) (*SettlingEvent, error) {
	// Settling事件格式：Settling(uint256 finalOption, bytes32 assertionId)
	// 所有参数都是非indexed，都在data中
	if len(log.Data) < 64 { // 两个参数，每个32字节
		return nil, fmt.Errorf("解析Settling事件失败: data不足")
	}

	// 从data中解析所有参数
	finalOptionBig := new(big.Int).SetBytes(log.Data[0:32])
	assertionId := log.Data[32:64]

	// 确保大整数可以安全转换为uint64
	var finalOption uint64
	if finalOptionBig.IsUint64() {
		finalOption = finalOptionBig.Uint64()
	} else {
		return nil, fmt.Errorf("解析Settling事件失败: finalOption超出uint64范围")
	}

	return &SettlingEvent{
		EventBase: EventBase{
			EventType:   EventTypeSettling,
			Address:     log.Address,
			Txhash:      log.TxHash,
			BlockNumber: log.BlockNumber,
			BlockTime:   time.Unix(int64(log.BlockNumber), 0),
		},
		FinalOption: finalOption,
		AssertionId: assertionId,
	}, nil
}

func (p *PredictionContract) ParseClaimedEvent(log types.Log) (*ClaimedEvent, error) {
	// Claimed事件格式：Claimed(address user, uint256 option, uint256 amount)
	// 所有参数都是非indexed，都在data中
	if len(log.Data) < 96 { // 一个地址(32字节)+两个uint256(32字节) = 96字节
		return nil, fmt.Errorf("解析Claimed事件失败: data不足. address: %s, txhash: %s", log.Address, log.TxHash)
	}

	// 从data中解析所有参数
	user := common.BytesToAddress(log.Data[0:32])
	optionBig := new(big.Int).SetBytes(log.Data[32:64])
	amount := new(big.Int).SetBytes(log.Data[64:96])

	// 确保大整数可以安全转换为uint64
	var option uint64
	if optionBig.IsUint64() {
		option = optionBig.Uint64()
	} else {
		return nil, fmt.Errorf("解析Claimed事件失败: option超出uint64范围, address: %s, txhash: %s", log.Address, log.TxHash)
	}

	return &ClaimedEvent{
		EventBase: EventBase{
			EventType:   EventTypeClaimed,
			Address:     log.Address,
			Txhash:      log.TxHash,
			BlockNumber: log.BlockNumber,
			BlockTime:   time.Unix(int64(log.BlockNumber), 0),
		},
		User:   user,
		Option: option,
		Amount: amount,
	}, nil
}

func (p *PredictionContract) ParseDepositedEvent(log types.Log) (*DepositedEvent, error) {
	// Deposited事件格式：Deposited(address user, uint256 optionOut, uint256 amountIn, uint256 amountOut)
	// 所有参数都是非indexed，都在data中
	if len(log.Data) < 128 { // 一个地址(32字节)+三个uint256(32字节) = 128字节
		return nil, fmt.Errorf("解析Deposited事件失败: data不足")
	}

	// 从data中解析所有参数
	user := common.BytesToAddress(log.Data[0:32])
	optionOutBig := new(big.Int).SetBytes(log.Data[32:64])
	amountIn := new(big.Int).SetBytes(log.Data[64:96])
	amountOut := new(big.Int).SetBytes(log.Data[96:128])

	// 确保大整数可以安全转换为uint64
	var optionOut uint64
	if optionOutBig.IsUint64() {
		optionOut = optionOutBig.Uint64()
	} else {
		return nil, fmt.Errorf("解析Deposited事件失败: optionOut超出uint64范围")
	}

	return &DepositedEvent{
		EventBase: EventBase{
			EventType:   EventTypeDeposited,
			Address:     log.Address,
			Txhash:      log.TxHash,
			BlockNumber: log.BlockNumber,
			BlockTime:   time.Unix(int64(log.BlockNumber), 0),
		},
		User:      user,
		OptionOut: optionOut,
		AmountIn:  amountIn,
		AmountOut: amountOut,
	}, nil
}

func (p *PredictionContract) ParseWithdrawnEvent(log types.Log) (*WithdrawnEvent, error) {
	// Withdrawn事件格式：Withdrawn(address user, uint256 optionIn, uint256 amountIn, uint256 amountOut)
	// 所有参数都是非indexed，都在data中
	if len(log.Data) < 128 { // 一个地址(32字节)+三个uint256(32字节) = 128字节
		return nil, fmt.Errorf("解析Withdrawn事件失败: data不足")
	}

	// 从data中解析所有参数
	user := common.BytesToAddress(log.Data[0:32])
	optionInBig := new(big.Int).SetBytes(log.Data[32:64])
	amountIn := new(big.Int).SetBytes(log.Data[64:96])
	amountOut := new(big.Int).SetBytes(log.Data[96:128])

	// 确保大整数可以安全转换为uint64
	var optionIn uint64
	if optionInBig.IsUint64() {
		optionIn = optionInBig.Uint64()
	} else {
		return nil, fmt.Errorf("解析Withdrawn事件失败: optionIn超出uint64范围")
	}

	return &WithdrawnEvent{
		EventBase: EventBase{
			EventType:   EventTypeWithdrawn,
			Address:     log.Address,
			Txhash:      log.TxHash,
			BlockNumber: log.BlockNumber,
			BlockTime:   time.Unix(int64(log.BlockNumber), 0),
		},
		User:      user,
		OptionIn:  optionIn,
		AmountIn:  amountIn,
		AmountOut: amountOut,
	}, nil
}

func (p *PredictionContract) ParseAssertionDisputedEvent(log types.Log) (*AssertionDisputedEvent, error) {
	// AssertionDisputed事件格式：AssertionDisputed(bytes32 assertionId)
	// 所有参数都是非indexed，都在data中
	if len(log.Data) < 32 { // 一个bytes32参数，32字节
		return nil, fmt.Errorf("解析AssertionDisputed事件失败: data不足")
	}

	// 从data中解析参数
	assertionId := common.BytesToHash(log.Data[0:32])

	return &AssertionDisputedEvent{
		EventBase: EventBase{
			EventType:   EventTypeAssertionDisputed,
			Address:     log.Address,
			Txhash:      log.TxHash,
			BlockNumber: log.BlockNumber,
			BlockTime:   time.Unix(int64(log.BlockNumber), 0),
		},
		AssertionId: assertionId,
	}, nil
}

func (p *PredictionContract) ParseAssertionResolvedEvent(log types.Log) (*AssertionResolvedEvent, error) {
	// AssertionResolved事件格式：AssertionResolved(bytes32 assertionId, bool assertedTruthfully)
	// 所有参数都是非indexed，都在data中
	if len(log.Data) < 64 { // 一个bytes32(32字节)+一个bool(32字节) = 64字节
		return nil, fmt.Errorf("解析AssertionResolved事件失败: data不足")
	}

	// 从data中解析参数
	assertionId := common.BytesToHash(log.Data[0:32])
	// 布尔值在EVM中以uint256表示，非零值为true
	assertedTruthfully := new(big.Int).SetBytes(log.Data[32:64]).Cmp(big.NewInt(0)) != 0

	return &AssertionResolvedEvent{
		EventBase: EventBase{
			EventType:   EventTypeAssertionResolved,
			Address:     log.Address,
			Txhash:      log.TxHash,
			BlockNumber: log.BlockNumber,
			BlockTime:   time.Unix(int64(log.BlockNumber), 0),
		},
		AssertionId:        assertionId,
		AssertedTruthfully: assertedTruthfully,
	}, nil
}

func (p *PredictionContract) ParseFeeSetEvent(log types.Log) (*FeeSetEvent, error) {
	// FeeSet事件格式：FeeSet(uint256 oldFee, uint256 newFee)
	// 所有参数都是非indexed，都在data中
	if len(log.Data) < 64 { // 两个uint256参数，每个32字节
		return nil, fmt.Errorf("解析FeeSet事件失败: data不足")
	}

	// 从data中解析参数
	oldFee := new(big.Int).SetBytes(log.Data[0:32])
	newFee := new(big.Int).SetBytes(log.Data[32:64])

	return &FeeSetEvent{
		EventBase: EventBase{
			EventType:   EventTypeFeeSet,
			Address:     log.Address,
			Txhash:      log.TxHash,
			BlockNumber: log.BlockNumber,
			BlockTime:   time.Unix(int64(log.BlockNumber), 0),
		},
		OldFee: oldFee,
		NewFee: newFee,
	}, nil
}

func (p *PredictionContract) ParseOwnerSetEvent(log types.Log) (*OwnerSetEvent, error) {
	// OwnerSet事件格式：OwnerSet(address oldOwner, address newOwner)
	// 所有参数都是非indexed，都在data中
	if len(log.Data) < 64 { // 两个地址参数，每个32字节
		return nil, fmt.Errorf("解析OwnerSet事件失败: data不足")
	}

	// 从data中解析参数
	oldOwner := common.BytesToAddress(log.Data[0:32])
	newOwner := common.BytesToAddress(log.Data[32:64])

	return &OwnerSetEvent{
		EventBase: EventBase{
			EventType:   EventTypeOwnerSet,
			Address:     log.Address,
			Txhash:      log.TxHash,
			BlockNumber: log.BlockNumber,
			BlockTime:   time.Unix(int64(log.BlockNumber), 0),
		},
		OldOwner: oldOwner,
		NewOwner: newOwner,
	}, nil
}
