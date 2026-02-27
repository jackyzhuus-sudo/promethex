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

// PredictionCTF合约方法名常量
const (
	MethodConditionId      = "conditionId"
	MethodOutcomeSlotCount = "outcomeSlotCount"
)

// PredictionCTF合约事件类型
const (
	EventTypeCTFLiquidityAdded   = "LiquidityAdded"
	EventTypeCTFLiquidityRemoved = "LiquidityRemoved"
	EventTypeCTFSwapped          = "Swapped"
	EventTypeCTFDeposited        = "Deposited"
	EventTypeCTFWithdrawn        = "Withdrawn"
	EventTypeCTFMarketResolved   = "MarketResolved"
	EventTypeCTFFeeSet           = "FeeSet"
	EventTypeCTFFeeCollected     = "FeeCollected"
)

// PredictionCTF市场信息
type PredictionCTFMarketInfo struct {
	Address          common.Address // 池合约地址
	Description      string         // 市场描述
	BaseToken        common.Address // 基础代币地址
	ConditionId      common.Hash    // CTF条件ID
	OutcomeSlotCount uint64         // 结果槽数量
	Oracle           common.Address // 预言机地址
	BlockNumber      uint64         // 创建区块号
}

// 事件结构体定义 - CTF特有的事件使用新结构体

type CTFLiquidityRemovedEvent struct {
	EventBase
	User            common.Address
	Amount          *big.Int
	LpAmount        *big.Int
	ExcessPositions []*big.Int
}

type CTFMarketResolvedEvent struct {
	EventBase
}

type CTFFeeCollectedEvent struct {
	EventBase
	Collector common.Address
	Amount    *big.Int
}

// PredictionCTFContract 用于解析PredictionCTF合约事件
type PredictionCTFContract struct {
	abi                  abi.ABI
	log                  *log.Helper
	eventSignatureToType map[string]string
	eventTypeToSignature map[string]string
}

// NewPredictionCTFContract 创建新的PredictionCTF事件解析器
func NewPredictionCTFContract(logger log.Logger) (*PredictionCTFContract, error) {
	parser := &PredictionCTFContract{
		log:                  log.NewHelper(logger),
		eventSignatureToType: make(map[string]string),
		eventTypeToSignature: make(map[string]string),
	}

	var err error
	parser.abi, err = abi.JSON(strings.NewReader(PredictionCTFAbiJson))
	if err != nil {
		return nil, fmt.Errorf("解析PredictionCTF ABI失败: %v", err)
	}

	// 动态计算事件签名
	for _, event := range parser.abi.Events {
		parser.eventSignatureToType[event.ID.Hex()] = event.Name
		parser.eventTypeToSignature[event.Name] = event.ID.Hex()
	}

	return parser, nil
}

// GetEventType 根据事件签名获取事件类型
func (p *PredictionCTFContract) GetEventType(topicHex string) string {
	if eventType, exists := p.eventSignatureToType[topicHex]; exists {
		return eventType
	}
	return "Unknown"
}

// GetEventSignature 根据事件类型获取事件签名
func (p *PredictionCTFContract) GetEventSignature(eventType string) string {
	if signature, exists := p.eventTypeToSignature[eventType]; exists {
		return signature
	}
	return ""
}

// GetAllEventSignatures 返回所有已注册的事件签名
func (p *PredictionCTFContract) GetAllEventSignatures() []common.Hash {
	signatures := make([]common.Hash, 0, len(p.eventSignatureToType))
	for sigHex := range p.eventSignatureToType {
		signatures = append(signatures, common.HexToHash(sigHex))
	}
	return signatures
}

// GetABI 返回解析器使用的ABI
func (p *PredictionCTFContract) GetABI() abi.ABI {
	return p.abi
}

// ParseEvent 解析PredictionCTF合约事件
func (p *PredictionCTFContract) ParseEvent(log types.Log) (interface{}, error) {
	if len(log.Topics) == 0 {
		return nil, fmt.Errorf("解析PredictionCTF事件失败: topics不足")
	}

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

	switch eventType {
	case EventTypeCTFLiquidityAdded:
		return p.ParseLiquidityAddedEvent(log)
	case EventTypeCTFLiquidityRemoved:
		return p.ParseLiquidityRemovedEvent(log)
	case EventTypeCTFSwapped:
		return p.ParseSwappedEvent(log)
	case EventTypeCTFDeposited:
		return p.ParseDepositedEvent(log)
	case EventTypeCTFWithdrawn:
		return p.ParseWithdrawnEvent(log)
	case EventTypeCTFMarketResolved:
		return p.ParseMarketResolvedEvent(log)
	case EventTypeCTFFeeSet:
		return p.ParseFeeSetEvent(log)
	case EventTypeCTFFeeCollected:
		return p.ParseFeeCollectedEvent(log)
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

// ParseLiquidityAddedEvent 解析LiquidityAdded事件
// LiquidityAdded(address user, address to, uint256 amount, uint256 lpAmount)
// 与legacy Prediction合约相同，复用LiquidityAddedEvent结构体
func (p *PredictionCTFContract) ParseLiquidityAddedEvent(log types.Log) (*LiquidityAddedEvent, error) {
	if len(log.Data) < 128 {
		return nil, fmt.Errorf("解析CTF LiquidityAdded事件失败: data不足")
	}

	user := common.BytesToAddress(log.Data[0:32])
	to := common.BytesToAddress(log.Data[32:64])
	amount := new(big.Int).SetBytes(log.Data[64:96])
	lpAmount := new(big.Int).SetBytes(log.Data[96:128])

	return &LiquidityAddedEvent{
		EventBase: EventBase{
			EventType:   EventTypeCTFLiquidityAdded,
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

// ParseLiquidityRemovedEvent 解析LiquidityRemoved事件
// LiquidityRemoved(address user, uint256 amount, uint256 lpAmount, uint256[] excessPositions)
// CTF版本多了excessPositions参数
func (p *PredictionCTFContract) ParseLiquidityRemovedEvent(log types.Log) (*CTFLiquidityRemovedEvent, error) {
	type liquidityRemovedData struct {
		User            common.Address
		Amount          *big.Int
		LpAmount        *big.Int
		ExcessPositions []*big.Int
	}
	var decoded liquidityRemovedData
	err := p.abi.UnpackIntoInterface(&decoded, EventTypeCTFLiquidityRemoved, log.Data)
	if err != nil {
		return nil, fmt.Errorf("解析CTF LiquidityRemoved事件data失败: %v", err)
	}

	return &CTFLiquidityRemovedEvent{
		EventBase: EventBase{
			EventType:   EventTypeCTFLiquidityRemoved,
			Address:     log.Address,
			Txhash:      log.TxHash,
			BlockNumber: log.BlockNumber,
			BlockTime:   time.Unix(int64(log.BlockNumber), 0),
		},
		User:            decoded.User,
		Amount:          decoded.Amount,
		LpAmount:        decoded.LpAmount,
		ExcessPositions: decoded.ExcessPositions,
	}, nil
}

// ParseSwappedEvent 解析Swapped事件
// Swapped(address user, uint256 optionIn, uint256 optionOut, uint256 amountIn, uint256 amountOut)
// 与legacy Prediction合约相同，复用SwappedEvent结构体
func (p *PredictionCTFContract) ParseSwappedEvent(log types.Log) (*SwappedEvent, error) {
	if len(log.Data) < 160 {
		return nil, fmt.Errorf("解析CTF Swapped事件失败: data不足")
	}

	user := common.BytesToAddress(log.Data[0:32])
	optionInBig := new(big.Int).SetBytes(log.Data[32:64])
	optionOutBig := new(big.Int).SetBytes(log.Data[64:96])
	amountIn := new(big.Int).SetBytes(log.Data[96:128])
	amountOut := new(big.Int).SetBytes(log.Data[128:160])

	var optionIn, optionOut uint64
	if optionInBig.IsUint64() && optionOutBig.IsUint64() {
		optionIn = optionInBig.Uint64()
		optionOut = optionOutBig.Uint64()
	} else {
		return nil, fmt.Errorf("解析CTF Swapped事件失败: optionIn或optionOut超出uint64范围")
	}

	return &SwappedEvent{
		EventBase: EventBase{
			EventType:   EventTypeCTFSwapped,
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

// ParseDepositedEvent 解析Deposited事件
// Deposited(address user, uint256 optionOut, uint256 amountIn, uint256 amountOut)
// 与legacy Prediction合约相同，复用DepositedEvent结构体
func (p *PredictionCTFContract) ParseDepositedEvent(log types.Log) (*DepositedEvent, error) {
	if len(log.Data) < 128 {
		return nil, fmt.Errorf("解析CTF Deposited事件失败: data不足")
	}

	user := common.BytesToAddress(log.Data[0:32])
	optionOutBig := new(big.Int).SetBytes(log.Data[32:64])
	amountIn := new(big.Int).SetBytes(log.Data[64:96])
	amountOut := new(big.Int).SetBytes(log.Data[96:128])

	var optionOut uint64
	if optionOutBig.IsUint64() {
		optionOut = optionOutBig.Uint64()
	} else {
		return nil, fmt.Errorf("解析CTF Deposited事件失败: optionOut超出uint64范围")
	}

	return &DepositedEvent{
		EventBase: EventBase{
			EventType:   EventTypeCTFDeposited,
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

// ParseWithdrawnEvent 解析Withdrawn事件
// Withdrawn(address user, uint256 optionIn, uint256 amountIn, uint256 amountOut)
// 与legacy Prediction合约相同，复用WithdrawnEvent结构体
func (p *PredictionCTFContract) ParseWithdrawnEvent(log types.Log) (*WithdrawnEvent, error) {
	if len(log.Data) < 128 {
		return nil, fmt.Errorf("解析CTF Withdrawn事件失败: data不足")
	}

	user := common.BytesToAddress(log.Data[0:32])
	optionInBig := new(big.Int).SetBytes(log.Data[32:64])
	amountIn := new(big.Int).SetBytes(log.Data[64:96])
	amountOut := new(big.Int).SetBytes(log.Data[96:128])

	var optionIn uint64
	if optionInBig.IsUint64() {
		optionIn = optionInBig.Uint64()
	} else {
		return nil, fmt.Errorf("解析CTF Withdrawn事件失败: optionIn超出uint64范围")
	}

	return &WithdrawnEvent{
		EventBase: EventBase{
			EventType:   EventTypeCTFWithdrawn,
			Address:     log.Address,
			Txhash:      log.TxHash,
			BlockNumber: log.BlockNumber,
			BlockTime:   time.Unix(int64(log.BlockNumber), 0),
		},
		User:     user,
		OptionIn: optionIn,
		AmountIn: amountIn,
		AmountOut: amountOut,
	}, nil
}

// ParseMarketResolvedEvent 解析MarketResolved事件
// MarketResolved()
func (p *PredictionCTFContract) ParseMarketResolvedEvent(log types.Log) (*CTFMarketResolvedEvent, error) {
	return &CTFMarketResolvedEvent{
		EventBase: EventBase{
			EventType:   EventTypeCTFMarketResolved,
			Address:     log.Address,
			Txhash:      log.TxHash,
			BlockNumber: log.BlockNumber,
			BlockTime:   time.Unix(int64(log.BlockNumber), 0),
		},
	}, nil
}

// ParseFeeSetEvent 解析FeeSet事件
// FeeSet(uint256 oldFee, uint256 newFee)
// 与legacy Prediction合约相同，复用FeeSetEvent结构体
func (p *PredictionCTFContract) ParseFeeSetEvent(log types.Log) (*FeeSetEvent, error) {
	if len(log.Data) < 64 {
		return nil, fmt.Errorf("解析CTF FeeSet事件失败: data不足")
	}

	oldFee := new(big.Int).SetBytes(log.Data[0:32])
	newFee := new(big.Int).SetBytes(log.Data[32:64])

	return &FeeSetEvent{
		EventBase: EventBase{
			EventType:   EventTypeCTFFeeSet,
			Address:     log.Address,
			Txhash:      log.TxHash,
			BlockNumber: log.BlockNumber,
			BlockTime:   time.Unix(int64(log.BlockNumber), 0),
		},
		OldFee: oldFee,
		NewFee: newFee,
	}, nil
}

// ParseFeeCollectedEvent 解析FeeCollected事件
// FeeCollected(address indexed collector, uint256 amount)
func (p *PredictionCTFContract) ParseFeeCollectedEvent(log types.Log) (*CTFFeeCollectedEvent, error) {
	if len(log.Topics) < 2 {
		return nil, fmt.Errorf("解析CTF FeeCollected事件失败: topics不足")
	}
	if len(log.Data) < 32 {
		return nil, fmt.Errorf("解析CTF FeeCollected事件失败: data不足")
	}

	collector := common.BytesToAddress(log.Topics[1].Bytes())
	amount := new(big.Int).SetBytes(log.Data[0:32])

	return &CTFFeeCollectedEvent{
		EventBase: EventBase{
			EventType:   EventTypeCTFFeeCollected,
			Address:     log.Address,
			Txhash:      log.TxHash,
			BlockNumber: log.BlockNumber,
			BlockTime:   time.Unix(int64(log.BlockNumber), 0),
		},
		Collector: collector,
		Amount:    amount,
	}, nil
}
