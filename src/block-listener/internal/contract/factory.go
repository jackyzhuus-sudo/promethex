package contract

import (
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/go-kratos/kratos/v2/log"
)

// 工厂合约事件类型
const (
	EventTypePredictionCreated           = "PredictionCreated"
	EventTypeImplementationUpdated       = "ImplementationUpdated"
	EventTypeOptionImplementationUpdated = "OptionImplementationUpdated"
)

// 事件结构体定义
type PredictionCreatedEvent struct {
	EventBase
	Prediction common.Address
	Creator    common.Address
}

type ImplementationUpdatedEvent struct {
	EventType   string
	OldImpl     common.Address
	NewImpl     common.Address
	BlockNumber uint64
	TxHash      common.Hash
	LogIndex    uint
}

type OptionImplementationUpdatedEvent struct {
	EventType   string
	OldImpl     common.Address
	NewImpl     common.Address
	BlockNumber uint64
	TxHash      common.Hash
	LogIndex    uint
}

type UnknownFactoryEvent struct {
	EventType   string
	RawData     []byte
	Topics      []common.Hash
	BlockNumber uint64
	TxHash      common.Hash
	LogIndex    uint
}

// FactoryContract 用于解析工厂合约事件
type FactoryContract struct {
	abi                  abi.ABI
	log                  *log.Helper
	eventSignatureToType map[string]string // 事件签名到类型的映射
	eventTypeToSignature map[string]string // 事件类型到签名的映射
}

// NewFactoryContract 创建新的工厂合约事件解析器
func NewFactoryContract(logger log.Logger) (*FactoryContract, error) {
	parser := &FactoryContract{
		log:                  log.NewHelper(logger),
		eventSignatureToType: make(map[string]string),
		eventTypeToSignature: make(map[string]string),
	}

	var err error
	parser.abi, err = abi.JSON(strings.NewReader(FactoryAbiJson))
	if err != nil {
		parser.log.Errorf("解析工厂合约ABI失败: %v", err)
		return nil, err
	}

	// 动态计算事件签名
	for _, event := range parser.abi.Events {
		parser.eventSignatureToType[event.ID.Hex()] = event.Name
		parser.eventTypeToSignature[event.Name] = event.ID.Hex()
	}

	return parser, nil
}

// GetEventType 根据事件签名获取事件类型
func (p *FactoryContract) GetEventType(topicHex string) string {
	if eventType, exists := p.eventSignatureToType[topicHex]; exists {
		return eventType
	}
	return "Unknown"
}

func (p *FactoryContract) GetEventSignature(eventType string) string {
	if signature, exists := p.eventTypeToSignature[eventType]; exists {
		return signature
	}
	return ""
}

// GetAllEventSignatures 返回所有已注册的事件签名
func (p *FactoryContract) GetAllEventSignatures() []common.Hash {
	signatures := make([]common.Hash, 0, len(p.eventSignatureToType))
	for sigHex := range p.eventSignatureToType {
		signatures = append(signatures, common.HexToHash(sigHex))
	}
	return signatures
}

// ParseEvent 解析工厂合约事件
func (p *FactoryContract) ParseEvent(log types.Log) (interface{}, error) {
	// 检查日志是否有足够的topics
	if len(log.Topics) == 0 {
		return nil, nil
	}

	// 获取事件类型
	eventType := p.GetEventType(log.Topics[0].Hex())
	if eventType == "Unknown" {
		return &UnknownFactoryEvent{
			EventType:   "Unknown",
			RawData:     log.Data,
			Topics:      log.Topics,
			BlockNumber: log.BlockNumber,
			TxHash:      log.TxHash,
			LogIndex:    log.Index,
		}, nil
	}

	// 解析事件数据
	switch eventType {
	case EventTypePredictionCreated:
		return p.ParsePredictionCreatedEvent(log)
	case EventTypeImplementationUpdated:
		return p.ParseImplementationUpdatedEvent(log)
	case EventTypeOptionImplementationUpdated:
		return p.ParseOptionImplementationUpdatedEvent(log)
	default:
		return &UnknownFactoryEvent{
			EventType:   eventType,
			RawData:     log.Data,
			Topics:      log.Topics,
			BlockNumber: log.BlockNumber,
			TxHash:      log.TxHash,
			LogIndex:    log.Index,
		}, nil
	}
}

// 以下是各事件的解析函数

func (p *FactoryContract) ParsePredictionCreatedEvent(log types.Log) (*PredictionCreatedEvent, error) {
	// PredictionCreated事件格式：PredictionCreated(address indexed prediction, address indexed creator)
	if len(log.Topics) < 3 {
		p.log.Errorf("解析PredictionCreated事件失败: topics不足")
		return nil, nil
	}

	// 从indexed参数(topics)中获取prediction和creator地址
	prediction := common.BytesToAddress(log.Topics[1].Bytes())
	creator := common.BytesToAddress(log.Topics[2].Bytes())

	return &PredictionCreatedEvent{
		EventBase: EventBase{
			EventType:   EventTypePredictionCreated,
			Address:     log.Address,
			Txhash:      log.TxHash,
			BlockNumber: log.BlockNumber,
			BlockTime:   time.Unix(int64(log.BlockNumber), 0),
		},
		Prediction: prediction,
		Creator:    creator,
	}, nil
}

func (p *FactoryContract) ParseImplementationUpdatedEvent(log types.Log) (*ImplementationUpdatedEvent, error) {
	// ImplementationUpdated事件格式：ImplementationUpdated(address indexed oldImpl, address indexed newImpl)
	if len(log.Topics) < 3 {
		p.log.Errorf("解析ImplementationUpdated事件失败: topics不足")
		return nil, nil
	}

	// 从indexed参数(topics)中获取oldImpl和newImpl地址
	oldImpl := common.BytesToAddress(log.Topics[1].Bytes())
	newImpl := common.BytesToAddress(log.Topics[2].Bytes())

	return &ImplementationUpdatedEvent{
		EventType:   EventTypeImplementationUpdated,
		OldImpl:     oldImpl,
		NewImpl:     newImpl,
		BlockNumber: log.BlockNumber,
		TxHash:      log.TxHash,
		LogIndex:    log.Index,
	}, nil
}

func (p *FactoryContract) ParseOptionImplementationUpdatedEvent(log types.Log) (*OptionImplementationUpdatedEvent, error) {
	// OptionImplementationUpdated事件格式：OptionImplementationUpdated(address indexed oldImpl, address indexed newImpl)
	if len(log.Topics) < 3 {
		p.log.Errorf("解析OptionImplementationUpdated事件失败: topics不足")
		return nil, nil
	}

	// 从indexed参数(topics)中获取oldImpl和newImpl地址
	oldImpl := common.BytesToAddress(log.Topics[1].Bytes())
	newImpl := common.BytesToAddress(log.Topics[2].Bytes())

	return &OptionImplementationUpdatedEvent{
		EventType:   EventTypeOptionImplementationUpdated,
		OldImpl:     oldImpl,
		NewImpl:     newImpl,
		BlockNumber: log.BlockNumber,
		TxHash:      log.TxHash,
		LogIndex:    log.Index,
	}, nil
}
