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

// ConditionalTokens合约事件类型
const (
	EventTypeConditionPreparation = "ConditionPreparation"
	EventTypeConditionResolution  = "ConditionResolution"
	EventTypePositionSplit        = "PositionSplit"
	EventTypePositionsMerge       = "PositionsMerge"
	EventTypePayoutRedemption     = "PayoutRedemption"
	EventTypeTransferSingle       = "TransferSingle"
	EventTypeTransferBatch        = "TransferBatch"
)

// 事件结构体定义

type ConditionPreparationEvent struct {
	EventBase
	ConditionId      common.Hash
	Oracle           common.Address
	QuestionId       common.Hash
	OutcomeSlotCount uint64
}

type ConditionResolutionEvent struct {
	EventBase
	ConditionId      common.Hash
	Oracle           common.Address
	QuestionId       common.Hash
	OutcomeSlotCount uint64
	PayoutNumerators []*big.Int
}

type PositionSplitEvent struct {
	EventBase
	Stakeholder        common.Address
	CollateralToken    common.Address
	ParentCollectionId common.Hash
	ConditionId        common.Hash
	Partition          []*big.Int
	Amount             *big.Int
}

type PositionsMergeEvent struct {
	EventBase
	Stakeholder        common.Address
	CollateralToken    common.Address
	ParentCollectionId common.Hash
	ConditionId        common.Hash
	Partition          []*big.Int
	Amount             *big.Int
}

type PayoutRedemptionEvent struct {
	EventBase
	Redeemer           common.Address
	CollateralToken    common.Address
	ParentCollectionId common.Hash
	ConditionId        common.Hash
	IndexSets          []*big.Int
	Payout             *big.Int
}

type TransferSingleEvent struct {
	EventBase
	Operator common.Address
	From     common.Address
	To       common.Address
	Id       *big.Int
	Value    *big.Int
}

type TransferBatchEvent struct {
	EventBase
	Operator common.Address
	From     common.Address
	To       common.Address
	Ids      []*big.Int
	Values   []*big.Int
}

type UnknownCTFEvent struct {
	EventBase
	RawData []byte
	Topics  []common.Hash
}

// ConditionalTokensContract 用于解析ConditionalTokens合约事件
type ConditionalTokensContract struct {
	abi                  abi.ABI
	log                  *log.Helper
	eventSignatureToType map[string]string
	eventTypeToSignature map[string]string
}

// NewConditionalTokensContract 创建新的ConditionalTokens事件解析器
func NewConditionalTokensContract(logger log.Logger) (*ConditionalTokensContract, error) {
	parser := &ConditionalTokensContract{
		log:                  log.NewHelper(logger),
		eventSignatureToType: make(map[string]string),
		eventTypeToSignature: make(map[string]string),
	}

	var err error
	parser.abi, err = abi.JSON(strings.NewReader(ConditionalTokensAbiJson))
	if err != nil {
		return nil, fmt.Errorf("解析ConditionalTokens ABI失败: %v", err)
	}

	// 动态计算事件签名
	for _, event := range parser.abi.Events {
		parser.eventSignatureToType[event.ID.Hex()] = event.Name
		parser.eventTypeToSignature[event.Name] = event.ID.Hex()
	}

	return parser, nil
}

// GetEventType 根据事件签名获取事件类型
func (p *ConditionalTokensContract) GetEventType(topicHex string) string {
	if eventType, exists := p.eventSignatureToType[topicHex]; exists {
		return eventType
	}
	return "Unknown"
}

// GetEventSignature 根据事件类型获取事件签名
func (p *ConditionalTokensContract) GetEventSignature(eventType string) string {
	if signature, exists := p.eventTypeToSignature[eventType]; exists {
		return signature
	}
	return ""
}

// GetAllEventSignatures 返回所有已注册的事件签名
func (p *ConditionalTokensContract) GetAllEventSignatures() []common.Hash {
	signatures := make([]common.Hash, 0, len(p.eventSignatureToType))
	for sigHex := range p.eventSignatureToType {
		signatures = append(signatures, common.HexToHash(sigHex))
	}
	return signatures
}

// GetABI 返回解析器使用的ABI
func (p *ConditionalTokensContract) GetABI() abi.ABI {
	return p.abi
}

// ParseEvent 解析ConditionalTokens合约事件
func (p *ConditionalTokensContract) ParseEvent(log types.Log) (interface{}, error) {
	if len(log.Topics) == 0 {
		return nil, fmt.Errorf("解析ConditionalTokens事件失败: topics不足")
	}

	eventType := p.GetEventType(log.Topics[0].Hex())
	if eventType == "Unknown" {
		return &UnknownCTFEvent{
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
	case EventTypeConditionPreparation:
		return p.ParseConditionPreparationEvent(log)
	case EventTypeConditionResolution:
		return p.ParseConditionResolutionEvent(log)
	case EventTypePositionSplit:
		return p.ParsePositionSplitEvent(log)
	case EventTypePositionsMerge:
		return p.ParsePositionsMergeEvent(log)
	case EventTypePayoutRedemption:
		return p.ParsePayoutRedemptionEvent(log)
	case EventTypeTransferSingle:
		return p.ParseTransferSingleEvent(log)
	case EventTypeTransferBatch:
		return p.ParseTransferBatchEvent(log)
	default:
		return &UnknownCTFEvent{
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

// ParseConditionPreparationEvent 解析ConditionPreparation事件
// ConditionPreparation(bytes32 indexed conditionId, address indexed oracle, bytes32 indexed questionId, uint256 outcomeSlotCount)
func (p *ConditionalTokensContract) ParseConditionPreparationEvent(log types.Log) (*ConditionPreparationEvent, error) {
	if len(log.Topics) < 4 {
		return nil, fmt.Errorf("解析ConditionPreparation事件失败: topics不足")
	}
	if len(log.Data) < 32 {
		return nil, fmt.Errorf("解析ConditionPreparation事件失败: data不足")
	}

	conditionId := log.Topics[1]
	oracle := common.BytesToAddress(log.Topics[2].Bytes())
	questionId := log.Topics[3]
	outcomeSlotCount := new(big.Int).SetBytes(log.Data[0:32]).Uint64()

	return &ConditionPreparationEvent{
		EventBase: EventBase{
			EventType:   EventTypeConditionPreparation,
			Address:     log.Address,
			Txhash:      log.TxHash,
			BlockNumber: log.BlockNumber,
			BlockTime:   time.Unix(int64(log.BlockNumber), 0),
		},
		ConditionId:      conditionId,
		Oracle:           oracle,
		QuestionId:       questionId,
		OutcomeSlotCount: outcomeSlotCount,
	}, nil
}

// ParseConditionResolutionEvent 解析ConditionResolution事件
// ConditionResolution(bytes32 indexed conditionId, address indexed oracle, bytes32 indexed questionId, uint256 outcomeSlotCount, uint256[] payoutNumerators)
func (p *ConditionalTokensContract) ParseConditionResolutionEvent(log types.Log) (*ConditionResolutionEvent, error) {
	if len(log.Topics) < 4 {
		return nil, fmt.Errorf("解析ConditionResolution事件失败: topics不足")
	}

	conditionId := log.Topics[1]
	oracle := common.BytesToAddress(log.Topics[2].Bytes())
	questionId := log.Topics[3]

	// 使用ABI解码data部分 (outcomeSlotCount + payoutNumerators dynamic array)
	type conditionResolutionData struct {
		OutcomeSlotCount *big.Int
		PayoutNumerators []*big.Int
	}
	var decoded conditionResolutionData
	err := p.abi.UnpackIntoInterface(&decoded, EventTypeConditionResolution, log.Data)
	if err != nil {
		return nil, fmt.Errorf("解析ConditionResolution事件data失败: %v", err)
	}

	return &ConditionResolutionEvent{
		EventBase: EventBase{
			EventType:   EventTypeConditionResolution,
			Address:     log.Address,
			Txhash:      log.TxHash,
			BlockNumber: log.BlockNumber,
			BlockTime:   time.Unix(int64(log.BlockNumber), 0),
		},
		ConditionId:      conditionId,
		Oracle:           oracle,
		QuestionId:       questionId,
		OutcomeSlotCount: decoded.OutcomeSlotCount.Uint64(),
		PayoutNumerators: decoded.PayoutNumerators,
	}, nil
}

// ParsePositionSplitEvent 解析PositionSplit事件
// PositionSplit(address indexed stakeholder, address collateralToken, bytes32 indexed parentCollectionId, bytes32 indexed conditionId, uint256[] partition, uint256 amount)
func (p *ConditionalTokensContract) ParsePositionSplitEvent(log types.Log) (*PositionSplitEvent, error) {
	if len(log.Topics) < 4 {
		return nil, fmt.Errorf("解析PositionSplit事件失败: topics不足")
	}

	stakeholder := common.BytesToAddress(log.Topics[1].Bytes())
	parentCollectionId := log.Topics[2]
	conditionId := log.Topics[3]

	// 使用ABI解码data部分 (collateralToken + partition dynamic array + amount)
	type positionSplitData struct {
		CollateralToken common.Address
		Partition       []*big.Int
		Amount          *big.Int
	}
	var decoded positionSplitData
	err := p.abi.UnpackIntoInterface(&decoded, EventTypePositionSplit, log.Data)
	if err != nil {
		return nil, fmt.Errorf("解析PositionSplit事件data失败: %v", err)
	}

	return &PositionSplitEvent{
		EventBase: EventBase{
			EventType:   EventTypePositionSplit,
			Address:     log.Address,
			Txhash:      log.TxHash,
			BlockNumber: log.BlockNumber,
			BlockTime:   time.Unix(int64(log.BlockNumber), 0),
		},
		Stakeholder:        stakeholder,
		CollateralToken:    decoded.CollateralToken,
		ParentCollectionId: parentCollectionId,
		ConditionId:        conditionId,
		Partition:          decoded.Partition,
		Amount:             decoded.Amount,
	}, nil
}

// ParsePositionsMergeEvent 解析PositionsMerge事件
// PositionsMerge(address indexed stakeholder, address collateralToken, bytes32 indexed parentCollectionId, bytes32 indexed conditionId, uint256[] partition, uint256 amount)
func (p *ConditionalTokensContract) ParsePositionsMergeEvent(log types.Log) (*PositionsMergeEvent, error) {
	if len(log.Topics) < 4 {
		return nil, fmt.Errorf("解析PositionsMerge事件失败: topics不足")
	}

	stakeholder := common.BytesToAddress(log.Topics[1].Bytes())
	parentCollectionId := log.Topics[2]
	conditionId := log.Topics[3]

	type positionsMergeData struct {
		CollateralToken common.Address
		Partition       []*big.Int
		Amount          *big.Int
	}
	var decoded positionsMergeData
	err := p.abi.UnpackIntoInterface(&decoded, EventTypePositionsMerge, log.Data)
	if err != nil {
		return nil, fmt.Errorf("解析PositionsMerge事件data失败: %v", err)
	}

	return &PositionsMergeEvent{
		EventBase: EventBase{
			EventType:   EventTypePositionsMerge,
			Address:     log.Address,
			Txhash:      log.TxHash,
			BlockNumber: log.BlockNumber,
			BlockTime:   time.Unix(int64(log.BlockNumber), 0),
		},
		Stakeholder:        stakeholder,
		CollateralToken:    decoded.CollateralToken,
		ParentCollectionId: parentCollectionId,
		ConditionId:        conditionId,
		Partition:          decoded.Partition,
		Amount:             decoded.Amount,
	}, nil
}

// ParsePayoutRedemptionEvent 解析PayoutRedemption事件
// PayoutRedemption(address indexed redeemer, address indexed collateralToken, bytes32 indexed parentCollectionId, bytes32 conditionId, uint256[] indexSets, uint256 payout)
func (p *ConditionalTokensContract) ParsePayoutRedemptionEvent(log types.Log) (*PayoutRedemptionEvent, error) {
	if len(log.Topics) < 4 {
		return nil, fmt.Errorf("解析PayoutRedemption事件失败: topics不足")
	}

	redeemer := common.BytesToAddress(log.Topics[1].Bytes())
	collateralToken := common.BytesToAddress(log.Topics[2].Bytes())
	parentCollectionId := log.Topics[3]

	type payoutRedemptionData struct {
		ConditionId common.Hash
		IndexSets   []*big.Int
		Payout      *big.Int
	}
	var decoded payoutRedemptionData
	err := p.abi.UnpackIntoInterface(&decoded, EventTypePayoutRedemption, log.Data)
	if err != nil {
		return nil, fmt.Errorf("解析PayoutRedemption事件data失败: %v", err)
	}

	return &PayoutRedemptionEvent{
		EventBase: EventBase{
			EventType:   EventTypePayoutRedemption,
			Address:     log.Address,
			Txhash:      log.TxHash,
			BlockNumber: log.BlockNumber,
			BlockTime:   time.Unix(int64(log.BlockNumber), 0),
		},
		Redeemer:           redeemer,
		CollateralToken:    collateralToken,
		ParentCollectionId: parentCollectionId,
		ConditionId:        decoded.ConditionId,
		IndexSets:          decoded.IndexSets,
		Payout:             decoded.Payout,
	}, nil
}

// ParseTransferSingleEvent 解析TransferSingle事件 (ERC1155)
// TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value)
func (p *ConditionalTokensContract) ParseTransferSingleEvent(log types.Log) (*TransferSingleEvent, error) {
	if len(log.Topics) < 4 {
		return nil, fmt.Errorf("解析TransferSingle事件失败: topics不足")
	}
	if len(log.Data) < 64 {
		return nil, fmt.Errorf("解析TransferSingle事件失败: data不足")
	}

	operator := common.BytesToAddress(log.Topics[1].Bytes())
	from := common.BytesToAddress(log.Topics[2].Bytes())
	to := common.BytesToAddress(log.Topics[3].Bytes())
	id := new(big.Int).SetBytes(log.Data[0:32])
	value := new(big.Int).SetBytes(log.Data[32:64])

	return &TransferSingleEvent{
		EventBase: EventBase{
			EventType:   EventTypeTransferSingle,
			Address:     log.Address,
			Txhash:      log.TxHash,
			BlockNumber: log.BlockNumber,
			BlockTime:   time.Unix(int64(log.BlockNumber), 0),
		},
		Operator: operator,
		From:     from,
		To:       to,
		Id:       id,
		Value:    value,
	}, nil
}

// ParseTransferBatchEvent 解析TransferBatch事件 (ERC1155)
// TransferBatch(address indexed operator, address indexed from, address indexed to, uint256[] ids, uint256[] values)
func (p *ConditionalTokensContract) ParseTransferBatchEvent(log types.Log) (*TransferBatchEvent, error) {
	if len(log.Topics) < 4 {
		return nil, fmt.Errorf("解析TransferBatch事件失败: topics不足")
	}

	operator := common.BytesToAddress(log.Topics[1].Bytes())
	from := common.BytesToAddress(log.Topics[2].Bytes())
	to := common.BytesToAddress(log.Topics[3].Bytes())

	type transferBatchData struct {
		Ids    []*big.Int
		Values []*big.Int
	}
	var decoded transferBatchData
	err := p.abi.UnpackIntoInterface(&decoded, EventTypeTransferBatch, log.Data)
	if err != nil {
		return nil, fmt.Errorf("解析TransferBatch事件data失败: %v", err)
	}

	return &TransferBatchEvent{
		EventBase: EventBase{
			EventType:   EventTypeTransferBatch,
			Address:     log.Address,
			Txhash:      log.TxHash,
			BlockNumber: log.BlockNumber,
			BlockTime:   time.Unix(int64(log.BlockNumber), 0),
		},
		Operator: operator,
		From:     from,
		To:       to,
		Ids:      decoded.Ids,
		Values:   decoded.Values,
	}, nil
}
