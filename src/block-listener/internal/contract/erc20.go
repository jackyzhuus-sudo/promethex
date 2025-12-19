package contract

import (
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/go-kratos/kratos/v2/log"
)

// ERC20方法名常量
const (
	MethodBalanceOf = "balanceOf"
	MethodDecimals  = "decimals"
	MethodSymbol    = "symbol"
	MethodName      = "name"
)

// ERC20事件类型
const (
	EventTypeERC20Transfer = "Transfer"
	EventTypeERC20Approval = "Approval"
)

// ERC20TokenInfo ERC20代币信息
type ERC20TokenInfo struct {
	Address  common.Address
	Symbol   string
	Name     string
	Decimals uint8
}

// ERC20Balance ERC20代币余额
type ERC20Balance struct {
	TokenInfo ERC20TokenInfo
	Balance   *big.Int
}

// 事件结构体定义
type ERC20TransferEvent struct {
	EventBase
	From  common.Address
	To    common.Address
	Value *big.Int
}

type ERC20ApprovalEvent struct {
	EventType    string
	TokenAddress common.Address // 代币合约地址
	Owner        common.Address
	Spender      common.Address
	Value        *big.Int
	BlockNumber  uint64
	TxHash       common.Hash
	LogIndex     uint
}

type UnknownERC20Event struct {
	EventType    string
	TokenAddress common.Address // 代币合约地址
	RawData      []byte
	Topics       []common.Hash
	BlockNumber  uint64
	TxHash       common.Hash
	LogIndex     uint
}

// ERC20Contract 用于解析ERC20代币事件
type ERC20Contract struct {
	abi                  abi.ABI
	log                  *log.Helper
	erc20EventSignatures map[string]string
	erc20EventNames      map[string]string
}

// NewERC20Contract 创建新的ERC20事件解析器
func NewERC20Contract(logger log.Logger) (*ERC20Contract, error) {
	parser := &ERC20Contract{
		log:                  log.NewHelper(logger),
		erc20EventSignatures: make(map[string]string),
		erc20EventNames:      make(map[string]string),
	}

	var err error
	parser.abi, err = abi.JSON(strings.NewReader(ERC20AbiJson))
	if err != nil {
		return nil, fmt.Errorf("解析ERC20 ABI失败 [%s]", err.Error())
	}

	// 动态计算事件签名
	for _, event := range parser.abi.Events {
		parser.erc20EventSignatures[event.ID.Hex()] = event.Name
		parser.erc20EventNames[event.Name] = event.ID.Hex()
	}

	return parser, nil
}

// GetEventType 根据事件签名获取事件类型
func (p *ERC20Contract) GetEventType(topicHex string) string {
	if eventType, exists := p.erc20EventSignatures[topicHex]; exists {
		return eventType
	}
	return "Unknown"
}

// GetTransferEventSignature 返回Transfer事件的签名
func (p *ERC20Contract) GetTransferEventSignature() (common.Hash, error) {
	// 查找Transfer事件的签名
	for sigHex, eventType := range p.erc20EventSignatures {
		if eventType == EventTypeERC20Transfer {
			return common.HexToHash(sigHex), nil
		}
	}

	return common.Hash{}, fmt.Errorf("未找到Transfer事件的签名")
}

// GetAllEventSignatures 返回所有已注册的事件签名
func (p *ERC20Contract) GetAllEventSignatures() []common.Hash {
	signatures := make([]common.Hash, 0, len(p.erc20EventSignatures))
	for sigHex := range p.erc20EventSignatures {
		signatures = append(signatures, common.HexToHash(sigHex))
	}
	return signatures
}

// GetABI 返回解析器使用的ABI
func (p *ERC20Contract) GetABI() abi.ABI {
	return p.abi
}

// ParseEvent 解析ERC20事件
func (p *ERC20Contract) ParseEvent(log ethtypes.Log) (interface{}, error) {
	// 检查日志是否有足够的topics
	if len(log.Topics) == 0 {
		return nil, fmt.Errorf("解析ERC20事件失败: topics不足")
	}

	// 获取事件类型
	eventType := p.GetEventType(log.Topics[0].Hex())
	if eventType == "Unknown" {
		return &UnknownERC20Event{
			EventType:    "Unknown",
			TokenAddress: log.Address,
			RawData:      log.Data,
			Topics:       log.Topics,
			BlockNumber:  log.BlockNumber,
			TxHash:       log.TxHash,
			LogIndex:     log.Index,
		}, nil
	}

	// 解析事件数据
	switch eventType {
	case EventTypeERC20Transfer:
		return p.ParseTransferEvent(log)
	case EventTypeERC20Approval:
		return p.parseApprovalEvent(log)
	default:
		return &UnknownERC20Event{
			EventType:    eventType,
			TokenAddress: log.Address,
			RawData:      log.Data,
			Topics:       log.Topics,
			BlockNumber:  log.BlockNumber,
			TxHash:       log.TxHash,
			LogIndex:     log.Index,
		}, nil
	}
}

// 以下是各事件的解析函数
func (p *ERC20Contract) ParseTransferEvent(log ethtypes.Log) (*ERC20TransferEvent, error) {
	// Transfer事件格式：Transfer(address indexed from, address indexed to, uint256 value)
	if len(log.Topics) < 3 {
		return nil, fmt.Errorf("解析Transfer事件失败: topics不足")
	}

	// 从indexed参数(topics)中获取from和to地址
	from := common.BytesToAddress(log.Topics[1].Bytes())
	to := common.BytesToAddress(log.Topics[2].Bytes())

	// 解析非indexed参数(data)中的value
	var value *big.Int
	if len(log.Data) > 0 {
		value = new(big.Int).SetBytes(log.Data)
	} else {
		value = big.NewInt(0)
	}

	return &ERC20TransferEvent{
		EventBase: EventBase{
			EventType:   EventTypeERC20Transfer,
			Address:     log.Address,
			BlockNumber: log.BlockNumber,
			Txhash:      log.TxHash,
		},
		From:  from,
		To:    to,
		Value: value,
	}, nil
}

func (p *ERC20Contract) parseApprovalEvent(log ethtypes.Log) (*ERC20ApprovalEvent, error) {
	// Approval事件格式：Approval(address indexed owner, address indexed spender, uint256 value)
	if len(log.Topics) < 3 {
		return nil, fmt.Errorf("解析Approval事件失败: topics不足")
	}

	// 从indexed参数(topics)中获取owner和spender地址
	owner := common.BytesToAddress(log.Topics[1].Bytes())
	spender := common.BytesToAddress(log.Topics[2].Bytes())

	// 解析非indexed参数(data)中的value
	var value *big.Int
	if len(log.Data) > 0 {
		value = new(big.Int).SetBytes(log.Data)
	} else {
		value = big.NewInt(0)
	}

	return &ERC20ApprovalEvent{
		EventType:    EventTypeERC20Approval,
		TokenAddress: log.Address,
		Owner:        owner,
		Spender:      spender,
		Value:        value,
		BlockNumber:  log.BlockNumber,
		TxHash:       log.TxHash,
		LogIndex:     log.Index,
	}, nil
}
