package contract

import (
	"fmt"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/go-kratos/kratos/v2/log"
)

// Option方法名常量
const (
	MethodOptionDesc = "description"
	MethodPool       = "pool"
)

// OptionInfo Option代币信息，扩展自ERC20TokenInfo
type OptionInfo struct {
	ERC20TokenInfo                // 嵌入ERC20TokenInfo结构
	Description    string         // 选项描述
	PoolAddress    common.Address // 预测市场合约地址
	Index          uint64         // 选项索引，可从Option代币名称中提取
	BlockNumber    uint64         // 选项创建区块号
}

// OptionContract 用于解析Option代币
type OptionContract struct {
	abi                   abi.ABI
	log                   *log.Helper
	optionEventSignatures map[string]string
	optionEventNames      map[string]string
}

// NewOptionContract 创建新的Option合约解析器
func NewOptionContract(logger log.Logger) (*OptionContract, error) {
	parser := &OptionContract{
		log:                   log.NewHelper(logger),
		optionEventSignatures: make(map[string]string),
		optionEventNames:      make(map[string]string),
	}

	var err error
	parser.abi, err = abi.JSON(strings.NewReader(OptionTokenAbiJson))
	if err != nil {
		return nil, fmt.Errorf("解析Option ABI失败 [%s]", err.Error())
	}

	// 动态计算事件签名
	for _, event := range parser.abi.Events {
		parser.optionEventSignatures[event.ID.Hex()] = event.Name
		parser.optionEventNames[event.Name] = event.ID.Hex()
	}

	return parser, nil
}

// GetABI 返回解析器使用的ABI
func (p *OptionContract) GetABI() abi.ABI {
	return p.abi
}

// GetEventType 根据事件签名获取事件类型
func (p *OptionContract) GetEventType(topicHex string) string {
	if eventType, exists := p.optionEventSignatures[topicHex]; exists {
		return eventType
	}
	return "Unknown"
}

// GetAllEventSignatures 返回所有已注册的事件签名
func (p *OptionContract) GetAllEventSignatures() []common.Hash {
	signatures := make([]common.Hash, 0, len(p.optionEventSignatures))
	for sigHex := range p.optionEventSignatures {
		signatures = append(signatures, common.HexToHash(sigHex))
	}
	return signatures
}

// ParseEvent 解析Option合约事件
func (p *OptionContract) ParseEvent(log types.Log) (interface{}, error) {
	// 检查日志是否有足够的topics
	if len(log.Topics) == 0 {
		return nil, fmt.Errorf("解析Option事件失败: topics不足")
	}

	// 目前Option合约自定义事件较少，这里主要作为接口的一部分保留
	// 实际上Option合约的大部分事件应该是继承自ERC20的事件
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

	// 解析其他事件
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
