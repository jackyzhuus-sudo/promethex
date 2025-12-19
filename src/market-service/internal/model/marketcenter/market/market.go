package market

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"market-service/internal/biz/base"
	marketBiz "market-service/internal/biz/market"
	"market-service/internal/model"
	"strconv"
	"strings"

	"github.com/shopspring/decimal"
)

// Market 市场表结构
type Market struct {
	model.BaseModel
	Address            string          `gorm:"column:address;type:varchar(42);uniqueIndex:idx_address;not null;" comment:"市场合约地址"`
	TxHash             string          `gorm:"column:tx_hash;type:varchar(66);not null;" comment:"创建市场合约的tx hash"`
	Name               string          `gorm:"column:name;type:varchar(512);not null;default:''" comment:"市场名描述"`
	Fee                uint32          `gorm:"column:fee;type:integer;not null;default:0" comment:"交易费率 100代表1%"`
	TokenType          uint8           `gorm:"column:token_type;type:smallint;not null;default:1" comment:"使用的基础资产代币  1积分 2usdc"`
	IsShow             uint8           `gorm:"column:is_show;type:smallint;not null;default:1" comment:"前端是否可见 1是2否"`
	OracleAddress      string          `gorm:"column:oracle_address;type:varchar(42);not null;default:''" comment:"预言机地址"`
	Volume             decimal.Decimal `gorm:"column:volume;type:NUMERIC;not null;default:0" comment:"成交量总额"`
	ParticipantsCount  uint64          `gorm:"column:participants_count;type:bigint;not null;default:0" comment:"参与人数"`
	Result             string          `gorm:"column:result;type:varchar(42);not null;default:''" comment:"结果条件代币地址"`
	AssertedTruthfully bool            `gorm:"column:asserted_truthfully;type:boolean;not null;default:false" comment:"断言是否正确"`
	Deadline           uint64          `gorm:"column:deadline;type:bigint;not null;default:0" comment:"截止时间"`
	AssertionId        []byte          `gorm:"column:assertion_id;type:bytea;not null;default:''" comment:"断言id"`
	Description        string          `gorm:"column:description;type:text;not null;default:''" comment:"市场描述"`
	Rules              string          `gorm:"column:rules;type:text;not null;default:''" comment:"市场规则"`
	RulesUrl           string          `gorm:"column:rules_url;type:varchar(256);not null;default:''" comment:"市场规则url"`
	PicUrl             string          `gorm:"column:pic_url;type:varchar(256);not null;default:''" comment:"市场的图片url"`
	Status             uint8           `gorm:"column:status;type:smallint;not null;default:1" comment:"市场状态 1正常 2结算中 3有争议 4已结束"`
	Tags               StringSlice     `gorm:"column:tags;type:jsonb;not null;default:'[]'" comment:"市场标签"`
	Categories         StringSlice     `gorm:"column:categories;type:jsonb;not null;default:'[]'" comment:"市场分类"`
	Embedding          VectorArray     `gorm:"column:embedding;type:vector(384);default:NULL" comment:"市场向量"`
	BlockNumber        uint64          `gorm:"column:block_number;type:bigint;not null;default:0" comment:"区块高度"`
}

type StringSlice []string

func (s *StringSlice) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New("failed to cast value to []byte")
	}

	var slice []string
	err := json.Unmarshal(bytes, &slice)
	if err != nil {
		return err
	}

	*s = slice
	return nil
}

func (s StringSlice) Value() (driver.Value, error) {
	if s == nil {
		return []byte("[]"), nil
	}
	data, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}
	return data, nil
}

type VectorArray []float64

// Value 将Go切片转换为pgvector格式的字符串
func (v VectorArray) Value() (driver.Value, error) {
	if len(v) == 0 {
		return nil, nil
	}

	// 构造pgvector格式: [0.1,0.2,...]
	vectorString := "["
	for i, val := range v {
		if i > 0 {
			vectorString += ","
		}
		vectorString += fmt.Sprintf("%f", val)
	}
	vectorString += "]"

	return vectorString, nil
}

// Scan 将数据库中的向量值转换为Go切片
func (v *VectorArray) Scan(value interface{}) error {
	if value == nil {
		*v = nil
		return nil
	}

	var str string
	switch val := value.(type) {
	case string:
		str = val
	case []byte:
		str = string(val)
	default:
		return fmt.Errorf("无法将 %T 类型转换为 VectorArray", value)
	}

	// 移除首尾的 [ ]
	str = strings.TrimPrefix(str, "[")
	str = strings.TrimSuffix(str, "]")

	if str == "" {
		*v = []float64{}
		return nil
	}

	// 分割字符串并转换为float64
	parts := strings.Split(str, ",")
	result := make([]float64, len(parts))

	for i, part := range parts {
		val, err := strconv.ParseFloat(strings.TrimSpace(part), 64)
		if err != nil {
			return err
		}
		result[i] = val
	}

	*v = result
	return nil
}

func (m *Market) ToEntity() *marketBiz.MarketEntity {
	return &marketBiz.MarketEntity{
		BaseEntity: base.BaseEntity{
			Id:        m.ID,
			CreatedAt: m.CreatedAt,
			UpdatedAt: m.UpdatedAt,
		},
		Address:           m.Address,
		TxHash:            m.TxHash,
		Name:              m.Name,
		Fee:               m.Fee,
		TokenType:         m.TokenType,
		IsShow:            m.IsShow,
		OracleAddress:     m.OracleAddress,
		Volume:            m.Volume,
		ParticipantsCount: m.ParticipantsCount,
		Result:            m.Result,
		PicUrl:            m.PicUrl,
		Deadline:          m.Deadline,
		AssertionId:       m.AssertionId,
		Description:       m.Description,
		Rules:             m.Rules,
		RulesUrl:          m.RulesUrl,
		Status:            m.Status,
		Tags:              m.Tags,
		Categories:        m.Categories,
		Embedding:         m.Embedding,
		BlockNumber:       m.BlockNumber,
	}
}

func (m *Market) FromEntity(entity *marketBiz.MarketEntity) {
	m.Address = entity.Address
	m.TxHash = entity.TxHash
	m.Name = entity.Name
	m.Fee = entity.Fee
	m.TokenType = entity.TokenType
	m.IsShow = entity.IsShow
	m.OracleAddress = entity.OracleAddress
	m.Volume = entity.Volume
	m.ParticipantsCount = entity.ParticipantsCount
	m.Result = entity.Result
	m.PicUrl = entity.PicUrl
	m.Deadline = entity.Deadline
	m.AssertionId = entity.AssertionId
	m.Description = entity.Description
	m.Rules = entity.Rules
	m.RulesUrl = entity.RulesUrl
	m.Status = entity.Status
	m.Tags = entity.Tags
	m.Categories = entity.Categories
	m.Embedding = entity.Embedding
	m.BlockNumber = entity.BlockNumber
	m.ID = entity.Id
	m.CreatedAt = entity.CreatedAt
	m.UpdatedAt = entity.UpdatedAt
}

// TableName 指定表名
func (m *Market) TableName() string {
	return "t_market"
}
