package market

import (
	"market-service/internal/biz/base"
	marketBiz "market-service/internal/biz/market"
	"market-service/internal/model"
)

// PredictionEvent 表示一个 NegRisk Event
type PredictionEvent struct {
	model.BaseModel
	EventId          string `gorm:"column:event_id;type:varchar(66);uniqueIndex:idx_event_id;not null;default:''" comment:"NegRisk Event ID (bytes32 hex)"`
	Title            string `gorm:"column:title;type:varchar(512);not null;default:''" comment:"Event 标题"`
	OutcomeSlotCount int32  `gorm:"column:outcome_slot_count;type:integer;not null;default:2" comment:"结果数量"`
	Collateral       string `gorm:"column:collateral;type:varchar(42);not null;default:''" comment:"USDC 地址"`
	Status           uint8  `gorm:"column:status;type:smallint;not null;default:1" comment:"状态 1:Active 2:Resolved 3:Voided 4:Frozen"`
	MetadataHash     string `gorm:"column:metadata_hash;type:varchar(128);not null;default:''" comment:"元数据哈希"`
}

func (e *PredictionEvent) ToEntity() *marketBiz.PredictionEventEntity {
	return &marketBiz.PredictionEventEntity{
		BaseEntity: base.BaseEntity{
			Id:        e.ID,
			CreatedAt: e.CreatedAt,
			UpdatedAt: e.UpdatedAt,
		},
		EventId:          e.EventId,
		Title:            e.Title,
		OutcomeSlotCount: e.OutcomeSlotCount,
		Collateral:       e.Collateral,
		Status:           e.Status,
		MetadataHash:     e.MetadataHash,
	}
}

func (e *PredictionEvent) FromEntity(entity *marketBiz.PredictionEventEntity) {
	e.EventId = entity.EventId
	e.Title = entity.Title
	e.OutcomeSlotCount = entity.OutcomeSlotCount
	e.Collateral = entity.Collateral
	e.Status = entity.Status
	e.MetadataHash = entity.MetadataHash
	e.ID = entity.Id
	e.CreatedAt = entity.CreatedAt
	e.UpdatedAt = entity.UpdatedAt
}

// TableName 指定表名
func (PredictionEvent) TableName() string {
	return "t_prediction_event"
}
