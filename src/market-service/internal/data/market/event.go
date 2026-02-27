package market

import (
	marketBiz "market-service/internal/biz/market"
	"market-service/internal/data/base"
	marketModel "market-service/internal/model/marketcenter/market"
	"market-service/internal/pkg/common"

	"gorm.io/gorm"
)

type eventRepo struct {
	base.MarketcenterInfra
}

// NewEventRepo creates a new event repository
func NewEventRepo(infra base.MarketcenterInfra) marketBiz.EventRepoInterface {
	return &eventRepo{
		infra,
	}
}

func (r *eventRepo) GetEvent(ctx common.Ctx, query *marketBiz.PredictionEventQuery) (*marketBiz.PredictionEventEntity, error) {
	db := query.Condition(r.GetDb().WithContext(ctx.Ctx).Model(&marketModel.PredictionEvent{}), nil)
	event := &marketModel.PredictionEvent{}
	if err := db.First(event).Error; err != nil && err != gorm.ErrRecordNotFound {
		ctx.Log.Errorf("GetEvent sql failed, err: %v", err)
		return nil, err
	}
	if event.ID == 0 {
		return nil, nil
	}
	return event.ToEntity(), nil
}

func (r *eventRepo) GetEventsWithTotal(ctx common.Ctx, query *marketBiz.PredictionEventQuery) ([]*marketBiz.PredictionEventEntity, int64, error) {
	var total int64
	db := query.Condition(r.GetDb().WithContext(ctx.Ctx).Model(&marketModel.PredictionEvent{}), &total)
	var events []*marketModel.PredictionEvent
	if err := db.Find(&events).Error; err != nil {
		ctx.Log.Errorf("GetEventsWithTotal sql failed, err: %v", err)
		return nil, 0, err
	}
	entities := make([]*marketBiz.PredictionEventEntity, 0, len(events))
	for _, e := range events {
		entities = append(entities, e.ToEntity())
	}
	return entities, total, nil
}

func (r *eventRepo) CreateEvent(ctx common.Ctx, entity *marketBiz.PredictionEventEntity) error {
	model := &marketModel.PredictionEvent{}
	model.FromEntity(entity)
	if err := r.Create(ctx, model); err != nil {
		ctx.Log.Errorf("CreateEvent sql failed, err: %v", err)
		return err
	}
	entity.Id = model.ID
	return nil
}
