package market

import (
	"market-service/internal/biz/base"
	marketBiz "market-service/internal/biz/market"
	"market-service/internal/model"
)

type Tag struct {
	model.BaseModel
	TagName string `gorm:"column:tag_name;type:varchar(256);uniqueIndex:idx_tag_name;not null;default:''" comment:"标签名称"`
}

func (t *Tag) TableName() string {
	return "t_tag"
}

func (t *Tag) ToEntity() *marketBiz.TagEntity {
	return &marketBiz.TagEntity{
		BaseEntity: base.BaseEntity{
			Id:        t.ID,
			CreatedAt: t.CreatedAt,
			UpdatedAt: t.UpdatedAt,
		},
		TagName: t.TagName,
	}
}

func (t *Tag) FromEntity(entity *marketBiz.TagEntity) {
	t.ID = entity.Id
	t.CreatedAt = entity.CreatedAt
	t.UpdatedAt = entity.UpdatedAt
	t.TagName = entity.TagName
}
