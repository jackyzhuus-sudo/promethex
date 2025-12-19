package base

import "gorm.io/gorm"

// BaseQuery 基础查询结构体
type BaseQuery struct {
	Id          int64
	Order       string
	Limit       int32
	Offset      int32
	IdLargeThan int64
	IdLessThan  int64
}

func (query *BaseQuery) Condition(db *gorm.DB, total *int64) *gorm.DB {
	if query.Id > 0 {
		db = db.Where("id = ?", query.Id)
	}
	if query.IdLargeThan > 0 {
		db = db.Where("id > ?", query.IdLargeThan)
	}
	if query.IdLessThan > 0 {
		db = db.Where("id < ?", query.IdLessThan)
	}
	if total != nil {
		db = db.Count(total)
	}
	if query.Order != "" {
		db = db.Order(query.Order)
	}
	if query.Limit > 0 {
		db = db.Limit(int(query.Limit))
	}
	if query.Offset > 0 {
		db = db.Offset(int(query.Offset))
	}
	return db
}
