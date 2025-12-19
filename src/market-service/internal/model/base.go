package model

import (
	"time"
)

// BaseModel 基础模型
type BaseModel struct {
	ID        uint      `gorm:"primarykey"`
	CreatedAt time.Time // 存储带时区的时间
	UpdatedAt time.Time // 存储带时区的时间
}
