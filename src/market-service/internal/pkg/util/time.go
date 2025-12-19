package util

import (
	"time"
)

// GetDayStartTimeStr 获取指定时间戳所在天的0点时间字符串（UTC+0）
func GetDayStartTimeStr(timestamp int64) string {
	// 将时间戳转换为UTC时间
	t := time.Unix(timestamp, 0).UTC()
	// 获取当天0点
	dayStart := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
	// 返回格式化的时间字符串
	return dayStart.Format("2006-01-02-15:04:05.000+0000")
}

// GetWeekStartTimeStr 获取指定时间戳所在周的周一0点时间字符串（UTC+0）
func GetWeekStartTimeStr(timestamp int64) string {
	// 将时间戳转换为UTC时间
	t := time.Unix(timestamp, 0).UTC()

	// 计算当前是周几（Go中，周日是0，周一是1，...）
	weekday := t.Weekday()
	// 计算到周一的偏移量
	var offset int
	if weekday == time.Sunday {
		offset = 6 // 如果是周日，需要往前推6天到上周一
	} else {
		offset = int(weekday) - 1 // 其他情况，减去相应天数到本周一
	}

	// 获取本周一的日期
	weekStart := time.Date(t.Year(), t.Month(), t.Day()-offset, 0, 0, 0, 0, time.UTC)
	// 返回格式化的时间字符串
	return weekStart.Format("2006-01-02-15:04:05.000+0000")
}

// GetMonthStartTimeStr 获取指定时间戳所在月的1号0点时间字符串（UTC+0）
func GetMonthStartTimeStr(timestamp int64) string {
	// 将时间戳转换为UTC时间
	t := time.Unix(timestamp, 0).UTC()

	// 获取当月1号
	monthStart := time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC)
	// 返回格式化的时间字符串
	return monthStart.Format("2006-01-02-15:04:05.000+0000")
}
