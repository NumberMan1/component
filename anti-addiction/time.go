package anti_addiction

import (
	"github.com/NumberMan1/numbox/utils/collection"
	"time"
)

type Holiday struct {
	Month int
	Day   int
}

type Holidays []Holiday

// convertSliceToSet 通用的切片转Set函数
func convertSliceToSet[T comparable](slice []T) collection.Set[T] {
	set := collection.NewSet[T]()
	for _, item := range slice {
		set.Add(item)
	}
	return set
}

// TimeConfig 防沉迷时间配置
type TimeConfig struct {
	StartHour       int
	EndHour         int
	StartMinute     int
	EndMinute       int
	StartSecond     int
	EndSecond       int
	AllowedWeekDays []time.Weekday
	Holidays        Holidays
}

// TimeRange 定义时间段结构
type TimeRange struct {
	StartHour       int
	EndHour         int
	StartMinute     int
	EndMinute       int
	StartSecond     int
	EndSecond       int
	AllowedWeekDays collection.Set[time.Weekday]
	HolidaySet      collection.Set[Holiday]
}

// AntiAddictionTimeChecker 防沉迷时间检查器
type AntiAddictionTimeChecker struct {
	timeRange TimeRange
	timeNow   func() time.Time
}

// NewAntiAddictionTimeChecker 创建防沉迷时间检查器
func NewAntiAddictionTimeChecker(config TimeConfig) *AntiAddictionTimeChecker {
	return &AntiAddictionTimeChecker{
		timeRange: TimeRange{
			StartHour:       config.StartHour,
			EndHour:         config.EndHour,
			StartMinute:     config.StartMinute,
			EndMinute:       config.EndMinute,
			StartSecond:     config.StartSecond,
			EndSecond:       config.EndSecond,
			AllowedWeekDays: convertSliceToSet(config.AllowedWeekDays),
			HolidaySet:      convertSliceToSet(config.Holidays),
		},
		timeNow: time.Now,
	}
}

// GetPlayEndTime 获取指定年龄在今天的可游玩结束时间戳（毫秒）
// 返回值：-1表示不可游玩，0表示无限制，其他值表示具体的结束时间戳
func (checker *AntiAddictionTimeChecker) GetPlayEndTime(age int32) int64 {
	// 成年人无限制
	if age >= 18 {
		return 0
	}

	now := checker.timeNow()
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())

	// 检查是否是节假日
	if checker.IsHoliday(now) {
		if checker.IsInHourTimeRange(now) {
			// 返回今天的结束时间点
			endTime := today.Add(time.Duration(checker.timeRange.EndHour)*time.Hour +
				time.Duration(checker.timeRange.EndMinute)*time.Minute +
				time.Duration(checker.timeRange.EndSecond)*time.Second)
			return endTime.UnixMilli()
		}
		return -1
	}

	// 检查是否是允许的星期
	if checker.IsWeekAllowedDay(now) {
		if checker.IsInHourTimeRange(now) {
			// 返回今天的结束时间点
			endTime := today.Add(time.Duration(checker.timeRange.EndHour)*time.Hour +
				time.Duration(checker.timeRange.EndMinute)*time.Minute +
				time.Duration(checker.timeRange.EndSecond)*time.Second)
			return endTime.UnixMilli()
		}
		return -1
	}

	// 不在任何允许时间段内
	return -1
}

// SetTimeRange 设置时间范围
func (checker *AntiAddictionTimeChecker) SetTimeRange(timeRange TimeRange) {
	checker.timeRange = timeRange
}

// IsInHourTimeRange 检查当前时间是否在指定的时间段内
func (checker *AntiAddictionTimeChecker) IsInHourTimeRange(nowTime time.Time) bool {
	// 获取当前时间
	hour := nowTime.Hour()
	minute := nowTime.Minute()
	second := nowTime.Second()

	// 转换为秒计数便于比较
	currentSeconds := hour*3600 + minute*60 + second
	startSeconds := checker.timeRange.StartHour*3600 + checker.timeRange.StartMinute*60 + checker.timeRange.StartSecond
	endSeconds := checker.timeRange.EndHour*3600 + checker.timeRange.EndMinute*60 + checker.timeRange.EndSecond

	return currentSeconds >= startSeconds && currentSeconds <= endSeconds
}

// IsWeekAllowedDay 检查当前日期是否在允许的星期内
func (checker *AntiAddictionTimeChecker) IsWeekAllowedDay(nowTime time.Time) bool {
	currentWeekday := nowTime.Weekday()
	return checker.timeRange.AllowedWeekDays.Contains(currentWeekday)
}

// IsHoliday 检查当前日期是否在指定的节假日内
func (checker *AntiAddictionTimeChecker) IsHoliday(now time.Time) bool {
	month := int(now.Month())
	day := now.Day()
	return checker.timeRange.HolidaySet.Contains(Holiday{
		Month: month,
		Day:   day,
	})
}

// IsInWeekdayPlayTime 检查当前时间是否在指定的星期游戏时间段内
func (checker *AntiAddictionTimeChecker) IsInWeekdayPlayTime(nowTime time.Time) bool {
	// 先检查是否是允许的星期
	if !checker.IsWeekAllowedDay(nowTime) {
		return false
	}
	// 再检查是否在允许的时间段内
	return checker.IsInHourTimeRange(nowTime)
}

// IsInHolidayPlayTime 检查当前时间是否在指定的节假日游戏时间段内
func (checker *AntiAddictionTimeChecker) IsInHolidayPlayTime(nowTime time.Time) bool {
	// 先检查是否是节假日
	if !checker.IsHoliday(nowTime) {
		return false
	}
	// 再检查是否在允许的时间段内
	return checker.IsInHourTimeRange(nowTime)
}

// SetTimeNow 为了便于测试，添加设置时间函数的方法
func (checker *AntiAddictionTimeChecker) SetTimeNow(timeNow func() time.Time) {
	checker.timeNow = timeNow
}

// IsInPlayTime 检查是否在允许游戏时间内
func (checker *AntiAddictionTimeChecker) IsInPlayTime(age int32) bool {
	if age >= 18 {
		return true
	}

	now := checker.timeNow()

	// 判断是否在节假日游戏时间段内
	if checker.IsInHolidayPlayTime(now) {
		return true
	}

	// 判断是否在星期游戏时间段内
	if checker.IsInWeekdayPlayTime(now) {
		return true
	}
	return false
}
