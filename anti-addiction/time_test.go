package anti_addiction

import (
	"github.com/NumberMan1/numbox/utils/collection"
	"testing"
	"time"
)

// 添加测试用的默认配置函数
func getTestTimeConfig() TimeConfig {
	return TimeConfig{
		StartHour:       20,
		EndHour:         21,
		StartMinute:     0,
		EndMinute:       0,
		StartSecond:     0,
		EndSecond:       0,
		AllowedWeekDays: []time.Weekday{time.Friday, time.Saturday, time.Sunday},
		Holidays: Holidays{{
			Month: 1,
			Day:   1,
		}},
	}
}

func TestAntiAddictionTimeChecker_IsInHourTimeRange(t *testing.T) {
	checker := NewAntiAddictionTimeChecker(getTestTimeConfig())
	baseTime := time.Date(2025, 3, 30, 0, 0, 0, 0, time.Local)

	tests := []struct {
		name       string
		inputTime  time.Time
		wantResult bool
	}{
		{
			name:       "时间范围之前",
			inputTime:  baseTime.Add(19*time.Hour + 59*time.Minute + 59*time.Second),
			wantResult: false,
		},
		{
			name:       "时间范围开始",
			inputTime:  baseTime.Add(20 * time.Hour),
			wantResult: true,
		},
		{
			name:       "时间范围中间",
			inputTime:  baseTime.Add(20*time.Hour + 30*time.Minute),
			wantResult: true,
		},
		{
			name:       "时间范围结束",
			inputTime:  baseTime.Add(21 * time.Hour),
			wantResult: true,
		},
		{
			name:       "时间范围之后",
			inputTime:  baseTime.Add(21*time.Hour + 1*time.Second),
			wantResult: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := checker.IsInHourTimeRange(tt.inputTime)
			if got != tt.wantResult {
				t.Errorf("IsInHourTimeRange() = %v, want %v, time: %v",
					got,
					tt.wantResult,
					tt.inputTime.Format("15:04:05"),
				)
			}
		})
	}
}

func TestAntiAddictionTimeChecker_IsWeekAllowedDay(t *testing.T) {
	checker := NewAntiAddictionTimeChecker(getTestTimeConfig())
	baseTime := time.Date(2025, 3, 28, 12, 0, 0, 0, time.Local) // 星期五

	tests := []struct {
		name       string
		inputTime  time.Time
		wantResult bool
	}{
		{
			name:       "星期五-允许",
			inputTime:  baseTime,
			wantResult: true,
		},
		{
			name:       "星期六-允许",
			inputTime:  baseTime.AddDate(0, 0, 1),
			wantResult: true,
		},
		{
			name:       "星期日-允许",
			inputTime:  baseTime.AddDate(0, 0, 2),
			wantResult: true,
		},
		{
			name:       "星期一-不允许",
			inputTime:  baseTime.AddDate(0, 0, 3),
			wantResult: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := checker.IsWeekAllowedDay(tt.inputTime)
			if got != tt.wantResult {
				t.Errorf("IsWeekAllowedDay() = %v, want %v, weekday: %v",
					got,
					tt.wantResult,
					tt.inputTime.Weekday(),
				)
			}
		})
	}
}

func TestAntiAddictionTimeChecker_IsHoliday(t *testing.T) {
	checker := NewAntiAddictionTimeChecker(getTestTimeConfig())

	tests := []struct {
		name       string
		inputTime  time.Time
		wantResult bool
	}{
		{
			name:       "元旦节-是节假日",
			inputTime:  time.Date(2025, 1, 1, 12, 0, 0, 0, time.Local),
			wantResult: true,
		},
		{
			name:       "普通日期-不是节假日",
			inputTime:  time.Date(2025, 1, 2, 12, 0, 0, 0, time.Local),
			wantResult: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := checker.IsHoliday(tt.inputTime)
			if got != tt.wantResult {
				t.Errorf("IsHoliday() = %v, want %v, date: %v",
					got,
					tt.wantResult,
					tt.inputTime.Format("2006-01-02"),
				)
			}
		})
	}
}

func TestAntiAddictionTimeChecker_IsInWeekdayPlayTime(t *testing.T) {
	checker := NewAntiAddictionTimeChecker(getTestTimeConfig())
	baseTime := time.Date(2025, 3, 28, 0, 0, 0, 0, time.Local) // 星期五

	tests := []struct {
		name       string
		inputTime  time.Time
		wantResult bool
	}{
		{
			name:       "星期五-允许时间之前",
			inputTime:  baseTime.Add(19*time.Hour + 59*time.Minute + 59*time.Second),
			wantResult: false,
		},
		{
			name:       "星期五-允许时间开始",
			inputTime:  baseTime.Add(20 * time.Hour),
			wantResult: true,
		},
		{
			name:       "星期五-允许时间结束",
			inputTime:  baseTime.Add(21 * time.Hour),
			wantResult: true,
		},
		{
			name:       "星期五-允许时间之后",
			inputTime:  baseTime.Add(21*time.Hour + 1*time.Second),
			wantResult: false,
		},
		{
			name:       "星期四-不允许的日期",
			inputTime:  baseTime.AddDate(0, 0, -1).Add(20 * time.Hour),
			wantResult: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := checker.IsInWeekdayPlayTime(tt.inputTime)
			if got != tt.wantResult {
				t.Errorf("IsInWeekdayPlayTime() = %v, want %v, time: %v, weekday: %v",
					got,
					tt.wantResult,
					tt.inputTime.Format("2006-01-02 15:04:05"),
					tt.inputTime.Weekday(),
				)
			}
		})
	}
}

func TestAntiAddictionTimeChecker_IsInHolidayPlayTime(t *testing.T) {
	checker := NewAntiAddictionTimeChecker(getTestTimeConfig())
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.Local) // 元旦节

	tests := []struct {
		name       string
		inputTime  time.Time
		wantResult bool
	}{
		{
			name:       "节假日-允许时间之前",
			inputTime:  baseTime.Add(19*time.Hour + 59*time.Minute + 59*time.Second),
			wantResult: false,
		},
		{
			name:       "节假日-允许时间开始",
			inputTime:  baseTime.Add(20 * time.Hour),
			wantResult: true,
		},
		{
			name:       "节假日-允许时间结束",
			inputTime:  baseTime.Add(21 * time.Hour),
			wantResult: true,
		},
		{
			name:       "节假日-允许时间之后",
			inputTime:  baseTime.Add(21*time.Hour + 1*time.Second),
			wantResult: false,
		},
		{
			name:       "非节假日-允许时间内",
			inputTime:  time.Date(2025, 1, 2, 20, 30, 0, 0, time.Local),
			wantResult: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := checker.IsInHolidayPlayTime(tt.inputTime)
			if got != tt.wantResult {
				t.Errorf("IsInHolidayPlayTime() = %v, want %v, time: %v",
					got,
					tt.wantResult,
					tt.inputTime.Format("2006-01-02 15:04:05"),
				)
			}
		})
	}
}

func TestAntiAddictionTimeChecker_IsInPlayTime(t *testing.T) {
	checker := NewAntiAddictionTimeChecker(getTestTimeConfig())

	tests := []struct {
		name       string
		age        int32
		inputTime  time.Time
		wantResult bool
	}{
		{
			name:       "成年人",
			age:        18,
			inputTime:  time.Date(2025, 3, 27, 20, 30, 0, 0, time.Local),
			wantResult: true,
		},
		{
			name:       "未成年人-节假日允许时间",
			age:        17,
			inputTime:  time.Date(2025, 1, 1, 20, 30, 0, 0, time.Local),
			wantResult: true,
		},
		{
			name:       "未成年人-周末允许时间",
			age:        17,
			inputTime:  time.Date(2025, 3, 28, 20, 30, 0, 0, time.Local), // 星期五
			wantResult: true,
		},
		{
			name:       "未成年人-非允许时间",
			age:        17,
			inputTime:  time.Date(2025, 3, 27, 20, 30, 0, 0, time.Local), // 星期四
			wantResult: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			checker.SetTimeNow(func() time.Time {
				return tt.inputTime
			})

			got := checker.IsInPlayTime(tt.age)
			if got != tt.wantResult {
				t.Errorf("IsInPlayTime() = %v, want %v, age: %v, time: %v",
					got,
					tt.wantResult,
					tt.age,
					tt.inputTime.Format("2006-01-02 15:04:05"),
				)
			}
		})
	}
}

func TestAntiAddictionTimeChecker_SetTimeRange(t *testing.T) {
	checker := NewAntiAddictionTimeChecker(getTestTimeConfig())

	// 设置新的时间范围
	weekDaySet := collection.NewSet[time.Weekday]()
	weekDaySet.Add(time.Monday)
	weekDaySet.Add(time.Tuesday)

	// 使用 convertSliceToSet 替代 convertHolidays2Set
	holidaySet := convertSliceToSet(Holidays{{
		Month: 5,
		Day:   1,
	}})

	newTimeRange := TimeRange{
		StartHour:       14,
		EndHour:         16,
		StartMinute:     0,
		EndMinute:       0,
		StartSecond:     0,
		EndSecond:       0,
		AllowedWeekDays: weekDaySet,
		HolidaySet:      holidaySet,
	}
	checker.SetTimeRange(newTimeRange)

	// 验证时间范围设置
	if checker.timeRange.StartHour != 14 ||
		checker.timeRange.EndHour != 16 ||
		!checker.timeRange.AllowedWeekDays.Contains(time.Monday) ||
		!checker.timeRange.AllowedWeekDays.Contains(time.Tuesday) ||
		len(checker.timeRange.AllowedWeekDays.All()) != 2 {
		t.Error("SetTimeRange failed to set new time range correctly")
	}

	// 验证新的时间范围是否生效
	testTime := time.Date(2025, 5, 1, 15, 0, 0, 0, time.Local) // 五一劳动节 15:00
	if !checker.IsHoliday(testTime) {
		t.Error("SetTimeRange failed: new holiday not recognized")
	}
	if !checker.IsInHourTimeRange(testTime) {
		t.Error("SetTimeRange failed: new time range not effective")
	}
}

func TestAntiAddictionTimeChecker_GetPlayEndTime(t *testing.T) {
	checker := NewAntiAddictionTimeChecker(getTestTimeConfig())
	baseTime := time.Date(2025, 3, 28, 0, 0, 0, 0, time.Local) // 星期五

	tests := []struct {
		name       string
		age        int32
		inputTime  time.Time
		wantResult int64
		checkType  string // 用于验证结果类型：'unlimited', 'forbidden', 'limited'
	}{
		{
			name:       "成年人-任意时间",
			age:        18,
			inputTime:  baseTime.Add(14 * time.Hour),
			checkType:  "unlimited",
			wantResult: 0,
		},
		{
			name:      "未成年人-节假日允许时间",
			age:       17,
			inputTime: time.Date(2025, 1, 1, 20, 30, 0, 0, time.Local), // 元旦节
			checkType: "limited",
		},
		{
			name:       "未成年人-节假日不允许时间",
			age:        17,
			inputTime:  time.Date(2025, 1, 1, 19, 30, 0, 0, time.Local), // 元旦节
			checkType:  "forbidden",
			wantResult: -1,
		},
		{
			name:      "未成年人-周末允许时间",
			age:       17,
			inputTime: time.Date(2025, 3, 28, 20, 30, 0, 0, time.Local), // 星期五
			checkType: "limited",
		},
		{
			name:       "未成年人-周末不允许时间",
			age:        17,
			inputTime:  time.Date(2025, 3, 28, 19, 30, 0, 0, time.Local), // 星期五
			checkType:  "forbidden",
			wantResult: -1,
		},
		{
			name:       "未成年人-非允许日期",
			age:        17,
			inputTime:  time.Date(2025, 3, 27, 20, 30, 0, 0, time.Local), // 星期四
			checkType:  "forbidden",
			wantResult: -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			checker.SetTimeNow(func() time.Time {
				return tt.inputTime
			})

			got := checker.GetPlayEndTime(tt.age)

			switch tt.checkType {
			case "unlimited":
				if got != 0 {
					t.Errorf("GetPlayEndTime() = %v, want unlimited (0)", got)
				}
			case "forbidden":
				if got != -1 {
					t.Errorf("GetPlayEndTime() = %v, want forbidden (-1)", got)
				}
			case "limited":
				// 计算预期的结束时间
				expectedEnd := time.Date(
					tt.inputTime.Year(),
					tt.inputTime.Month(),
					tt.inputTime.Day(),
					checker.timeRange.EndHour,
					checker.timeRange.EndMinute,
					checker.timeRange.EndSecond,
					0,
					tt.inputTime.Location(),
				).UnixMilli()

				if got != expectedEnd {
					t.Errorf("GetPlayEndTime() = %v, want %v", got, expectedEnd)
				}
			}
		})
	}
}
