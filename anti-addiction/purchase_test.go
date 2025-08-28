package anti_addiction

import (
	"testing"
)

func getDefaultPurchaseConfig() PurchaseConfig {
	return PurchaseConfig{
		AgeLimits: []AgePurchaseLimit{
			{
				MinAge: 0,
				MaxAge: 8,
				Limit: PurchaseLimit{
					SingleLimit:  0,
					MonthlyLimit: 0,
				},
			},
			{
				MinAge: 8,
				MaxAge: 16,
				Limit: PurchaseLimit{
					SingleLimit:  5000,  // 50元
					MonthlyLimit: 20000, // 200元
				},
			},
			{
				MinAge: 16,
				MaxAge: 18,
				Limit: PurchaseLimit{
					SingleLimit:  10000, // 100元
					MonthlyLimit: 40000, // 400元
				},
			},
		},
	}
}

func TestPurchaseChecker_GetPurchaseLimit(t *testing.T) {
	checker := NewPurchaseChecker(getDefaultPurchaseConfig())

	tests := []struct {
		name string
		age  int32
		want PurchaseLimit
	}{
		{
			name: "7岁-禁止充值",
			age:  7,
			want: PurchaseLimit{SingleLimit: 0, MonthlyLimit: 0},
		},
		{
			name: "8岁-限制充值",
			age:  8,
			want: PurchaseLimit{SingleLimit: 5000, MonthlyLimit: 20000},
		},
		{
			name: "15岁-限制充值",
			age:  15,
			want: PurchaseLimit{SingleLimit: 5000, MonthlyLimit: 20000},
		},
		{
			name: "16岁-较高限制",
			age:  16,
			want: PurchaseLimit{SingleLimit: 10000, MonthlyLimit: 40000},
		},
		{
			name: "17岁-较高限制",
			age:  17,
			want: PurchaseLimit{SingleLimit: 10000, MonthlyLimit: 40000},
		},
		{
			name: "18岁-无限制",
			age:  18,
			want: PurchaseLimit{SingleLimit: -1, MonthlyLimit: -1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := checker.GetPurchaseLimit(tt.age)
			if got != tt.want {
				t.Errorf("GetPurchaseLimit() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPurchaseChecker_CheckSinglePurchase(t *testing.T) {
	checker := NewPurchaseChecker(getDefaultPurchaseConfig())

	tests := []struct {
		name        string
		amount      int64
		age         int32
		opt         *PurchaseOption
		wantAllowed bool
	}{
		{
			name:        "7岁-禁止充值-默认单位(分)",
			amount:      100,
			age:         7,
			opt:         nil,
			wantAllowed: false,
		},
		{
			name:        "10岁-允许充值-默认单位(分)",
			amount:      4000,
			age:         10,
			opt:         nil,
			wantAllowed: true,
		},
		{
			name:        "10岁-允许充值-元",
			amount:      40,
			age:         10,
			opt:         &PurchaseOption{Unit: UnitYuan},
			wantAllowed: true,
		},
		{
			name:        "10岁-允许充值-角",
			amount:      400,
			age:         10,
			opt:         &PurchaseOption{Unit: UnitJiao},
			wantAllowed: true,
		},
		{
			name:        "10岁-超额充值-默认单位(分)",
			amount:      6000,
			age:         10,
			opt:         nil,
			wantAllowed: false,
		},
		{
			name:        "18岁-无限制-默认单位(分)",
			amount:      100000,
			age:         18,
			opt:         nil,
			wantAllowed: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var allowed bool
			if tt.opt != nil {
				allowed = checker.CheckSinglePurchase(tt.amount, tt.age, *tt.opt)
			} else {
				allowed = checker.CheckSinglePurchase(tt.amount, tt.age)
			}
			if allowed != tt.wantAllowed {
				t.Errorf("CheckSinglePurchase() = %v, want %v", allowed, tt.wantAllowed)
			}
		})
	}
}

func TestPurchaseChecker_CheckMonthlyPurchase(t *testing.T) {
	checker := NewPurchaseChecker(getDefaultPurchaseConfig())

	tests := []struct {
		name         string
		amount       int64
		monthlyTotal int64
		age          int32
		opt          *PurchaseOption
		wantAllowed  bool
	}{
		{
			name:         "7岁-禁止充值-默认单位(分)",
			amount:       100,
			monthlyTotal: 0,
			age:          7,
			opt:          nil,
			wantAllowed:  false,
		},
		{
			name:         "10岁-允许充值-默认单位(分)",
			amount:       4000,
			monthlyTotal: 15000,
			age:          10,
			opt:          nil,
			wantAllowed:  true,
		},
		{
			name:         "10岁-超额充值-默认单位(分)",
			amount:       6000,
			monthlyTotal: 15000,
			age:          10,
			opt:          nil,
			wantAllowed:  false,
		},
		{
			name:         "16岁-允许充值-元",
			amount:       90,
			monthlyTotal: 300,
			age:          16,
			opt:          &PurchaseOption{Unit: UnitYuan},
			wantAllowed:  true,
		},
		{
			name:         "16岁-超额充值-默认单位(分)",
			amount:       11000,
			monthlyTotal: 30000,
			age:          16,
			opt:          nil,
			wantAllowed:  false,
		},
		{
			name:         "18岁-无限制-默认单位(分)",
			amount:       100000,
			monthlyTotal: 1000000,
			age:          18,
			opt:          nil,
			wantAllowed:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var allowed bool
			if tt.opt != nil {
				allowed = checker.CheckMonthlyPurchase(tt.amount, tt.monthlyTotal, tt.age, *tt.opt)
			} else {
				allowed = checker.CheckMonthlyPurchase(tt.amount, tt.monthlyTotal, tt.age)
			}
			if allowed != tt.wantAllowed {
				t.Errorf("CheckMonthlyPurchase() = %v, want %v", allowed, tt.wantAllowed)
			}
		})
	}
}

func TestNewPurchaseChecker_ConfigSorting(t *testing.T) {
	// 创建一个乱序的配置
	unsortedConfig := PurchaseConfig{
		AgeLimits: []AgePurchaseLimit{
			{
				MinAge: 16,
				MaxAge: 18,
				Limit: PurchaseLimit{
					SingleLimit:  10000,
					MonthlyLimit: 40000,
				},
			},
			{
				MinAge: 0,
				MaxAge: 8,
				Limit: PurchaseLimit{
					SingleLimit:  0,
					MonthlyLimit: 0,
				},
			},
			{
				MinAge: 8,
				MaxAge: 16,
				Limit: PurchaseLimit{
					SingleLimit:  5000,
					MonthlyLimit: 20000,
				},
			},
		},
	}

	// 创建检查器
	checker := NewPurchaseChecker(unsortedConfig)

	// 验证配置是否已排序
	for i := 0; i < len(checker.config.AgeLimits)-1; i++ {
		if checker.config.AgeLimits[i].MinAge > checker.config.AgeLimits[i+1].MinAge {
			t.Errorf("Config not properly sorted: age %d comes before %d",
				checker.config.AgeLimits[i].MinAge,
				checker.config.AgeLimits[i+1].MinAge)
		}
	}

	// 验证排序后的功能是否正常
	tests := []struct {
		name string
		age  int32
		want PurchaseLimit
	}{
		{
			name: "7岁-禁止充值",
			age:  7,
			want: PurchaseLimit{SingleLimit: 0, MonthlyLimit: 0},
		},
		{
			name: "12岁-限制充值",
			age:  12,
			want: PurchaseLimit{SingleLimit: 5000, MonthlyLimit: 20000},
		},
		{
			name: "17岁-较高限制",
			age:  17,
			want: PurchaseLimit{SingleLimit: 10000, MonthlyLimit: 40000},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := checker.GetPurchaseLimit(tt.age)
			if got != tt.want {
				t.Errorf("GetPurchaseLimit() = %v, want %v", got, tt.want)
			}
		})
	}

	// 验证原始配置未被修改
	if unsortedConfig.AgeLimits[0].MinAge != 16 {
		t.Error("Original config was modified")
	}
}
