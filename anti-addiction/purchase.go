package anti_addiction

import "slices"

// MoneyUnit 定义金额单位
type MoneyUnit int

const (
	UnitFen  MoneyUnit = 1   // 分
	UnitJiao MoneyUnit = 10  // 角
	UnitYuan MoneyUnit = 100 // 元
)

// PurchaseLimit 定义不同年龄段的充值限制
type PurchaseLimit struct {
	// 单笔充值限制（单位：分）
	SingleLimit int64
	// 月度充值限制（单位：分）
	MonthlyLimit int64
}

// AgePurchaseLimit 定义年龄段的充值限制
type AgePurchaseLimit struct {
	// 年龄下限（包含）
	MinAge int32
	// 年龄上限（不包含）
	MaxAge int32
	// 充值限制
	Limit PurchaseLimit
}

type AgePurchaseLimits []AgePurchaseLimit

// PurchaseConfig 充值限制配置
type PurchaseConfig struct {
	// 按年龄段配置的充值限制列表
	AgeLimits AgePurchaseLimits
}

// PurchaseOption 充值检查选项
type PurchaseOption struct {
	// 金额单位，默认为分
	Unit MoneyUnit
}

// DefaultPurchaseOption 默认选项，使用分作为单位
var DefaultPurchaseOption = PurchaseOption{
	Unit: UnitFen,
}

// PurchaseChecker 充值检查器
type PurchaseChecker struct {
	config PurchaseConfig
}

// NewPurchaseChecker 创建充值检查器
func NewPurchaseChecker(config PurchaseConfig) *PurchaseChecker {
	// 创建配置的副本，避免修改原始配置
	sortedConfig := PurchaseConfig{
		AgeLimits: make(AgePurchaseLimits, len(config.AgeLimits)),
	}
	copy(sortedConfig.AgeLimits, config.AgeLimits)

	// 进行排序，防止年龄检查出错
	slices.SortFunc(sortedConfig.AgeLimits, func(a, b AgePurchaseLimit) int {
		return int(a.MinAge - b.MinAge)
	})

	return &PurchaseChecker{
		config: sortedConfig,
	}
}

// GetPurchaseLimit 获取指定年龄的充值限制
func (checker *PurchaseChecker) GetPurchaseLimit(age int32) PurchaseLimit {
	// 遍历配置的年龄段，找到匹配的限制
	for _, ageLimit := range checker.config.AgeLimits {
		if age >= ageLimit.MinAge && age < ageLimit.MaxAge {
			return ageLimit.Limit
		}
	}

	// 如果没有匹配的年龄段，返回无限制
	return PurchaseLimit{
		SingleLimit:  -1,
		MonthlyLimit: -1,
	}
}

// convertAmount 根据单位转换金额为分
func convertAmount(amount int64, unit MoneyUnit) int64 {
	return amount * int64(unit)
}

// CheckSinglePurchase 检查单笔充值是否超限
// amount: 充值金额
// age: 玩家年龄
// opts: 可选参数，不传则使用默认选项
// 返回值：是否允许充值
func (checker *PurchaseChecker) CheckSinglePurchase(amount int64, age int32, opts ...PurchaseOption) bool {
	// 使用默认选项
	opt := DefaultPurchaseOption
	if len(opts) > 0 {
		opt = opts[0]
	}

	// 转换金额为分
	amountInFen := convertAmount(amount, opt.Unit)
	limit := checker.GetPurchaseLimit(age)

	if limit.SingleLimit == 0 {
		return false
	}

	if limit.SingleLimit == -1 {
		return true
	}

	return amountInFen <= limit.SingleLimit
}

// CheckMonthlyPurchase 检查月度充值是否超限
// amount: 本次充值金额
// monthlyTotal: 当月已充值总额
// age: 玩家年龄
// opts: 可选参数，不传则使用默认选项
// 返回值：是否允许充值
func (checker *PurchaseChecker) CheckMonthlyPurchase(amount int64, monthlyTotal int64, age int32, opts ...PurchaseOption) bool {
	// 使用默认选项
	opt := DefaultPurchaseOption
	if len(opts) > 0 {
		opt = opts[0]
	}

	// 转换金额为分
	amountInFen := convertAmount(amount, opt.Unit)
	monthlyTotalInFen := convertAmount(monthlyTotal, opt.Unit)
	limit := checker.GetPurchaseLimit(age)

	if limit.MonthlyLimit == 0 {
		return false
	}

	if limit.MonthlyLimit == -1 {
		return true
	}

	return monthlyTotalInFen+amountInFen <= limit.MonthlyLimit
}
