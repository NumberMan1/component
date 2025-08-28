package anti_addiction

import (
	"errors"
	"github.com/NumberMan1/numbox/utils"
)

// Config 防沉迷总配置
type Config struct {
	TimeConfig     TimeConfig
	PurchaseConfig PurchaseConfig
}

type AntiAddictionChecker interface {
	IsInPlayTime(age int32) bool
	GetPlayEndTime(age int32) int64
	// CheckSinglePurchase 检查单笔充值是否超限
	// amount: 充值金额
	// age: 玩家年龄
	// opts: 可选参数，不传则使用默认选项，使用分作为单位
	// 返回值：是否允许充值
	CheckSinglePurchase(amount int64, age int32, opts ...PurchaseOption) bool
	// CheckMonthlyPurchase 检查月度充值是否超限
	// amount: 本次充值金额
	// monthlyTotal: 当月已充值总额
	// age: 玩家年龄
	// opts: 可选参数，不传则使用默认选项，使用分作为单位
	// 返回值：是否允许充值
	CheckMonthlyPurchase(amount int64, monthlyTotal int64, age int32, opts ...PurchaseOption) bool
}

type antiAddictionChecker struct {
	TimeChecker     *AntiAddictionTimeChecker
	PurchaseChecker *PurchaseChecker
}

var antiAddictionCheckerInstance AntiAddictionChecker

func InitAntiAddictionChecker(config Config) {
	antiAddictionCheckerInstance = &antiAddictionChecker{
		TimeChecker:     NewAntiAddictionTimeChecker(config.TimeConfig),
		PurchaseChecker: NewPurchaseChecker(config.PurchaseConfig),
	}
}

func GetAddictionChecker() AntiAddictionChecker {
	utils.Asset(antiAddictionCheckerInstance != nil, errors.New("anti-addiction checker not initialized"))
	return antiAddictionCheckerInstance
}

func (checker *antiAddictionChecker) IsInPlayTime(age int32) bool {
	return checker.TimeChecker.IsInPlayTime(age)
}

func (checker *antiAddictionChecker) GetPlayEndTime(age int32) int64 {
	return checker.TimeChecker.GetPlayEndTime(age)
}

func (checker *antiAddictionChecker) CheckSinglePurchase(amount int64, age int32, opts ...PurchaseOption) bool {
	return checker.PurchaseChecker.CheckSinglePurchase(amount, age, opts...)
}

func (checker *antiAddictionChecker) CheckMonthlyPurchase(amount int64, monthlyTotal int64, age int32, opts ...PurchaseOption) bool {
	return checker.PurchaseChecker.CheckMonthlyPurchase(amount, monthlyTotal, age, opts...)
}
