package prometheus_mgr

import (
	"fmt"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// RegisterCounter 注册Counter指标
func (m *manager) RegisterCounter(name string, opts MetricOptions) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.counters[name]; exists {
		return NewMetricError(fmt.Sprintf("counter %s already exists", name), ErrMetricAlreadyExists)
	}

	// 构建完整的指标名称
	fullName := m.buildMetricName(name)

	// 合并标签
	labels := m.mergeLabels(opts.Labels)
	constLabels := m.mergeConstLabels(opts.ConstLabels)

	counter := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace:   m.options.Config.Namespace,
			Subsystem:   m.options.Config.Subsystem,
			Name:        fullName,
			Help:        opts.Help,
			ConstLabels: constLabels,
		},
		labels,
	)

	if err := m.registry.Register(counter); err != nil {
		return NewRegistryError(fmt.Sprintf("failed to register counter %s", name), err)
	}

	m.counters[name] = counter
	m.logger.Debug("Registered counter", zap.String("name", name), zap.String("full_name", fullName))

	return nil
}

// RegisterGauge 注册Gauge指标
func (m *manager) RegisterGauge(name string, opts MetricOptions) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.gauges[name]; exists {
		return NewMetricError(fmt.Sprintf("gauge %s already exists", name), ErrMetricAlreadyExists)
	}

	fullName := m.buildMetricName(name)
	labels := m.mergeLabels(opts.Labels)
	constLabels := m.mergeConstLabels(opts.ConstLabels)

	gauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace:   m.options.Config.Namespace,
			Subsystem:   m.options.Config.Subsystem,
			Name:        fullName,
			Help:        opts.Help,
			ConstLabels: constLabels,
		},
		labels,
	)

	if err := m.registry.Register(gauge); err != nil {
		return NewRegistryError(fmt.Sprintf("failed to register gauge %s", name), err)
	}

	m.gauges[name] = gauge
	m.logger.Debug("Registered gauge", zap.String("name", name), zap.String("full_name", fullName))

	return nil
}

// RegisterHistogram 注册Histogram指标
func (m *manager) RegisterHistogram(name string, opts MetricOptions) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.histograms[name]; exists {
		return NewMetricError(fmt.Sprintf("histogram %s already exists", name), ErrMetricAlreadyExists)
	}

	fullName := m.buildMetricName(name)
	labels := m.mergeLabels(opts.Labels)
	constLabels := m.mergeConstLabels(opts.ConstLabels)

	// 使用提供的桶或默认桶
	buckets := opts.Buckets
	if len(buckets) == 0 {
		buckets = m.options.Config.Metrics.DefaultHistogramBuckets
	}

	histogram := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace:   m.options.Config.Namespace,
			Subsystem:   m.options.Config.Subsystem,
			Name:        fullName,
			Help:        opts.Help,
			ConstLabels: constLabels,
			Buckets:     buckets,
		},
		labels,
	)

	if err := m.registry.Register(histogram); err != nil {
		return NewRegistryError(fmt.Sprintf("failed to register histogram %s", name), err)
	}

	m.histograms[name] = histogram
	m.logger.Debug("Registered histogram", zap.String("name", name), zap.String("full_name", fullName))

	return nil
}

// RegisterSummary 注册Summary指标
func (m *manager) RegisterSummary(name string, opts MetricOptions) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.summaries[name]; exists {
		return NewMetricError(fmt.Sprintf("summary %s already exists", name), ErrMetricAlreadyExists)
	}

	fullName := m.buildMetricName(name)
	labels := m.mergeLabels(opts.Labels)
	constLabels := m.mergeConstLabels(opts.ConstLabels)

	// 使用提供的目标或默认目标
	objectives := opts.Objectives
	if len(objectives) == 0 {
		objectives = m.options.Config.Metrics.DefaultSummaryObjectives
	}

	summary := prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Namespace:   m.options.Config.Namespace,
			Subsystem:   m.options.Config.Subsystem,
			Name:        fullName,
			Help:        opts.Help,
			ConstLabels: constLabels,
			Objectives:  objectives,
		},
		labels,
	)

	if err := m.registry.Register(summary); err != nil {
		return NewRegistryError(fmt.Sprintf("failed to register summary %s", name), err)
	}

	m.summaries[name] = summary
	m.logger.Debug("Registered summary", zap.String("name", name), zap.String("full_name", fullName))

	return nil
}

// Counter操作方法

// IncCounter 增加Counter
func (m *manager) IncCounter(name string, labels MetricLabels) error {
	m.mu.RLock()
	counter, exists := m.counters[name]
	m.mu.RUnlock()

	if !exists {
		return NewMetricError(fmt.Sprintf("counter %s not found", name), ErrMetricNotFound)
	}

	counter.With(prometheus.Labels(labels)).Inc()
	return nil
}

// AddCounter 增加Counter指定值
func (m *manager) AddCounter(name string, value float64, labels MetricLabels) error {
	m.mu.RLock()
	counter, exists := m.counters[name]
	m.mu.RUnlock()

	if !exists {
		return NewMetricError(fmt.Sprintf("counter %s not found", name), ErrMetricNotFound)
	}

	counter.With(prometheus.Labels(labels)).Add(value)
	return nil
}

// Gauge操作方法

// SetGauge 设置Gauge值
func (m *manager) SetGauge(name string, value float64, labels MetricLabels) error {
	m.mu.RLock()
	gauge, exists := m.gauges[name]
	m.mu.RUnlock()

	if !exists {
		return NewMetricError(fmt.Sprintf("gauge %s not found", name), ErrMetricNotFound)
	}

	gauge.With(prometheus.Labels(labels)).Set(value)
	return nil
}

// IncGauge 增加Gauge
func (m *manager) IncGauge(name string, labels MetricLabels) error {
	m.mu.RLock()
	gauge, exists := m.gauges[name]
	m.mu.RUnlock()

	if !exists {
		return NewMetricError(fmt.Sprintf("gauge %s not found", name), ErrMetricNotFound)
	}

	gauge.With(prometheus.Labels(labels)).Inc()
	return nil
}

// DecGauge 减少Gauge
func (m *manager) DecGauge(name string, labels MetricLabels) error {
	m.mu.RLock()
	gauge, exists := m.gauges[name]
	m.mu.RUnlock()

	if !exists {
		return NewMetricError(fmt.Sprintf("gauge %s not found", name), ErrMetricNotFound)
	}

	gauge.With(prometheus.Labels(labels)).Dec()
	return nil
}

// AddGauge 增加Gauge指定值
func (m *manager) AddGauge(name string, value float64, labels MetricLabels) error {
	m.mu.RLock()
	gauge, exists := m.gauges[name]
	m.mu.RUnlock()

	if !exists {
		return NewMetricError(fmt.Sprintf("gauge %s not found", name), ErrMetricNotFound)
	}

	gauge.With(prometheus.Labels(labels)).Add(value)
	return nil
}

// SubGauge 减少Gauge指定值
func (m *manager) SubGauge(name string, value float64, labels MetricLabels) error {
	m.mu.RLock()
	gauge, exists := m.gauges[name]
	m.mu.RUnlock()

	if !exists {
		return NewMetricError(fmt.Sprintf("gauge %s not found", name), ErrMetricNotFound)
	}

	gauge.With(prometheus.Labels(labels)).Sub(value)
	return nil
}

// Histogram操作方法

// ObserveHistogram 观察Histogram值
func (m *manager) ObserveHistogram(name string, value float64, labels MetricLabels) error {
	m.mu.RLock()
	histogram, exists := m.histograms[name]
	m.mu.RUnlock()

	if !exists {
		return NewMetricError(fmt.Sprintf("histogram %s not found", name), ErrMetricNotFound)
	}

	histogram.With(prometheus.Labels(labels)).Observe(value)
	return nil
}

// Summary操作方法

// ObserveSummary 观察Summary值
func (m *manager) ObserveSummary(name string, value float64, labels MetricLabels) error {
	m.mu.RLock()
	summary, exists := m.summaries[name]
	m.mu.RUnlock()

	if !exists {
		return NewMetricError(fmt.Sprintf("summary %s not found", name), ErrMetricNotFound)
	}

	summary.With(prometheus.Labels(labels)).Observe(value)
	return nil
}

// 获取指标方法

// GetMetric 获取指标
func (m *manager) GetMetric(name string) (prometheus.Collector, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// 尝试从各种类型的指标中查找
	if counter, exists := m.counters[name]; exists {
		return counter, nil
	}
	if gauge, exists := m.gauges[name]; exists {
		return gauge, nil
	}
	if histogram, exists := m.histograms[name]; exists {
		return histogram, nil
	}
	if summary, exists := m.summaries[name]; exists {
		return summary, nil
	}

	return nil, NewMetricError(fmt.Sprintf("metric %s not found", name), ErrMetricNotFound)
}

// GetAllMetrics 获取所有指标
func (m *manager) GetAllMetrics() map[string]prometheus.Collector {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string]prometheus.Collector)

	for name, counter := range m.counters {
		result[name] = counter
	}
	for name, gauge := range m.gauges {
		result[name] = gauge
	}
	for name, histogram := range m.histograms {
		result[name] = histogram
	}
	for name, summary := range m.summaries {
		result[name] = summary
	}

	return result
}

// 辅助方法

// buildMetricName 构建指标名称
func (m *manager) buildMetricName(name string) string {
	if m.options.Config.Metrics.Prefix != "" {
		return m.options.Config.Metrics.Prefix + "_" + name
	}
	return name
}

// mergeLabels 合并标签
func (m *manager) mergeLabels(labels []string) []string {
	// 直接返回用户提供的标签，不自动添加默认标签键
	// 默认标签通过 ConstLabels 处理
	return labels
}

// mergeConstLabels 合并常量标签
func (m *manager) mergeConstLabels(constLabels MetricLabels) prometheus.Labels {
	result := make(prometheus.Labels)

	// 添加默认标签
	for key, value := range m.options.Config.Metrics.DefaultLabels {
		result[key] = value
	}

	// 添加指定的常量标签（会覆盖默认标签）
	for key, value := range constLabels {
		result[key] = value
	}

	return result
}

// Timer相关方法

// NewHistogramTimer 创建Histogram计时器
func (m *manager) NewHistogramTimer(name string, labels MetricLabels) HistogramTimer {
	return &histogramTimer{
		manager: m,
		name:    name,
		labels:  labels,
		start:   time.Now(),
	}
}

// NewSummaryTimer 创建Summary计时器
func (m *manager) NewSummaryTimer(name string, labels MetricLabels) SummaryTimer {
	return &summaryTimer{
		manager: m,
		name:    name,
		labels:  labels,
		start:   time.Now(),
	}
}

// histogramTimer Histogram计时器实现
type histogramTimer struct {
	manager *manager
	name    string
	labels  MetricLabels
	start   time.Time
}

func (t *histogramTimer) ObserveDuration() {
	duration := time.Since(t.start).Seconds()
	_ = t.manager.ObserveHistogram(t.name, duration, t.labels)
}

// summaryTimer Summary计时器实现
type summaryTimer struct {
	manager *manager
	name    string
	labels  MetricLabels
	start   time.Time
}

func (t *summaryTimer) ObserveDuration() {
	duration := time.Since(t.start).Seconds()
	_ = t.manager.ObserveSummary(t.name, duration, t.labels)
}

// 便捷方法

// TimerFunc 计时器函数
func (m *manager) TimerFunc(name string, labels MetricLabels, fn func()) {
	timer := m.NewHistogramTimer(name, labels)
	defer timer.ObserveDuration()
	fn()
}

// TimerFuncWithResult 带返回值的计时器函数
func (m *manager) TimerFuncWithResult(name string, labels MetricLabels, fn func() error) error {
	timer := m.NewHistogramTimer(name, labels)
	defer timer.ObserveDuration()
	return fn()
}

// 批量操作方法

// BatchIncCounter 批量增加Counter
func (m *manager) BatchIncCounter(operations []struct {
	Name   string
	Labels MetricLabels
}) []error {
	errors := make([]error, len(operations))
	for i, op := range operations {
		errors[i] = m.IncCounter(op.Name, op.Labels)
	}
	return errors
}

// BatchSetGauge 批量设置Gauge
func (m *manager) BatchSetGauge(operations []struct {
	Name   string
	Value  float64
	Labels MetricLabels
}) []error {
	errors := make([]error, len(operations))
	for i, op := range operations {
		errors[i] = m.SetGauge(op.Name, op.Value, op.Labels)
	}
	return errors
}

// 指标名称验证
func (m *manager) validateMetricName(name string) error {
	if name == "" {
		return ErrInvalidMetricName
	}

	// Prometheus指标名称规则：只能包含字母、数字、下划线和冒号
	for _, char := range name {
		if !((char >= 'a' && char <= 'z') ||
			(char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') ||
			char == '_' || char == ':') {
			return ErrInvalidMetricName
		}
	}

	// 不能以数字开头
	if name[0] >= '0' && name[0] <= '9' {
		return ErrInvalidMetricName
	}

	// 不能包含连续的下划线
	if strings.Contains(name, "__") {
		return ErrInvalidMetricName
	}

	return nil
}