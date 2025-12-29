package prometheus_mgr

import (
	"context"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// MetricType 指标类型
type MetricType string

const (
	MetricTypeCounter   MetricType = "counter"
	MetricTypeGauge     MetricType = "gauge"
	MetricTypeHistogram MetricType = "histogram"
	MetricTypeSummary   MetricType = "summary"
)

// MetricLabels 指标标签
type MetricLabels map[string]string

// MetricOptions 指标选项
type MetricOptions struct {
	Name        string
	Help        string
	Labels      []string
	ConstLabels MetricLabels
	Buckets     []float64 // 用于Histogram
	Objectives  map[float64]float64 // 用于Summary
}

// PrometheusManager Prometheus管理器接口
type PrometheusManager interface {
	// 注册指标
	RegisterCounter(name string, opts MetricOptions) error
	RegisterGauge(name string, opts MetricOptions) error
	RegisterHistogram(name string, opts MetricOptions) error
	RegisterSummary(name string, opts MetricOptions) error

	// 操作Counter
	IncCounter(name string, labels MetricLabels) error
	AddCounter(name string, value float64, labels MetricLabels) error

	// 操作Gauge
	SetGauge(name string, value float64, labels MetricLabels) error
	IncGauge(name string, labels MetricLabels) error
	DecGauge(name string, labels MetricLabels) error
	AddGauge(name string, value float64, labels MetricLabels) error
	SubGauge(name string, value float64, labels MetricLabels) error

	// 操作Histogram
	ObserveHistogram(name string, value float64, labels MetricLabels) error

	// 操作Summary
	ObserveSummary(name string, value float64, labels MetricLabels) error

	// 获取指标
	GetMetric(name string) (prometheus.Collector, error)
	GetAllMetrics() map[string]prometheus.Collector

	// HTTP处理器
	GetHTTPHandler() http.Handler

	// 启动和停止
	Start(ctx context.Context) error
	Stop(ctx context.Context) error

	// 健康检查
	HealthCheck() error

	// 获取注册器
	GetRegistry() *prometheus.Registry
}

// MetricCollector 指标收集器接口
type MetricCollector interface {
	Collect(ch chan<- prometheus.Metric)
	Describe(ch chan<- *prometheus.Desc)
}

// CustomCollector 自定义收集器接口
type CustomCollector interface {
	MetricCollector
	Name() string
	Help() string
}

// TimerFunc 计时器函数类型
type TimerFunc func() time.Duration

// HistogramTimer Histogram计时器
type HistogramTimer interface {
	ObserveDuration()
}

// SummaryTimer Summary计时器
type SummaryTimer interface {
	ObserveDuration()
}