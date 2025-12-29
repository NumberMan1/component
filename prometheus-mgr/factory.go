package prometheus_mgr

import (
	"sync"

	zaplog "github.com/NumberMan1/component/zap-logger"
)

// 全局实例管理
var (
	defaultManager PrometheusManager
	defaultOnce    sync.Once
	defaultMu      sync.RWMutex

	// 多实例管理
	instances   = make(map[string]PrometheusManager)
	instancesMu sync.RWMutex
)

// GetDefaultManager 获取默认单例管理器
func GetDefaultManager() PrometheusManager {
	defaultMu.RLock()
	if defaultManager != nil {
		defer defaultMu.RUnlock()
		return defaultManager
	}
	defaultMu.RUnlock()

	defaultMu.Lock()
	defer defaultMu.Unlock()

	// 双重检查
	if defaultManager != nil {
		return defaultManager
	}

	panic("default prometheus manager not initialized, call InitDefaultManager first")
}

// InitDefaultManager 初始化默认管理器
func InitDefaultManager(logger zaplog.Logger, opts ...Option) error {
	defaultMu.Lock()
	defer defaultMu.Unlock()

	if defaultManager != nil {
		// 如果已经存在，先停止旧的管理器
		_ = defaultManager.Stop(nil)
		defaultManager = nil
	}

	// 确保是单例模式
	options := []Option{WithSingleton(true), WithInstanceName("default"), WithLogger(logger)}
	options = append(options, opts...)

	manager, err := NewManager(options...)
	if err != nil {
		return err
	}

	defaultManager = manager
	return nil
}

// GetManager 获取指定名称的管理器实例
func GetManager(name string) (PrometheusManager, bool) {
	instancesMu.RLock()
	defer instancesMu.RUnlock()

	manager, exists := instances[name]
	return manager, exists
}

// CreateManager 创建新的管理器实例
func CreateManager(name string, logger zaplog.Logger, opts ...Option) (PrometheusManager, error) {
	instancesMu.Lock()
	defer instancesMu.Unlock()

	if _, exists := instances[name]; exists {
		return nil, NewConfigError("manager instance already exists", ErrManagerAlreadyStarted)
	}

	// 设置实例名称和非单例模式
	options := []Option{WithSingleton(false), WithInstanceName(name), WithLogger(logger)}
	options = append(options, opts...)

	manager, err := NewManager(options...)
	if err != nil {
		return nil, err
	}

	instances[name] = manager
	return manager, nil
}

// RemoveManager 移除管理器实例
func RemoveManager(name string) error {
	instancesMu.Lock()
	defer instancesMu.Unlock()

	manager, exists := instances[name]
	if !exists {
		return NewConfigError("manager instance not found", ErrManagerNotInitialized)
	}

	// 停止管理器
	if err := manager.Stop(nil); err != nil {
		return err
	}

	delete(instances, name)
	return nil
}

// ListManagers 列出所有管理器实例
func ListManagers() []string {
	instancesMu.RLock()
	defer instancesMu.RUnlock()

	names := make([]string, 0, len(instances))
	for name := range instances {
		names = append(names, name)
	}

	return names
}

// StopAllManagers 停止所有管理器实例
func StopAllManagers() error {
	instancesMu.Lock()
	defer instancesMu.Unlock()

	var lastError error

	// 停止所有实例
	for name, manager := range instances {
		if err := manager.Stop(nil); err != nil {
			lastError = err
		}
		delete(instances, name)
	}

	// 停止默认管理器
	defaultMu.Lock()
	if defaultManager != nil {
		if err := defaultManager.Stop(nil); err != nil {
			lastError = err
		}
		defaultManager = nil
	}
	defaultMu.Unlock()

	return lastError
}

// 便捷方法 - 使用默认管理器

// RegisterCounter 使用默认管理器注册Counter
func RegisterCounter(name string, opts MetricOptions) error {
	return GetDefaultManager().RegisterCounter(name, opts)
}

// RegisterGauge 使用默认管理器注册Gauge
func RegisterGauge(name string, opts MetricOptions) error {
	return GetDefaultManager().RegisterGauge(name, opts)
}

// RegisterHistogram 使用默认管理器注册Histogram
func RegisterHistogram(name string, opts MetricOptions) error {
	return GetDefaultManager().RegisterHistogram(name, opts)
}

// RegisterSummary 使用默认管理器注册Summary
func RegisterSummary(name string, opts MetricOptions) error {
	return GetDefaultManager().RegisterSummary(name, opts)
}

// IncCounter 使用默认管理器增加Counter
func IncCounter(name string, labels MetricLabels) error {
	return GetDefaultManager().IncCounter(name, labels)
}

// AddCounter 使用默认管理器增加Counter指定值
func AddCounter(name string, value float64, labels MetricLabels) error {
	return GetDefaultManager().AddCounter(name, value, labels)
}

// SetGauge 使用默认管理器设置Gauge值
func SetGauge(name string, value float64, labels MetricLabels) error {
	return GetDefaultManager().SetGauge(name, value, labels)
}

// IncGauge 使用默认管理器增加Gauge
func IncGauge(name string, labels MetricLabels) error {
	return GetDefaultManager().IncGauge(name, labels)
}

// DecGauge 使用默认管理器减少Gauge
func DecGauge(name string, labels MetricLabels) error {
	return GetDefaultManager().DecGauge(name, labels)
}

// AddGauge 使用默认管理器增加Gauge指定值
func AddGauge(name string, value float64, labels MetricLabels) error {
	return GetDefaultManager().AddGauge(name, value, labels)
}

// SubGauge 使用默认管理器减少Gauge指定值
func SubGauge(name string, value float64, labels MetricLabels) error {
	return GetDefaultManager().SubGauge(name, value, labels)
}

// ObserveHistogram 使用默认管理器观察Histogram值
func ObserveHistogram(name string, value float64, labels MetricLabels) error {
	return GetDefaultManager().ObserveHistogram(name, value, labels)
}

// ObserveSummary 使用默认管理器观察Summary值
func ObserveSummary(name string, value float64, labels MetricLabels) error {
	return GetDefaultManager().ObserveSummary(name, value, labels)
}

// 配置构建器

// ConfigBuilder 配置构建器
type ConfigBuilder struct {
	config Config
}

// NewConfigBuilder 创建配置构建器
func NewConfigBuilder() *ConfigBuilder {
	return &ConfigBuilder{
		config: DefaultConfig(),
	}
}

// WithServiceName 设置服务名称
func (b *ConfigBuilder) WithServiceName(name string) *ConfigBuilder {
	b.config.ServiceName = name
	b.config.Metrics.DefaultLabels["service"] = name
	return b
}

// WithNamespace 设置命名空间
func (b *ConfigBuilder) WithNamespace(namespace string) *ConfigBuilder {
	b.config.Namespace = namespace
	return b
}

// WithSubsystem 设置子系统
func (b *ConfigBuilder) WithSubsystem(subsystem string) *ConfigBuilder {
	b.config.Subsystem = subsystem
	return b
}

// WithHTTPPort 设置HTTP端口
func (b *ConfigBuilder) WithHTTPPort(port int) *ConfigBuilder {
	b.config.HTTP.Port = port
	return b
}

// WithHTTPPath 设置HTTP路径
func (b *ConfigBuilder) WithHTTPPath(path string) *ConfigBuilder {
	b.config.HTTP.Path = path
	return b
}

// WithDefaultLabel 添加默认标签
func (b *ConfigBuilder) WithDefaultLabel(key, value string) *ConfigBuilder {
	if b.config.Metrics.DefaultLabels == nil {
		b.config.Metrics.DefaultLabels = make(MetricLabels)
	}
	b.config.Metrics.DefaultLabels[key] = value
	return b
}

// WithMetricPrefix 设置指标前缀
func (b *ConfigBuilder) WithMetricPrefix(prefix string) *ConfigBuilder {
	b.config.Metrics.Prefix = prefix
	return b
}

// WithGoMetrics 设置是否包含Go运行时指标
func (b *ConfigBuilder) WithGoMetrics(enabled bool) *ConfigBuilder {
	b.config.Metrics.IncludeGoMetrics = enabled
	return b
}

// WithProcessMetrics 设置是否包含进程指标
func (b *ConfigBuilder) WithProcessMetrics(enabled bool) *ConfigBuilder {
	b.config.Metrics.IncludeProcessMetrics = enabled
	return b
}

// WithTLS 设置TLS配置
func (b *ConfigBuilder) WithTLS(certFile, keyFile string) *ConfigBuilder {
	b.config.HTTP.TLS.Enabled = true
	b.config.HTTP.TLS.CertFile = certFile
	b.config.HTTP.TLS.KeyFile = keyFile
	return b
}

// WithBasicAuth 设置基础认证
func (b *ConfigBuilder) WithBasicAuth(username, password string) *ConfigBuilder {
	b.config.Advanced.Security.BasicAuth.Enabled = true
	b.config.Advanced.Security.BasicAuth.Username = username
	b.config.Advanced.Security.BasicAuth.Password = password
	return b
}

// Build 构建配置
func (b *ConfigBuilder) Build() Config {
	return b.config
}

// BuildOption 构建选项
func (b *ConfigBuilder) BuildOption() Option {
	return WithConfig(b.config)
}
