package prometheus_mgr

import (
	"net/http"
	"time"

	zaplog "github.com/NumberMan1/component/zap-logger"
	"github.com/prometheus/client_golang/prometheus"
)

// Options 选项结构
type Options struct {
	Config   Config
	Logger   zaplog.Logger
	Registry *prometheus.Registry

	// 实例配置
	InstanceName string
	Singleton    bool

	// 自定义收集器
	CustomCollectors []CustomCollector

	// 中间件
	Middlewares []func(http.Handler) http.Handler
}

// Option 选项函数类型
type Option func(*Options)

// DefaultOptions 返回默认选项
func DefaultOptions() Options {
	return Options{
		Config:           DefaultConfig(),
		Logger:           nil, // 需要外部提供
		Registry:         nil, // 将在manager中创建
		InstanceName:     "default",
		Singleton:        true,
		CustomCollectors: []CustomCollector{},
		Middlewares:      []func(http.Handler) http.Handler{},
	}
}

// WithConfig 设置配置
func WithConfig(config Config) Option {
	return func(o *Options) {
		o.Config = config
	}
}

// WithLogger 设置日志器
func WithLogger(logger zaplog.Logger) Option {
	return func(o *Options) {
		o.Logger = logger
	}
}

// WithRegistry 设置注册器
func WithRegistry(registry *prometheus.Registry) Option {
	return func(o *Options) {
		o.Registry = registry
	}
}

// WithInstanceName 设置实例名称
func WithInstanceName(name string) Option {
	return func(o *Options) {
		o.InstanceName = name
	}
}

// WithSingleton 设置是否为单例模式
func WithSingleton(singleton bool) Option {
	return func(o *Options) {
		o.Singleton = singleton
	}
}

// WithCustomCollector 添加自定义收集器
func WithCustomCollector(collector CustomCollector) Option {
	return func(o *Options) {
		o.CustomCollectors = append(o.CustomCollectors, collector)
	}
}

// WithCustomCollectors 设置自定义收集器列表
func WithCustomCollectors(collectors []CustomCollector) Option {
	return func(o *Options) {
		o.CustomCollectors = collectors
	}
}

// WithMiddleware 添加HTTP中间件
func WithMiddleware(middleware func(http.Handler) http.Handler) Option {
	return func(o *Options) {
		o.Middlewares = append(o.Middlewares, middleware)
	}
}

// WithMiddlewares 设置HTTP中间件列表
func WithMiddlewares(middlewares []func(http.Handler) http.Handler) Option {
	return func(o *Options) {
		o.Middlewares = middlewares
	}
}

// 配置相关选项

// WithServiceName 设置服务名称
func WithServiceName(name string) Option {
	return func(o *Options) {
		o.Config.ServiceName = name
		o.Config.Metrics.DefaultLabels["service"] = name
	}
}

// WithNamespace 设置命名空间
func WithNamespace(namespace string) Option {
	return func(o *Options) {
		o.Config.Namespace = namespace
	}
}

// WithSubsystem 设置子系统
func WithSubsystem(subsystem string) Option {
	return func(o *Options) {
		o.Config.Subsystem = subsystem
	}
}

// WithHTTPConfig 设置HTTP配置
func WithHTTPConfig(config HTTPConfig) Option {
	return func(o *Options) {
		o.Config.HTTP = config
	}
}

// WithHTTPPort 设置HTTP端口
func WithHTTPPort(port int) Option {
	return func(o *Options) {
		o.Config.HTTP.Port = port
	}
}

// WithHTTPPath 设置HTTP路径
func WithHTTPPath(path string) Option {
	return func(o *Options) {
		o.Config.HTTP.Path = path
	}
}

// WithDefaultLabels 设置默认标签
func WithDefaultLabels(labels MetricLabels) Option {
	return func(o *Options) {
		for k, v := range labels {
			o.Config.Metrics.DefaultLabels[k] = v
		}
	}
}

// WithDefaultLabel 添加默认标签
func WithDefaultLabel(key, value string) Option {
	return func(o *Options) {
		if o.Config.Metrics.DefaultLabels == nil {
			o.Config.Metrics.DefaultLabels = make(MetricLabels)
		}
		o.Config.Metrics.DefaultLabels[key] = value
	}
}

// WithHistogramBuckets 设置默认Histogram桶
func WithHistogramBuckets(buckets []float64) Option {
	return func(o *Options) {
		o.Config.Metrics.DefaultHistogramBuckets = buckets
	}
}

// WithSummaryObjectives 设置默认Summary目标
func WithSummaryObjectives(objectives map[float64]float64) Option {
	return func(o *Options) {
		o.Config.Metrics.DefaultSummaryObjectives = objectives
	}
}

// WithMetricPrefix 设置指标前缀
func WithMetricPrefix(prefix string) Option {
	return func(o *Options) {
		o.Config.Metrics.Prefix = prefix
	}
}

// WithGoMetrics 设置是否包含Go运行时指标
func WithGoMetrics(enabled bool) Option {
	return func(o *Options) {
		o.Config.Metrics.IncludeGoMetrics = enabled
	}
}

// WithProcessMetrics 设置是否包含进程指标
func WithProcessMetrics(enabled bool) Option {
	return func(o *Options) {
		o.Config.Metrics.IncludeProcessMetrics = enabled
	}
}

// WithCollectorInterval 设置收集器间隔
func WithCollectorInterval(interval time.Duration) Option {
	return func(o *Options) {
		o.Config.Collectors.Interval = interval
	}
}

// WithBuiltinCollectors 设置是否启用内置收集器
func WithBuiltinCollectors(enabled bool) Option {
	return func(o *Options) {
		o.Config.Collectors.BuiltinEnabled = enabled
	}
}

// WithTLS 设置TLS配置
func WithTLS(certFile, keyFile string) Option {
	return func(o *Options) {
		o.Config.HTTP.TLS.Enabled = true
		o.Config.HTTP.TLS.CertFile = certFile
		o.Config.HTTP.TLS.KeyFile = keyFile
	}
}

// WithBasicAuth 设置基础认证
func WithBasicAuth(username, password string) Option {
	return func(o *Options) {
		o.Config.Advanced.Security.BasicAuth.Enabled = true
		o.Config.Advanced.Security.BasicAuth.Username = username
		o.Config.Advanced.Security.BasicAuth.Password = password
	}
}

// WithAllowedIPs 设置IP白名单
func WithAllowedIPs(ips []string) Option {
	return func(o *Options) {
		o.Config.Advanced.Security.AllowedIPs = ips
	}
}

// WithCORS 设置是否启用CORS
func WithCORS(enabled bool) Option {
	return func(o *Options) {
		o.Config.Advanced.Security.EnableCORS = enabled
	}
}

// WithPerformanceConfig 设置性能配置
func WithPerformanceConfig(config PerformanceConfig) Option {
	return func(o *Options) {
		o.Config.Advanced.Performance = config
	}
}

// WithMaxMetrics 设置最大指标数量
func WithMaxMetrics(max int) Option {
	return func(o *Options) {
		o.Config.Advanced.Registry.MaxMetrics = max
	}
}

// WithGlobalRegistry 设置是否使用全局注册器
func WithGlobalRegistry(useGlobal bool) Option {
	return func(o *Options) {
		o.Config.Advanced.Registry.UseGlobal = useGlobal
	}
}

// WithMetricValidation 设置是否启用指标验证
func WithMetricValidation(enabled bool) Option {
	return func(o *Options) {
		o.Config.Advanced.Registry.ValidateMetrics = enabled
	}
}
