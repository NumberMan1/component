package prometheus_mgr

import (
	"time"
)

// Config Prometheus配置
type Config struct {
	// 基础配置
	Enabled     bool   `yaml:"enabled" json:"enabled"`         // 是否启用
	ServiceName string `yaml:"service_name" json:"service_name"` // 服务名称
	Namespace   string `yaml:"namespace" json:"namespace"`       // 命名空间
	Subsystem   string `yaml:"subsystem" json:"subsystem"`       // 子系统

	// HTTP服务配置
	HTTP HTTPConfig `yaml:"http" json:"http"`

	// 指标配置
	Metrics MetricsConfig `yaml:"metrics" json:"metrics"`

	// 收集器配置
	Collectors CollectorsConfig `yaml:"collectors" json:"collectors"`

	// 高级配置
	Advanced AdvancedConfig `yaml:"advanced" json:"advanced"`
}

// HTTPConfig HTTP服务配置
type HTTPConfig struct {
	Enabled bool   `yaml:"enabled" json:"enabled"` // 是否启用HTTP服务
	Host    string `yaml:"host" json:"host"`       // 监听地址
	Port    int    `yaml:"port" json:"port"`       // 监听端口
	Path    string `yaml:"path" json:"path"`       // 指标路径

	// TLS配置
	TLS TLSConfig `yaml:"tls" json:"tls"`

	// 超时配置
	ReadTimeout  time.Duration `yaml:"read_timeout" json:"read_timeout"`
	WriteTimeout time.Duration `yaml:"write_timeout" json:"write_timeout"`
	IdleTimeout  time.Duration `yaml:"idle_timeout" json:"idle_timeout"`
}

// TLSConfig TLS配置
type TLSConfig struct {
	Enabled  bool   `yaml:"enabled" json:"enabled"`
	CertFile string `yaml:"cert_file" json:"cert_file"`
	KeyFile  string `yaml:"key_file" json:"key_file"`
}

// MetricsConfig 指标配置
type MetricsConfig struct {
	// 默认标签
	DefaultLabels MetricLabels `yaml:"default_labels" json:"default_labels"`

	// 默认Histogram桶
	DefaultHistogramBuckets []float64 `yaml:"default_histogram_buckets" json:"default_histogram_buckets"`

	// 默认Summary目标
	DefaultSummaryObjectives map[float64]float64 `yaml:"default_summary_objectives" json:"default_summary_objectives"`

	// 指标前缀
	Prefix string `yaml:"prefix" json:"prefix"`

	// 是否包含Go运行时指标
	IncludeGoMetrics bool `yaml:"include_go_metrics" json:"include_go_metrics"`

	// 是否包含进程指标
	IncludeProcessMetrics bool `yaml:"include_process_metrics" json:"include_process_metrics"`
}

// CollectorsConfig 收集器配置
type CollectorsConfig struct {
	// 自定义收集器
	Custom []CustomCollectorConfig `yaml:"custom" json:"custom"`

	// 收集间隔
	Interval time.Duration `yaml:"interval" json:"interval"`

	// 是否启用内置收集器
	BuiltinEnabled bool `yaml:"builtin_enabled" json:"builtin_enabled"`
}

// CustomCollectorConfig 自定义收集器配置
type CustomCollectorConfig struct {
	Name    string `yaml:"name" json:"name"`
	Enabled bool   `yaml:"enabled" json:"enabled"`
	Config  map[string]interface{} `yaml:"config" json:"config"`
}

// AdvancedConfig 高级配置
type AdvancedConfig struct {
	// 注册器配置
	Registry RegistryConfig `yaml:"registry" json:"registry"`

	// 性能配置
	Performance PerformanceConfig `yaml:"performance" json:"performance"`

	// 安全配置
	Security SecurityConfig `yaml:"security" json:"security"`
}

// RegistryConfig 注册器配置
type RegistryConfig struct {
	// 是否使用全局注册器
	UseGlobal bool `yaml:"use_global" json:"use_global"`

	// 是否启用指标验证
	ValidateMetrics bool `yaml:"validate_metrics" json:"validate_metrics"`

	// 最大指标数量
	MaxMetrics int `yaml:"max_metrics" json:"max_metrics"`
}

// PerformanceConfig 性能配置
type PerformanceConfig struct {
	// 缓冲区大小
	BufferSize int `yaml:"buffer_size" json:"buffer_size"`

	// 工作协程数
	WorkerCount int `yaml:"worker_count" json:"worker_count"`

	// 批处理大小
	BatchSize int `yaml:"batch_size" json:"batch_size"`

	// 刷新间隔
	FlushInterval time.Duration `yaml:"flush_interval" json:"flush_interval"`
}

// SecurityConfig 安全配置
type SecurityConfig struct {
	// 基础认证
	BasicAuth BasicAuthConfig `yaml:"basic_auth" json:"basic_auth"`

	// IP白名单
	AllowedIPs []string `yaml:"allowed_ips" json:"allowed_ips"`

	// 是否启用CORS
	EnableCORS bool `yaml:"enable_cors" json:"enable_cors"`
}

// BasicAuthConfig 基础认证配置
type BasicAuthConfig struct {
	Enabled  bool   `yaml:"enabled" json:"enabled"`
	Username string `yaml:"username" json:"username"`
	Password string `yaml:"password" json:"password"`
}

// DefaultConfig 返回默认配置
func DefaultConfig() Config {
	return Config{
		Enabled:     true,
		ServiceName: "unknown",
		Namespace:   "",
		Subsystem:   "",
		HTTP: HTTPConfig{
			Enabled:      true,
			Host:         "0.0.0.0",
			Port:         9090,
			Path:         "/metrics",
			ReadTimeout:  30 * time.Second,
			WriteTimeout: 30 * time.Second,
			IdleTimeout:  60 * time.Second,
			TLS: TLSConfig{
				Enabled: false,
			},
		},
		Metrics: MetricsConfig{
			DefaultLabels: MetricLabels{
				"service": "unknown",
			},
			DefaultHistogramBuckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
			DefaultSummaryObjectives: map[float64]float64{
				0.5:  0.05,
				0.9:  0.01,
				0.99: 0.001,
			},
			Prefix:                   "",
			IncludeGoMetrics:         true,
			IncludeProcessMetrics:    true,
		},
		Collectors: CollectorsConfig{
			Custom:         []CustomCollectorConfig{},
			Interval:       30 * time.Second,
			BuiltinEnabled: true,
		},
		Advanced: AdvancedConfig{
			Registry: RegistryConfig{
				UseGlobal:       false,
				ValidateMetrics: true,
				MaxMetrics:      10000,
			},
			Performance: PerformanceConfig{
				BufferSize:    1000,
				WorkerCount:   4,
				BatchSize:     100,
				FlushInterval: 5 * time.Second,
			},
			Security: SecurityConfig{
				BasicAuth: BasicAuthConfig{
					Enabled: false,
				},
				AllowedIPs: []string{},
				EnableCORS: false,
			},
		},
	}
}