# Prometheus Manager 组件

这是一个功能完整的 Prometheus 监控指标管理组件，支持多种启动方式、多种指标类型、单例和多实例模式。

## 特性

- 🚀 **多种启动方式**: 支持 Options 和 Config 两种配置方式
- 📊 **完整指标支持**: Counter、Gauge、Histogram、Summary 四种指标类型
- 🏗️ **灵活架构**: 支持单例和多实例模式
- 🔒 **安全特性**: 支持 TLS、基础认证、IP 白名单、CORS
- 🎯 **自定义收集器**: 支持注册自定义指标收集器
- 🌐 **HTTP 服务**: 内置 HTTP 服务器，提供 `/metrics` 和 `/health` 端点
- 📝 **丰富配置**: 支持命名空间、子系统、标签、桶配置等
- 🔧 **便捷方法**: 提供全局便捷方法和配置构建器

## 快速开始

### 1. 基本使用（单例模式）

```go
package main

import (
    "context"
    "log"
    
    zaplog "github.com/NumberMan1/component/zap-logger"
    "github.com/NumberMan1/component/prometheus-mgr"
)

func main() {
    // 创建 logger
    logger := zaplog.DefaultLogger()
    
    // 初始化默认管理器（单例模式）
    err := prometheus_mgr.InitDefaultManager(logger,
        prometheus_mgr.WithServiceName("my_service"),
        prometheus_mgr.WithHTTPPort(9090),
        prometheus_mgr.WithNamespace("myapp"),
    )
    if err != nil {
        log.Fatal(err)
    }
    
    // 启动管理器
    ctx := context.Background()
    manager := prometheus_mgr.GetDefaultManager()
    if err := manager.Start(ctx); err != nil {
        log.Fatal(err)
    }
    defer manager.Stop(ctx)
    
    // 注册指标
    err = prometheus_mgr.RegisterCounter("requests_total", prometheus_mgr.MetricOptions{
        Help:   "Total number of requests",
        Labels: []string{"method", "status"},
    })
    if err != nil {
        log.Fatal(err)
    }
    
    // 使用指标
    _ = prometheus_mgr.IncCounter("requests_total", prometheus_mgr.MetricLabels{
        "method": "GET",
        "status": "200",
    })
    
    // 访问 http://localhost:9090/metrics 查看指标
    select {} // 保持程序运行
}
```

### 2. 多实例模式

```go
package main

import (
    "context"
    "log"
    
    zaplog "github.com/NumberMan1/component/zap-logger"
    "github.com/NumberMan1/component/prometheus-mgr"
)

func main() {
    logger := zaplog.DefaultLogger()
    
    // 创建 Web 服务实例
    webManager, err := prometheus_mgr.CreateManager("web", logger,
        prometheus_mgr.WithServiceName("web_service"),
        prometheus_mgr.WithHTTPPort(9091),
        prometheus_mgr.WithNamespace("web"),
    )
    if err != nil {
        log.Fatal(err)
    }
    
    // 创建 API 服务实例
    apiManager, err := prometheus_mgr.CreateManager("api", logger,
        prometheus_mgr.WithServiceName("api_service"),
        prometheus_mgr.WithHTTPPort(9092),
        prometheus_mgr.WithNamespace("api"),
    )
    if err != nil {
        log.Fatal(err)
    }
    
    // 启动实例
    ctx := context.Background()
    if err := webManager.Start(ctx); err != nil {
        log.Fatal(err)
    }
    defer webManager.Stop(ctx)
    
    if err := apiManager.Start(ctx); err != nil {
        log.Fatal(err)
    }
    defer apiManager.Stop(ctx)
    
    // 为不同实例注册不同指标
    _ = webManager.RegisterCounter("http_requests_total", prometheus_mgr.MetricOptions{
        Help:   "Total HTTP requests",
        Labels: []string{"path"},
    })
    
    _ = apiManager.RegisterCounter("api_calls_total", prometheus_mgr.MetricOptions{
        Help:   "Total API calls",
        Labels: []string{"version"},
    })
    
    // 使用不同实例的指标
    _ = webManager.IncCounter("http_requests_total", prometheus_mgr.MetricLabels{"path": "/home"})
    _ = apiManager.IncCounter("api_calls_total", prometheus_mgr.MetricLabels{"version": "v1"})
    
    select {} // 保持程序运行
}
```

### 3. 使用配置文件

```go
package main

import (
    "context"
    "log"
    
    zaplog "github.com/NumberMan1/component/zap-logger"
    "github.com/NumberMan1/component/prometheus-mgr"
)

func main() {
    logger := zaplog.DefaultLogger()
    
    // 使用配置构建器
    config := prometheus_mgr.NewConfigBuilder().
        WithServiceName("config_service").
        WithNamespace("myapp").
        WithSubsystem("web").
        WithHTTPPort(9093).
        WithHTTPPath("/custom-metrics").
        WithDefaultLabel("environment", "production").
        WithDefaultLabel("version", "1.0.0").
        WithMetricPrefix("custom").
        WithGoMetrics(true).
        WithProcessMetrics(true).
        WithBasicAuth("admin", "password123").
        WithCORS(true).
        Build()
    
    // 创建管理器
    manager, err := prometheus_mgr.NewManager(
        prometheus_mgr.WithLogger(logger),
        prometheus_mgr.WithConfig(config),
    )
    if err != nil {
        log.Fatal(err)
    }
    
    // 启动管理器
    ctx := context.Background()
    if err := manager.Start(ctx); err != nil {
        log.Fatal(err)
    }
    defer manager.Stop(ctx)
    
    select {} // 保持程序运行
}
```

## 指标类型详解

### Counter（计数器）

只能增加的指标，适用于请求数、错误数等。

```go
// 注册 Counter
err := prometheus_mgr.RegisterCounter("requests_total", prometheus_mgr.MetricOptions{
    Help:   "Total number of requests",
    Labels: []string{"method", "status", "endpoint"},
    ConstLabels: prometheus_mgr.MetricLabels{
        "service": "web",
    },
})

// 使用 Counter
_ = prometheus_mgr.IncCounter("requests_total", prometheus_mgr.MetricLabels{
    "method":   "GET",
    "status":   "200",
    "endpoint": "/api/users",
})

_ = prometheus_mgr.AddCounter("requests_total", 5, prometheus_mgr.MetricLabels{
    "method":   "POST",
    "status":   "201",
    "endpoint": "/api/users",
})
```

### Gauge（仪表盘）

可以增加或减少的指标，适用于内存使用量、连接数等。

```go
// 注册 Gauge
err := prometheus_mgr.RegisterGauge("memory_usage_bytes", prometheus_mgr.MetricOptions{
    Help:   "Memory usage in bytes",
    Labels: []string{"type"},
})

// 使用 Gauge
_ = prometheus_mgr.SetGauge("memory_usage_bytes", 1024*1024*100, prometheus_mgr.MetricLabels{"type": "heap"})
_ = prometheus_mgr.IncGauge("memory_usage_bytes", prometheus_mgr.MetricLabels{"type": "heap"})
_ = prometheus_mgr.DecGauge("memory_usage_bytes", prometheus_mgr.MetricLabels{"type": "heap"})
_ = prometheus_mgr.AddGauge("memory_usage_bytes", 1024, prometheus_mgr.MetricLabels{"type": "heap"})
_ = prometheus_mgr.SubGauge("memory_usage_bytes", 512, prometheus_mgr.MetricLabels{"type": "heap"})
```

### Histogram（直方图）

用于观察值的分布，适用于请求延迟、响应大小等。

```go
// 注册 Histogram
err := prometheus_mgr.RegisterHistogram("request_duration_seconds", prometheus_mgr.MetricOptions{
    Help:    "Request duration in seconds",
    Labels:  []string{"method", "endpoint"},
    Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
})

// 使用 Histogram
_ = prometheus_mgr.ObserveHistogram("request_duration_seconds", 0.123, prometheus_mgr.MetricLabels{
    "method":   "GET",
    "endpoint": "/api/users",
})

// 使用计时器
manager := prometheus_mgr.GetDefaultManager()
timer := manager.NewHistogramTimer("request_duration_seconds", prometheus_mgr.MetricLabels{
    "method":   "POST",
    "endpoint": "/api/users",
})
// ... 执行业务逻辑 ...
timer.ObserveDuration()
```

### Summary（摘要）

类似 Histogram，但提供分位数统计。

```go
// 注册 Summary
err := prometheus_mgr.RegisterSummary("response_size_bytes", prometheus_mgr.MetricOptions{
    Help:   "Response size in bytes",
    Labels: []string{"endpoint"},
    Objectives: map[float64]float64{
        0.5:  0.05,  // 50th percentile with 5% tolerance
        0.9:  0.01,  // 90th percentile with 1% tolerance
        0.99: 0.001, // 99th percentile with 0.1% tolerance
    },
})

// 使用 Summary
_ = prometheus_mgr.ObserveSummary("response_size_bytes", 1024, prometheus_mgr.MetricLabels{
    "endpoint": "/api/users",
})
```

## 自定义收集器

```go
package main

import (
    "github.com/prometheus/client_golang/prometheus"
    "github.com/NumberMan1/component/prometheus-mgr"
)

// 实现自定义收集器
type CustomCollector struct {
    desc *prometheus.Desc
}

func (c *CustomCollector) Name() string {
    return "custom_collector"
}

func (c *CustomCollector) Help() string {
    return "Custom collector example"
}

func (c *CustomCollector) Describe(ch chan<- *prometheus.Desc) {
    if c.desc == nil {
        c.desc = prometheus.NewDesc(
            "custom_metric",
            "Custom metric description",
            []string{"label1"},
            nil,
        )
    }
    ch <- c.desc
}

func (c *CustomCollector) Collect(ch chan<- prometheus.Metric) {
    // 收集自定义指标数据
    metric, err := prometheus.NewConstMetric(
        c.desc,
        prometheus.GaugeValue,
        42.0,
        "value1",
    )
    if err == nil {
        ch <- metric
    }
}

func main() {
    logger := zaplog.DefaultLogger()
    
    // 创建自定义收集器
    customCollector := &CustomCollector{}
    
    // 创建管理器并添加自定义收集器
    manager, err := prometheus_mgr.NewManager(
        prometheus_mgr.WithLogger(logger),
        prometheus_mgr.WithServiceName("custom_service"),
        prometheus_mgr.WithHTTPPort(9094),
        prometheus_mgr.WithCustomCollector(customCollector),
    )
    if err != nil {
        log.Fatal(err)
    }
    
    // 启动管理器
    ctx := context.Background()
    if err := manager.Start(ctx); err != nil {
        log.Fatal(err)
    }
    defer manager.Stop(ctx)
    
    select {} // 保持程序运行
}
```

## 安全配置

### TLS 配置

```go
manager, err := prometheus_mgr.NewManager(
    prometheus_mgr.WithLogger(logger),
    prometheus_mgr.WithServiceName("secure_service"),
    prometheus_mgr.WithHTTPPort(9095),
    prometheus_mgr.WithTLS("/path/to/cert.pem", "/path/to/key.pem"),
)
```

### 基础认证

```go
manager, err := prometheus_mgr.NewManager(
    prometheus_mgr.WithLogger(logger),
    prometheus_mgr.WithServiceName("auth_service"),
    prometheus_mgr.WithHTTPPort(9096),
    prometheus_mgr.WithBasicAuth("admin", "password123"),
)
```

### IP 白名单

```go
manager, err := prometheus_mgr.NewManager(
    prometheus_mgr.WithLogger(logger),
    prometheus_mgr.WithServiceName("restricted_service"),
    prometheus_mgr.WithHTTPPort(9097),
    prometheus_mgr.WithAllowedIPs([]string{"127.0.0.1", "192.168.1.0/24"}),
)
```

### CORS 支持

```go
manager, err := prometheus_mgr.NewManager(
    prometheus_mgr.WithLogger(logger),
    prometheus_mgr.WithServiceName("cors_service"),
    prometheus_mgr.WithHTTPPort(9098),
    prometheus_mgr.WithCORS(true),
)
```

## 配置选项详解

### 基础配置

```go
type Config struct {
    Enabled     bool   `yaml:"enabled"`      // 是否启用
    ServiceName string `yaml:"service_name"` // 服务名称
    Namespace   string `yaml:"namespace"`    // 命名空间
    Subsystem   string `yaml:"subsystem"`    // 子系统
    
    HTTP       HTTPConfig       `yaml:"http"`
    Metrics    MetricsConfig    `yaml:"metrics"`
    Collectors CollectorsConfig `yaml:"collectors"`
    Advanced   AdvancedConfig   `yaml:"advanced"`
}
```

### HTTP 配置

```go
type HTTPConfig struct {
    Enabled      bool          `yaml:"enabled"`       // 是否启用HTTP服务
    Host         string        `yaml:"host"`          // 监听地址
    Port         int           `yaml:"port"`          // 监听端口
    Path         string        `yaml:"path"`          // 指标路径
    ReadTimeout  time.Duration `yaml:"read_timeout"`  // 读取超时
    WriteTimeout time.Duration `yaml:"write_timeout"` // 写入超时
    IdleTimeout  time.Duration `yaml:"idle_timeout"`  // 空闲超时
    TLS          TLSConfig     `yaml:"tls"`           // TLS配置
}
```

### 指标配置

```go
type MetricsConfig struct {
    DefaultLabels               MetricLabels        `yaml:"default_labels"`
    DefaultHistogramBuckets     []float64           `yaml:"default_histogram_buckets"`
    DefaultSummaryObjectives    map[float64]float64 `yaml:"default_summary_objectives"`
    Prefix                      string              `yaml:"prefix"`
    IncludeGoMetrics           bool                `yaml:"include_go_metrics"`
    IncludeProcessMetrics      bool                `yaml:"include_process_metrics"`
}
```

## API 参考

### 管理器接口

```go
type PrometheusManager interface {
    // 注册指标
    RegisterCounter(name string, opts MetricOptions) error
    RegisterGauge(name string, opts MetricOptions) error
    RegisterHistogram(name string, opts MetricOptions) error
    RegisterSummary(name string, opts MetricOptions) error
    
    // 操作 Counter
    IncCounter(name string, labels MetricLabels) error
    AddCounter(name string, value float64, labels MetricLabels) error
    
    // 操作 Gauge
    SetGauge(name string, value float64, labels MetricLabels) error
    IncGauge(name string, labels MetricLabels) error
    DecGauge(name string, labels MetricLabels) error
    AddGauge(name string, value float64, labels MetricLabels) error
    SubGauge(name string, value float64, labels MetricLabels) error
    
    // 操作 Histogram
    ObserveHistogram(name string, value float64, labels MetricLabels) error
    
    // 操作 Summary
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
```

### 全局便捷方法

```go
// 管理器管理
func InitDefaultManager(logger zaplog.Logger, opts ...Option) error
func GetDefaultManager() PrometheusManager
func CreateManager(name string, logger zaplog.Logger, opts ...Option) (PrometheusManager, error)
func GetManager(name string) (PrometheusManager, bool)
func RemoveManager(name string) error
func ListManagers() []string
func StopAllManagers() error

// 指标操作（使用默认管理器）
func RegisterCounter(name string, opts MetricOptions) error
func RegisterGauge(name string, opts MetricOptions) error
func RegisterHistogram(name string, opts MetricOptions) error
func RegisterSummary(name string, opts MetricOptions) error

func IncCounter(name string, labels MetricLabels) error
func AddCounter(name string, value float64, labels MetricLabels) error
func SetGauge(name string, value float64, labels MetricLabels) error
// ... 其他便捷方法
```

## 最佳实践

### 1. 指标命名规范

- 使用小写字母和下划线
- 以单位结尾（如 `_seconds`、`_bytes`、`_total`）
- Counter 指标以 `_total` 结尾
- 避免使用保留字

```go
// 好的命名
"http_requests_total"
"response_time_seconds"
"memory_usage_bytes"
"active_connections"

// 不好的命名
"httpRequests"
"responseTime"
"memUsage"
"connections_active"
```

### 2. 标签使用

- 标签值的基数不要太高（避免标签爆炸）
- 使用有意义的标签名
- 避免在标签值中包含用户 ID 等高基数数据

```go
// 好的标签使用
labels := prometheus_mgr.MetricLabels{
    "method":   "GET",
    "status":   "200",
    "endpoint": "/api/users",
}

// 不好的标签使用（高基数）
labels := prometheus_mgr.MetricLabels{
    "user_id":    "12345",  // 避免
    "request_id": "abc123", // 避免
}
```

### 3. 错误处理

```go
// 总是检查错误
if err := prometheus_mgr.IncCounter("requests_total", labels); err != nil {
    logger.Error("Failed to increment counter", zap.Error(err))
}

// 或者使用批量操作减少错误处理
operations := []struct {
    Name   string
    Labels prometheus_mgr.MetricLabels
}{
    {"requests_total", prometheus_mgr.MetricLabels{"method": "GET"}},
    {"requests_total", prometheus_mgr.MetricLabels{"method": "POST"}},
}

errors := manager.BatchIncCounter(operations)
for i, err := range errors {
    if err != nil {
        logger.Error("Failed to increment counter", 
            zap.Int("index", i), 
            zap.Error(err))
    }
}
```

### 4. 性能优化

```go
// 预先注册所有指标
func init() {
    _ = prometheus_mgr.RegisterCounter("requests_total", prometheus_mgr.MetricOptions{
        Help:   "Total requests",
        Labels: []string{"method", "status"},
    })
}

// 重用标签对象
var getLabels = prometheus_mgr.MetricLabels{"method": "GET"}
var postLabels = prometheus_mgr.MetricLabels{"method": "POST"}

func handleRequest() {
    _ = prometheus_mgr.IncCounter("requests_total", getLabels)
}
```

## 故障排除

### 常见问题

1. **端口被占用**
   ```
   Error: HTTP server failed: listen tcp :9090: bind: address already in use
   ```
   解决：更换端口或停止占用端口的进程

2. **指标未找到**
   ```
   Error: metric requests_total not found
   ```
   解决：确保先注册指标再使用

3. **标签不匹配**
   ```
   Error: inconsistent label cardinality
   ```
   解决：确保使用的标签与注册时的标签一致

4. **内存使用过高**
   - 检查标签基数是否过高
   - 考虑使用 Summary 替代 Histogram
   - 定期清理不再使用的指标

### 调试技巧

1. **启用调试日志**
   ```go
   manager, err := prometheus_mgr.NewManager(
       prometheus_mgr.WithLogger(logger.With(zap.String("level", "debug"))),
       // ... 其他选项
   )
   ```

2. **检查健康状态**
   ```bash
   curl http://localhost:9090/health
   ```

3. **查看指标**
   ```bash
   curl http://localhost:9090/metrics
   ```

4. **使用 Prometheus 查询**
   ```promql
   # 查看所有指标
   {__name__=~".+"}
   
   # 查看特定指标
   requests_total
   
   # 按标签过滤
   requests_total{method="GET"}
   ```

## 版本兼容性

- Go 1.19+
- Prometheus Client Go v1.23+
- 兼容 Prometheus 2.x

## 许可证

本组件遵循项目的许可证协议。