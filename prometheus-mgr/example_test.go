package prometheus_mgr

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	zaplog "github.com/NumberMan1/component/zap-logger"
	"github.com/prometheus/client_golang/prometheus"
)

// TestBasicUsage 基本使用示例
func TestBasicUsage(t *testing.T) {
	// 创建logger
	logger := zaplog.DefaultLogger()

	// 创建管理器
	manager, err := NewManager(
		WithLogger(logger),
		WithServiceName("test_service"),
		WithHTTPPort(9091),
		WithNamespace("test"),
		WithSubsystem("example"),
	)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	// 启动管理器
	ctx := context.Background()
	if err := manager.Start(ctx); err != nil {
		t.Fatalf("Failed to start manager: %v", err)
	}
	defer manager.Stop(ctx)

	// 注册指标
	err = manager.RegisterCounter("requests_total", MetricOptions{
		Help:   "Total number of requests",
		Labels: []string{"method", "status"},
	})
	if err != nil {
		t.Fatalf("Failed to register counter: %v", err)
	}

	err = manager.RegisterGauge("active_connections", MetricOptions{
		Help:   "Number of active connections",
		Labels: []string{"server"},
	})
	if err != nil {
		t.Fatalf("Failed to register gauge: %v", err)
	}

	err = manager.RegisterHistogram("request_duration_seconds", MetricOptions{
		Help:    "Request duration in seconds",
		Labels:  []string{"method"},
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	})
	if err != nil {
		t.Fatalf("Failed to register histogram: %v", err)
	}

	// 使用指标
	_ = manager.IncCounter("requests_total", MetricLabels{"method": "GET", "status": "200"})
	_ = manager.AddCounter("requests_total", 5, MetricLabels{"method": "POST", "status": "201"})

	_ = manager.SetGauge("active_connections", 42, MetricLabels{"server": "web1"})
	_ = manager.IncGauge("active_connections", MetricLabels{"server": "web1"})

	_ = manager.ObserveHistogram("request_duration_seconds", 0.123, MetricLabels{"method": "GET"})

	fmt.Println("Basic usage test completed successfully")
}

// TestSingletonUsage 单例模式使用示例
func TestSingletonUsage(t *testing.T) {
	// 初始化默认管理器
	logger := zaplog.DefaultLogger()
	err := InitDefaultManager(logger,
		WithServiceName("singleton_service"),
		WithHTTPPort(9092),
	)
	if err != nil {
		t.Fatalf("Failed to init default manager: %v", err)
	}

	// 启动默认管理器
	ctx := context.Background()
	defaultMgr := GetDefaultManager()
	if err := defaultMgr.Start(ctx); err != nil {
		t.Fatalf("Failed to start default manager: %v", err)
	}
	defer defaultMgr.Stop(ctx)

	// 使用全局便捷方法
	err = RegisterCounter("global_requests_total", MetricOptions{
		Help:   "Total number of global requests",
		Labels: []string{"endpoint"},
	})
	if err != nil {
		t.Fatalf("Failed to register global counter: %v", err)
	}

	_ = IncCounter("global_requests_total", MetricLabels{"endpoint": "/api/v1/users"})
	_ = AddCounter("global_requests_total", 10, MetricLabels{"endpoint": "/api/v1/orders"})

	fmt.Println("Singleton usage test completed successfully")
}

// TestMultiInstanceUsage 多实例使用示例
func TestMultiInstanceUsage(t *testing.T) {
	logger := zaplog.DefaultLogger()

	// 创建多个实例
	webManager, err := CreateManager("web", logger,
		WithServiceName("web_service"),
		WithHTTPPort(9093),
		WithNamespace("web"),
	)
	if err != nil {
		t.Fatalf("Failed to create web manager: %v", err)
	}

	apiManager, err := CreateManager("api", logger,
		WithServiceName("api_service"),
		WithHTTPPort(9094),
		WithNamespace("api"),
	)
	if err != nil {
		t.Fatalf("Failed to create api manager: %v", err)
	}

	// 启动实例
	ctx := context.Background()
	if err := webManager.Start(ctx); err != nil {
		t.Fatalf("Failed to start web manager: %v", err)
	}
	defer webManager.Stop(ctx)

	if err := apiManager.Start(ctx); err != nil {
		t.Fatalf("Failed to start api manager: %v", err)
	}
	defer apiManager.Stop(ctx)

	// 为不同实例注册不同的指标
	err = webManager.RegisterCounter("http_requests_total", MetricOptions{
		Help:   "Total HTTP requests",
		Labels: []string{"path"},
	})
	if err != nil {
		t.Fatalf("Failed to register web counter: %v", err)
	}

	err = apiManager.RegisterCounter("api_calls_total", MetricOptions{
		Help:   "Total API calls",
		Labels: []string{"version"},
	})
	if err != nil {
		t.Fatalf("Failed to register api counter: %v", err)
	}

	// 使用不同实例的指标
	_ = webManager.IncCounter("http_requests_total", MetricLabels{"path": "/home"})
	_ = apiManager.IncCounter("api_calls_total", MetricLabels{"version": "v1"})

	// 列出所有实例
	instances := ListManagers()
	fmt.Printf("Active instances: %v\n", instances)

	fmt.Println("Multi-instance usage test completed successfully")
}

// TestConfigBuilder 配置构建器示例
func TestConfigBuilder(t *testing.T) {
	logger := zaplog.DefaultLogger()

	// 使用配置构建器
	config := NewConfigBuilder().
		WithServiceName("builder_service").
		WithNamespace("builder").
		WithSubsystem("test").
		WithHTTPPort(9095).
		WithHTTPPath("/custom-metrics").
		WithDefaultLabel("environment", "test").
		WithDefaultLabel("version", "1.0.0").
		WithMetricPrefix("custom").
		WithGoMetrics(true).
		WithProcessMetrics(true).
		Build()

	// 创建管理器
	manager, err := NewManager(
		WithLogger(logger),
		WithConfig(config),
	)
	if err != nil {
		t.Fatalf("Failed to create manager with config builder: %v", err)
	}

	// 启动管理器
	ctx := context.Background()
	if err := manager.Start(ctx); err != nil {
		t.Fatalf("Failed to start manager: %v", err)
	}
	defer manager.Stop(ctx)

	// 注册和使用指标
	err = manager.RegisterCounter("custom_events_total", MetricOptions{
		Help:   "Total custom events",
		Labels: []string{"type"},
	})
	if err != nil {
		t.Fatalf("Failed to register counter: %v", err)
	}

	_ = manager.IncCounter("custom_events_total", MetricLabels{"type": "user_action"})

	fmt.Println("Config builder test completed successfully")
}

// TestCustomCollector 自定义收集器示例
func TestCustomCollector(t *testing.T) {
	logger := zaplog.DefaultLogger()

	// 创建自定义收集器
	customCollector := &exampleCustomCollector{
		name: "example_custom_collector",
		help: "Example custom collector",
	}

	// 创建管理器并添加自定义收集器
	manager, err := NewManager(
		WithLogger(logger),
		WithServiceName("custom_collector_service"),
		WithHTTPPort(9096),
		WithCustomCollector(customCollector),
	)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	// 启动管理器
	ctx := context.Background()
	if err := manager.Start(ctx); err != nil {
		t.Fatalf("Failed to start manager: %v", err)
	}
	defer manager.Stop(ctx)

	fmt.Println("Custom collector test completed successfully")
}

// TestHTTPSecurity HTTP安全功能示例
func TestHTTPSecurity(t *testing.T) {
	logger := zaplog.DefaultLogger()

	// 创建带安全配置的管理器
	manager, err := NewManager(
		WithLogger(logger),
		WithServiceName("secure_service"),
		WithHTTPPort(9097),
		WithBasicAuth("admin", "password123"),
		WithCORS(true),
		WithAllowedIPs([]string{"127.0.0.1", "::1"}),
	)
	if err != nil {
		t.Fatalf("Failed to create secure manager: %v", err)
	}

	// 启动管理器
	ctx := context.Background()
	if err := manager.Start(ctx); err != nil {
		t.Fatalf("Failed to start secure manager: %v", err)
	}
	defer manager.Stop(ctx)

	// 测试健康检查端点
	go func() {
		time.Sleep(100 * time.Millisecond) // 等待服务器启动
		resp, err := http.Get("http://127.0.0.1:9097/health")
		if err != nil {
			t.Logf("Health check request failed: %v", err)
			return
		}
		defer resp.Body.Close()
		t.Logf("Health check status: %d", resp.StatusCode)
	}()

	time.Sleep(200 * time.Millisecond) // 等待测试完成
	fmt.Println("HTTP security test completed successfully")
}

// exampleCustomCollector 示例自定义收集器
type exampleCustomCollector struct {
	name string
	help string
	desc *prometheus.Desc
}

func (c *exampleCustomCollector) Name() string {
	return c.name
}

func (c *exampleCustomCollector) Help() string {
	return c.help
}

func (c *exampleCustomCollector) Describe(ch chan<- *prometheus.Desc) {
	if c.desc == nil {
		c.desc = prometheus.NewDesc(
			"example_custom_metric",
			"Example custom metric",
			[]string{"label1"},
			nil,
		)
	}
	ch <- c.desc
}

func (c *exampleCustomCollector) Collect(ch chan<- prometheus.Metric) {
	if c.desc == nil {
		c.desc = prometheus.NewDesc(
			"example_custom_metric",
			"Example custom metric",
			[]string{"label1"},
			nil,
		)
	}

	// 模拟收集指标数据
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

// Example 基本使用示例
func Example() {
	// 创建logger
	logger := zaplog.DefaultLogger()

	// 初始化默认管理器
	err := InitDefaultManager(logger,
		WithServiceName("my_service"),
		WithHTTPPort(9090),
		WithNamespace("myapp"),
	)
	if err != nil {
		panic(err)
	}

	// 启动管理器
	ctx := context.Background()
	manager := GetDefaultManager()
	if err := manager.Start(ctx); err != nil {
		panic(err)
	}
	defer manager.Stop(ctx)

	// 注册指标
	_ = RegisterCounter("requests_total", MetricOptions{
		Help:   "Total number of requests",
		Labels: []string{"method", "status"},
	})

	_ = RegisterGauge("active_connections", MetricOptions{
		Help:   "Number of active connections",
		Labels: []string{"server"},
	})

	_ = RegisterHistogram("request_duration_seconds", MetricOptions{
		Help:   "Request duration in seconds",
		Labels: []string{"method"},
	})

	// 使用指标
	_ = IncCounter("requests_total", MetricLabels{"method": "GET", "status": "200"})
	_ = SetGauge("active_connections", 10, MetricLabels{"server": "web1"})
	_ = ObserveHistogram("request_duration_seconds", 0.123, MetricLabels{"method": "GET"})

	fmt.Println("Metrics registered and updated successfully")
	// Output: Metrics registered and updated successfully
}
