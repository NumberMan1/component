package prometheus_mgr

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	zaplog "github.com/NumberMan1/component/zap-logger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

// manager Prometheus管理器实现
type manager struct {
	options  Options
	logger   zaplog.Logger
	registry *prometheus.Registry

	// 指标存储
	counters   map[string]*prometheus.CounterVec
	gauges     map[string]*prometheus.GaugeVec
	histograms map[string]*prometheus.HistogramVec
	summaries  map[string]*prometheus.SummaryVec
	mu         sync.RWMutex

	// HTTP服务器
	httpServer *http.Server

	// 状态管理
	started atomic.Bool
	stopped atomic.Bool

	// 上下文和取消函数
	ctx    context.Context
	cancel context.CancelFunc

	// 自定义收集器
	customCollectors []CustomCollector
	collectorsMu     sync.RWMutex
}

// NewManager 创建新的Prometheus管理器
func NewManager(opts ...Option) (PrometheusManager, error) {
	options := DefaultOptions()
	for _, opt := range opts {
		opt(&options)
	}

	// 验证配置
	if err := validateOptions(&options); err != nil {
		return nil, NewConfigError("invalid options", err)
	}

	// 创建注册器
	registry := options.Registry
	if registry == nil {
		if options.Config.Advanced.Registry.UseGlobal {
			registry = prometheus.DefaultRegisterer.(*prometheus.Registry)
		} else {
			registry = prometheus.NewRegistry()
		}
	}

	ctx, cancel := context.WithCancel(context.Background())

	m := &manager{
		options:          options,
		logger:           options.Logger,
		registry:         registry,
		counters:         make(map[string]*prometheus.CounterVec),
		gauges:           make(map[string]*prometheus.GaugeVec),
		histograms:       make(map[string]*prometheus.HistogramVec),
		summaries:        make(map[string]*prometheus.SummaryVec),
		ctx:              ctx,
		cancel:           cancel,
		customCollectors: options.CustomCollectors,
	}

	// 注册内置收集器
	if err := m.registerBuiltinCollectors(); err != nil {
		m.logger.Error("Failed to register builtin collectors", zap.Error(err))
	}

	// 注册自定义收集器
	if err := m.registerCustomCollectors(); err != nil {
		m.logger.Error("Failed to register custom collectors", zap.Error(err))
	}

	return m, nil
}

// validateOptions 验证选项
func validateOptions(options *Options) error {
	if options.Logger == nil {
		return ErrMissingLogger
	}

	config := &options.Config
	if config.HTTP.Port <= 0 || config.HTTP.Port > 65535 {
		return ErrInvalidPort
	}

	if config.HTTP.Path == "" {
		config.HTTP.Path = "/metrics"
	}

	if len(config.Metrics.DefaultHistogramBuckets) == 0 {
		config.Metrics.DefaultHistogramBuckets = prometheus.DefBuckets
	}

	if len(config.Metrics.DefaultSummaryObjectives) == 0 {
		config.Metrics.DefaultSummaryObjectives = map[float64]float64{
			0.5:  0.05,
			0.9:  0.01,
			0.99: 0.001,
		}
	}

	return nil
}

// registerBuiltinCollectors 注册内置收集器
func (m *manager) registerBuiltinCollectors() error {
	if !m.options.Config.Collectors.BuiltinEnabled {
		return nil
	}

	if m.options.Config.Metrics.IncludeGoMetrics {
		if err := m.registry.Register(collectors.NewGoCollector()); err != nil {
			m.logger.Warn("Failed to register Go collector", zap.Error(err))
		}
	}

	if m.options.Config.Metrics.IncludeProcessMetrics {
		if err := m.registry.Register(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{})); err != nil {
			m.logger.Warn("Failed to register process collector", zap.Error(err))
		}
	}

	return nil
}

// registerCustomCollectors 注册自定义收集器
func (m *manager) registerCustomCollectors() error {
	m.collectorsMu.Lock()
	defer m.collectorsMu.Unlock()

	for _, collector := range m.customCollectors {
		if err := m.registry.Register(collector); err != nil {
			m.logger.Error("Failed to register custom collector",
				zap.String("name", collector.Name()),
				zap.Error(err))
			continue
		}
		m.logger.Info("Registered custom collector", zap.String("name", collector.Name()))
	}

	return nil
}

// Start 启动管理器
func (m *manager) Start(ctx context.Context) error {
	if m.started.Load() {
		return ErrManagerAlreadyStarted
	}

	if m.stopped.Load() {
		return ErrManagerStopped
	}

	// 启动HTTP服务器
	if m.options.Config.HTTP.Enabled {
		if err := m.startHTTPServer(); err != nil {
			return NewHTTPError("failed to start HTTP server", err)
		}
	}

	m.started.Store(true)
	m.logger.Info("Prometheus manager started",
		zap.String("instance", m.options.InstanceName),
		zap.Bool("http_enabled", m.options.Config.HTTP.Enabled),
		zap.Int("port", m.options.Config.HTTP.Port))

	return nil
}

// Stop 停止管理器
func (m *manager) Stop(ctx context.Context) error {
	if !m.started.Load() {
		return ErrManagerNotStarted
	}

	if m.stopped.Load() {
		return nil
	}

	// 停止HTTP服务器
	if m.httpServer != nil {
		shutdownCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()

		if err := m.httpServer.Shutdown(shutdownCtx); err != nil {
			m.logger.Error("Failed to shutdown HTTP server", zap.Error(err))
		}
	}

	// 取消上下文
	m.cancel()

	m.stopped.Store(true)
	m.logger.Info("Prometheus manager stopped", zap.String("instance", m.options.InstanceName))

	return nil
}

// startHTTPServer 启动HTTP服务器
func (m *manager) startHTTPServer() error {
	mux := http.NewServeMux()

	// 创建处理器
	handler := promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{
		ErrorLog:      &zapLoggerAdapter{logger: m.logger},
		ErrorHandling: promhttp.ContinueOnError,
	})

	// 应用中间件
	for _, middleware := range m.options.Middlewares {
		handler = middleware(handler)
	}

	// 应用安全中间件
	handler = m.applySecurityMiddleware(handler)

	mux.Handle(m.options.Config.HTTP.Path, handler)

	// 健康检查端点
	mux.HandleFunc("/health", m.healthCheckHandler)

	addr := fmt.Sprintf("%s:%d", m.options.Config.HTTP.Host, m.options.Config.HTTP.Port)
	m.httpServer = &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  m.options.Config.HTTP.ReadTimeout,
		WriteTimeout: m.options.Config.HTTP.WriteTimeout,
		IdleTimeout:  m.options.Config.HTTP.IdleTimeout,
	}

	go func() {
		var err error
		if m.options.Config.HTTP.TLS.Enabled {
			err = m.httpServer.ListenAndServeTLS(
				m.options.Config.HTTP.TLS.CertFile,
				m.options.Config.HTTP.TLS.KeyFile,
			)
		} else {
			err = m.httpServer.ListenAndServe()
		}

		if err != nil && err != http.ErrServerClosed {
			m.logger.Error("HTTP server error", zap.Error(err))
		}
	}()

	m.logger.Info("HTTP server started",
		zap.String("addr", addr),
		zap.String("path", m.options.Config.HTTP.Path),
		zap.Bool("tls", m.options.Config.HTTP.TLS.Enabled))

	return nil
}

// applySecurityMiddleware 应用安全中间件
func (m *manager) applySecurityMiddleware(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// IP白名单检查
		if len(m.options.Config.Advanced.Security.AllowedIPs) > 0 {
			if !m.isIPAllowed(r.RemoteAddr) {
				http.Error(w, "IP not allowed", http.StatusForbidden)
				return
			}
		}

		// 基础认证
		if m.options.Config.Advanced.Security.BasicAuth.Enabled {
			username, password, ok := r.BasicAuth()
			if !ok ||
				username != m.options.Config.Advanced.Security.BasicAuth.Username ||
				password != m.options.Config.Advanced.Security.BasicAuth.Password {
				w.Header().Set("WWW-Authenticate", `Basic realm="Prometheus Metrics"`)
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}
		}

		// CORS处理
		if m.options.Config.Advanced.Security.EnableCORS {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

			if r.Method == "OPTIONS" {
				w.WriteHeader(http.StatusOK)
				return
			}
		}

		handler.ServeHTTP(w, r)
	})
}

// isIPAllowed 检查IP是否在白名单中
func (m *manager) isIPAllowed(remoteAddr string) bool {
	// 简化实现，实际应该解析IP地址
	for _, allowedIP := range m.options.Config.Advanced.Security.AllowedIPs {
		if allowedIP == remoteAddr {
			return true
		}
	}
	return false
}

// healthCheckHandler 健康检查处理器
func (m *manager) healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	if err := m.HealthCheck(); err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprintf(w, "Health check failed: %v", err)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "OK")
}

// HealthCheck 健康检查
func (m *manager) HealthCheck() error {
	if !m.started.Load() {
		return ErrManagerNotStarted
	}

	if m.stopped.Load() {
		return ErrManagerStopped
	}

	return nil
}

// GetRegistry 获取注册器
func (m *manager) GetRegistry() *prometheus.Registry {
	return m.registry
}

// GetHTTPHandler 获取HTTP处理器
func (m *manager) GetHTTPHandler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{
		ErrorLog:      &zapLoggerAdapter{logger: m.logger},
		ErrorHandling: promhttp.ContinueOnError,
	})
}

// zapLoggerAdapter zap日志适配器
type zapLoggerAdapter struct {
	logger zaplog.Logger
}

func (z *zapLoggerAdapter) Println(v ...interface{}) {
	z.logger.Error(fmt.Sprint(v...))
}
