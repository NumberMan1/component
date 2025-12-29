package prometheus_mgr

import (
	"errors"
	"fmt"
)

// 预定义错误
var (
	// 通用错误
	ErrManagerNotInitialized = errors.New("prometheus manager not initialized")
	ErrManagerAlreadyStarted = errors.New("prometheus manager already started")
	ErrManagerNotStarted     = errors.New("prometheus manager not started")
	ErrManagerStopped        = errors.New("prometheus manager stopped")

	// 配置错误
	ErrInvalidConfig     = errors.New("invalid config")
	ErrMissingLogger     = errors.New("logger is required")
	ErrInvalidPort       = errors.New("invalid port number")
	ErrInvalidPath       = errors.New("invalid metrics path")
	ErrInvalidBuckets    = errors.New("invalid histogram buckets")
	ErrInvalidObjectives = errors.New("invalid summary objectives")

	// 指标错误
	ErrMetricNotFound      = errors.New("metric not found")
	ErrMetricAlreadyExists = errors.New("metric already exists")
	ErrInvalidMetricName   = errors.New("invalid metric name")
	ErrInvalidMetricType   = errors.New("invalid metric type")
	ErrInvalidLabels       = errors.New("invalid labels")
	ErrMetricTypeMismatch  = errors.New("metric type mismatch")
	ErrTooManyMetrics      = errors.New("too many metrics")

	// 注册器错误
	ErrRegistryFull        = errors.New("registry is full")
	ErrRegistrationFailed  = errors.New("metric registration failed")
	ErrUnregistrationFailed = errors.New("metric unregistration failed")

	// HTTP服务错误
	ErrHTTPServerFailed    = errors.New("HTTP server failed")
	ErrTLSConfigInvalid    = errors.New("TLS config invalid")
	ErrAuthenticationFailed = errors.New("authentication failed")
	ErrIPNotAllowed        = errors.New("IP not allowed")

	// 收集器错误
	ErrCollectorFailed     = errors.New("collector failed")
	ErrInvalidCollector    = errors.New("invalid collector")
	ErrCollectorNotFound   = errors.New("collector not found")
)

// PrometheusError Prometheus错误类型
type PrometheusError struct {
	Code    string
	Message string
	Cause   error
	Context map[string]interface{}
}

func (e *PrometheusError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("[%s] %s: %v", e.Code, e.Message, e.Cause)
	}
	return fmt.Sprintf("[%s] %s", e.Code, e.Message)
}

func (e *PrometheusError) Unwrap() error {
	return e.Cause
}

func (e *PrometheusError) WithContext(key string, value interface{}) *PrometheusError {
	if e.Context == nil {
		e.Context = make(map[string]interface{})
	}
	e.Context[key] = value
	return e
}

// NewPrometheusError 创建新的Prometheus错误
func NewPrometheusError(code, message string, cause error) *PrometheusError {
	return &PrometheusError{
		Code:    code,
		Message: message,
		Cause:   cause,
		Context: make(map[string]interface{}),
	}
}

// 错误构造函数

// NewConfigError 创建配置错误
func NewConfigError(message string, cause error) *PrometheusError {
	return NewPrometheusError("CONFIG_ERROR", message, cause)
}

// NewMetricError 创建指标错误
func NewMetricError(message string, cause error) *PrometheusError {
	return NewPrometheusError("METRIC_ERROR", message, cause)
}

// NewRegistryError 创建注册器错误
func NewRegistryError(message string, cause error) *PrometheusError {
	return NewPrometheusError("REGISTRY_ERROR", message, cause)
}

// NewHTTPError 创建HTTP错误
func NewHTTPError(message string, cause error) *PrometheusError {
	return NewPrometheusError("HTTP_ERROR", message, cause)
}

// NewCollectorError 创建收集器错误
func NewCollectorError(message string, cause error) *PrometheusError {
	return NewPrometheusError("COLLECTOR_ERROR", message, cause)
}

// 错误检查函数

// IsConfigError 检查是否为配置错误
func IsConfigError(err error) bool {
	var promErr *PrometheusError
	return errors.As(err, &promErr) && promErr.Code == "CONFIG_ERROR"
}

// IsMetricError 检查是否为指标错误
func IsMetricError(err error) bool {
	var promErr *PrometheusError
	return errors.As(err, &promErr) && promErr.Code == "METRIC_ERROR"
}

// IsRegistryError 检查是否为注册器错误
func IsRegistryError(err error) bool {
	var promErr *PrometheusError
	return errors.As(err, &promErr) && promErr.Code == "REGISTRY_ERROR"
}

// IsHTTPError 检查是否为HTTP错误
func IsHTTPError(err error) bool {
	var promErr *PrometheusError
	return errors.As(err, &promErr) && promErr.Code == "HTTP_ERROR"
}

// IsCollectorError 检查是否为收集器错误
func IsCollectorError(err error) bool {
	var promErr *PrometheusError
	return errors.As(err, &promErr) && promErr.Code == "COLLECTOR_ERROR"
}