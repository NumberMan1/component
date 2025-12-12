package kvstorage

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrMiss 表示未命中（数据不存在或已过期）
	ErrMiss = errors.New("kvstorage: key not found")

	// ErrConfig 配置错误
	ErrConfig = errors.New("kvstorage: invalid config")
)

// Item 是一个接口，用于定义数据及其有效期。
type Item interface {
	// GetValue 返回实际需要存储的数据
	GetValue() any
	// GetTTL 返回该数据的有效期
	GetTTL() time.Duration
}

// Entry 是 Item 接口的默认实现。
type Entry struct {
	Val       any           `json:"v"` // 实际值
	TTL       time.Duration `json:"-"` // 输入时指定的 TTL
	ExpiresAt int64         `json:"e"` // 存储时的绝对过期时间 (纳秒)
}

func (e Entry) GetValue() any { return e.Val }

func (e Entry) GetTTL() time.Duration {
	if e.TTL > 0 {
		return e.TTL
	}
	if e.ExpiresAt > 0 {
		remain := time.Unix(0, e.ExpiresAt).Sub(time.Now())
		if remain < 0 {
			return 0
		}
		return remain
	}
	return 0
}

// FetchFunc 定义获取数据的回调函数，返回一个 Item (包含值和TTL)
type FetchFunc func() (Item, error)

// Option 定义操作的配置选项
type Option func(*options)

// options 内部配置结构体
type options struct {
	// lockTTL 用于所有操作，指定锁的有效期。若 > 0 则表示启用锁机制
	lockTTL time.Duration
	// fetchFunc 用于 Get 操作，当缓存未命中时调用的回源函数
	fetchFunc FetchFunc
}

// WithLock 启用锁机制保护该次操作
func WithLock(d time.Duration) Option {
	return func(o *options) {
		o.lockTTL = d
	}
}

// WithFetch 设置回源函数。
// 当 Get 返回 ErrMiss 时，会调用 fn 获取数据，并自动回填到缓存中。
// 结合 WithLock 使用可以防止缓存击穿（Cache Stampede）。
func WithFetch(fn FetchFunc) Option {
	return func(o *options) {
		o.fetchFunc = fn
	}
}

// Store 定义了通用存储组件的接口
type Store interface {
	// --- 字段 (Field) 操作 ---

	// Put 设置字段值
	Put(ctx context.Context, key, field string, item Item, opts ...Option) error

	// Get 获取字段值
	// 如果配置了 WithFetch 且发生 Miss，会自动执行回调并回填。
	Get(ctx context.Context, key, field string, opts ...Option) (Item, error)

	// Remove 删除一个或多个字段
	Remove(ctx context.Context, key string, fields []string, opts ...Option) error

	// Has 判断字段是否存在且未逻辑过期
	Has(ctx context.Context, key, field string, opts ...Option) (bool, error)

	// Close 关闭连接
	Close() error
}

// RedisConfig Redis 特有的配置参数
type RedisConfig struct {
	Addr         string
	Password     string
	DB           int
	PoolSize     int
	MinIdleConns int
}
