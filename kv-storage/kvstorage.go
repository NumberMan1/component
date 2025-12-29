package kvstorage

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	// ErrMiss 表示未命中（数据不存在或已过期）
	ErrMiss = errors.New("kvstorage: key not found")

	// ErrConfig 配置错误
	ErrConfig = errors.New("kvstorage: invalid config")
)

// 全局单例相关
var (
	globalStore Store
	globalMu    sync.RWMutex
)

// SetGlobal 设置全局存储实例。
// 通常在应用启动初始化时调用一次。
func SetGlobal(s Store) {
	globalMu.Lock()
	defer globalMu.Unlock()
	globalStore = s
}

// Global 获取全局存储实例。
// 在调用前请确保已通过 SetGlobal 初始化，否则可能返回 nil。
func Global() Store {
	globalMu.RLock()
	defer globalMu.RUnlock()
	return globalStore
}

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
	ExpiresAt int64         `json:"e"` // 存储时的绝对过期时间 (毫秒)
}

func (e Entry) GetValue() any { return e.Val }

func (e Entry) GetTTL() time.Duration {
	if e.TTL > 0 {
		return e.TTL
	}
	if e.ExpiresAt > 0 {
		// 使用 UnixMilli 解析
		remain := time.UnixMilli(e.ExpiresAt).Sub(time.Now())
		if remain < 0 {
			return 0
		}
		return remain
	}
	return 0
}

// FetchFunc 定义获取数据的回源函数 (Batch)
// missingFields: 缺失或过期的字段列表
// 返回值: 字段名 -> Item 的映射
type FetchFunc func(missingFields []string) (map[string]Item, error)

// Option 定义操作的配置选项
type Option func(*options)

// options 内部配置结构体
type options struct {
	// lockTTL 用于所有操作，指定锁的有效期。若 > 0 则表示启用锁机制
	lockTTL time.Duration
	// fetchFunc 用于 Get/GetBatch 操作，当缓存未命中时调用的回源函数
	fetchFunc FetchFunc
}

// WithLock 启用锁机制保护该次操作
func WithLock(d time.Duration) Option {
	return func(o *options) {
		o.lockTTL = d
	}
}

// WithFetch 设置回源函数。
// 当 Get/GetBatch 发现缺失字段时，会调用 fn 获取数据，并自动回填到缓存中。
func WithFetch(fn FetchFunc) Option {
	return func(o *options) {
		o.fetchFunc = fn
	}
}

// Store 定义了通用存储组件的接口
type Store interface {
	// --- 字段 (Field) 操作 ---

	// Put 设置单个字段值
	Put(ctx context.Context, key, field string, item Item, opts ...Option) error

	// PutBatch 批量设置字段值
	PutBatch(ctx context.Context, key string, items map[string]Item, opts ...Option) error

	// Get 获取单个字段值
	Get(ctx context.Context, key, field string, opts ...Option) (Item, error)

	// GetBatch 批量获取字段值
	GetBatch(ctx context.Context, key string, fields []string, opts ...Option) (map[string]Item, error)

	// GetAll 获取该 Key 下的所有字段值
	// 针对已过期但物理存在的字段，如果配置了 WithFetch 会触发回源。
	GetAll(ctx context.Context, key string, opts ...Option) (map[string]Item, error)
	
	// Remove 删除一个或多个字段
	Remove(ctx context.Context, key string, fields []string, opts ...Option) error

	// Has 判断字段是否存在且未逻辑过期
	Has(ctx context.Context, key, field string, opts ...Option) (bool, error)

	// Close 关闭连接
	Close() error
}
