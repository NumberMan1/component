package storage

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

var globalManager *StorageManager

func InitManager(config ManagerConfig) (err error) {
	globalManager, err = NewManager(config)
	return
}

func GlobalManager() *StorageManager {
	return globalManager
}

// ManagerConfig 定义 StorageManager 的配置项，如 Redis 连接信息
// RedisPass 可为空，RedisDB 默认为 0
// 示例：{RedisAddr: "localhost:6379", RedisPass: "", RedisDB: 0}
type ManagerConfig struct {
	RedisAddr string `json:"redis_addr" yaml:"redis-addr"`
	RedisPass string `json:"redis_pass" yaml:"redis-pass"`
	RedisDB   int    `json:"redis_db" yaml:"redis-db"`
}

// StorageManager 管理 KV、Hash、SortedSet 存储实例，并持有统一的 Redis 客户端
// 注册 Redis 存储时，会自动初始化并复用此客户端
// 支持内存事务快照
type StorageManager struct {
	mu sync.RWMutex

	// 统一 Redis client
	redisClient *redis.Client
	redisCtx    context.Context

	kvs      map[string]KVTransactional
	hashs    map[string]HashTransactional
	zsets    map[string]SortedSetTransactional
	memHashs map[string]MemoryTransactional
}

// NewManager 根据配置创建 StorageManager
func NewManager(cfg ManagerConfig) (*StorageManager, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPass,
		DB:       cfg.RedisDB,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := client.Ping(ctx).Err()
	if err != nil {
		return nil, err
	}

	return &StorageManager{
		redisClient: client,
		redisCtx:    context.Background(),
		kvs:         make(map[string]KVTransactional),
		hashs:       make(map[string]HashTransactional),
		zsets:       make(map[string]SortedSetTransactional),
		memHashs:    make(map[string]MemoryTransactional),
	}, nil
}

// Close 关闭 StorageManager 持有的 Redis 客户端连接
func (m *StorageManager) Close() error {
	if m.redisClient != nil {
		return m.redisClient.Close()
	}
	return nil
}

// —— Redis 注册方法 ——

// RegisterKVStorage 直接通过 Manager 的 Redis 客户端注册 KV 存储
func (m *StorageManager) RegisterKVStorage(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.kvs[name]; exists {
		return errors.New("KV storage already registered: " + name)
	}
	m.kvs[name] = NewRedisKV(m.redisClient, name)
	return nil
}

// RegisterHashStorage 直接通过 Manager 的 Redis 客户端注册 Hash 存储
func (m *StorageManager) RegisterHashStorage(name string, dataFactory StorageDataFactory) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.hashs[name]; exists {
		return errors.New("Hash storage already registered: " + name)
	}
	m.hashs[name] = NewRedisHash(m.redisClient, name, dataFactory)
	return nil
}

// RegisterSortedSetStorage 直接通过 Manager 的 Redis 客户端注册 SortedSet 存储
func (m *StorageManager) RegisterSortedSetStorage(name string, dataFactory SortedSetDataFactory) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.zsets[name]; exists {
		return errors.New("SortedSet storage already registered: " + name)
	}
	m.zsets[name] = NewRedisZSet(m.redisClient, name, dataFactory)
	return nil
}

// RegisterMemoryHash 直接通过 Manager 构建内存仓储
func (m *StorageManager) RegisterMemoryHash(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.memHashs[name]; exists {
		return errors.New("MemoryHash storage already registered: " + name)
	}
	m.memHashs[name] = NewMemoryStore()
	return nil
}

// —— 通用获取与事务方法 ——

// GetKV 获取已注册的 KV 存储
func (m *StorageManager) GetKV(name string) (KVTransactional, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if s, ok := m.kvs[name]; ok {
		return s, nil
	}
	return nil, errors.New("KV storage not found: " + name)
}

// GetHash 获取已注册的 Hash 存储
func (m *StorageManager) GetHash(name string) (HashTransactional, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if s, ok := m.hashs[name]; ok {
		return s, nil
	}
	return nil, errors.New("Hash storage not found: " + name)
}

// GetSortedSet 获取已注册的 SortedSet 存储
func (m *StorageManager) GetSortedSet(name string) (SortedSetTransactional, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if s, ok := m.zsets[name]; ok {
		return s, nil
	}
	return nil, errors.New("SortedSet storage not found: " + name)
}

// GetMemoryHash 获取已注册的 MemoryHash 存储
func (m *StorageManager) GetMemoryHash(name string) (MemoryTransactional, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if s, ok := m.memHashs[name]; ok {
		return s, nil
	}
	return nil, errors.New("MemoryHash storage not found: " + name)
}
