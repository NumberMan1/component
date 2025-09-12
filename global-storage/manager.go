package storage

import (
	"context"
	"errors"
	"strings"
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

// ManagerConfig 定义了 StorageManager 的配置项
// 支持 "standalone", "sentinel", "cluster" 三种模式
type ManagerConfig struct {
	Mode       string   `json:"mode" yaml:"mode"`               // "standalone", "sentinel", "cluster"
	Addrs      []string `json:"addrs" yaml:"addrs"`             // Redis 地址列表。standalone/sentinel模式下第一个地址是主地址
	MasterName string   `json:"master_name" yaml:"master-name"` // 哨兵模式下的 master name
	RedisPass  string   `json:"redis_pass" yaml:"redis-pass"`
	RedisDB    int      `json:"redis_db" yaml:"redis-db"` // 注意：集群模式不支持选择 DB
}

// StorageManager 管理存储实例，并持有统一的 Redis 客户端接口
type StorageManager struct {
	mu sync.RWMutex

	// redis.Cmdable 是一个通用接口，被 redis.Client, redis.FailoverClient, redis.ClusterClient 实现
	redisClient redis.Cmdable
	redisCtx    context.Context

	kvs      map[string]KVTransactional
	hashs    map[string]HashTransactional
	zsets    map[string]SortedSetTransactional
	memHashs map[string]MemoryTransactional
}

// NewManager 根据配置创建 StorageManager，自动识别模式并创建对应客户端
func NewManager(cfg ManagerConfig) (*StorageManager, error) {
	var client redis.Cmdable
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	switch strings.ToLower(cfg.Mode) {
	case "sentinel":
		if cfg.MasterName == "" || len(cfg.Addrs) == 0 {
			return nil, errors.New("sentinel mode requires master_name and at least one address")
		}
		// 注意: NewFailoverClient 返回的是 *redis.Client 类型
		failoverClient := redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:    cfg.MasterName,
			SentinelAddrs: cfg.Addrs,
			Password:      cfg.RedisPass,
			DB:            cfg.RedisDB,
		})
		if err := failoverClient.Ping(ctx).Err(); err != nil {
			return nil, err
		}
		client = failoverClient
	case "cluster":
		if len(cfg.Addrs) == 0 {
			return nil, errors.New("cluster mode requires at least one address")
		}
		clusterClient := redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:    cfg.Addrs,
			Password: cfg.RedisPass,
		})
		if err := clusterClient.Ping(ctx).Err(); err != nil {
			return nil, err
		}
		client = clusterClient
	case "standalone", "": // 默认为单机模式
		if len(cfg.Addrs) == 0 {
			return nil, errors.New("standalone mode requires one address")
		}
		standaloneClient := redis.NewClient(&redis.Options{
			Addr:     cfg.Addrs[0],
			Password: cfg.RedisPass,
			DB:       cfg.RedisDB,
		})
		if err := standaloneClient.Ping(ctx).Err(); err != nil {
			return nil, err
		}
		client = standaloneClient
	default:
		return nil, errors.New("unsupported redis mode: " + cfg.Mode)
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
	// **FIXED**: 移除了对 redis.FailoverClient 的错误断言。
	// NewFailoverClient 返回 *redis.Client，所以它会被第一个 case 捕获。
	if c, ok := m.redisClient.(*redis.Client); ok {
		return c.Close()
	}
	if c, ok := m.redisClient.(*redis.ClusterClient); ok {
		return c.Close()
	}
	// `redis.Ring` 也是一种可能性，尽管我们在这里没有创建它
	if c, ok := m.redisClient.(*redis.Ring); ok {
		return c.Close()
	}
	return nil
}

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
