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
	// 单体或主从模式 (提供 Master 地址)
	RedisAddr string `json:"redis_addr" yaml:"redis-addr"`

	// 哨兵模式 (提供哨兵地址列表和 Master 名称)
	RedisMasterName    string   `json:"redis_master_name" yaml:"redis-master-name"`
	RedisSentinelAddrs []string `json:"redis_sentinel_addrs" yaml:"redis-sentinel-addrs"`

	// 集群模式 (提供集群节点地址列表)
	RedisClusterAddrs []string `json:"redis_cluster_addrs" yaml:"redis-cluster-addrs"`

	// 通用配置
	RedisPass string `json:"redis_pass" yaml:"redis-pass"`
	RedisDB   int    `json:"redis_db" yaml:"redis-db"` // 注意: 在集群模式下通常无效
}

// StorageManager 管理 KV、Hash、SortedSet 存储实例，并持有统一的 Redis 客户端
// 注册 Redis 存储时，会自动初始化并复用此客户端
// 支持内存事务快照
type StorageManager struct {
	mu sync.RWMutex

	// 统一 Redis client
	redisClient redis.UniversalClient
	redisCtx    context.Context

	kvs      map[string]KVTransactional
	hashs    map[string]HashTransactional
	zsets    map[string]SortedSetTransactional
	memHashs map[string]MemoryTransactional
}

// NewManager 根据配置创建 StorageManager
func NewManager(cfg ManagerConfig) (*StorageManager, error) {
	var client redis.Cmdable // 使用接口类型以兼容不同客户端

	switch {
	case len(cfg.RedisClusterAddrs) > 0:
		// 集群模式
		clusterClient := redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:    cfg.RedisClusterAddrs,
			Password: cfg.RedisPass,
		})
		client = clusterClient

	case len(cfg.RedisSentinelAddrs) > 0 && cfg.RedisMasterName != "":
		// 哨兵模式
		failoverClient := redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:    cfg.RedisMasterName,
			SentinelAddrs: cfg.RedisSentinelAddrs,
			Password:      cfg.RedisPass,
			DB:            cfg.RedisDB,
		})
		client = failoverClient

	case cfg.RedisAddr != "":
		// 单体或主从模式
		simpleClient := redis.NewClient(&redis.Options{
			Addr:     cfg.RedisAddr,
			Password: cfg.RedisPass,
			DB:       cfg.RedisDB,
		})
		client = simpleClient

	default:
		return nil, errors.New("无效的 Redis 配置：必须提供 Addr, SentinelAddrs, 或 ClusterAddrs")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := client.Ping(ctx).Err()
	if err != nil {
		return nil, err
	}

	var universalClient redis.UniversalClient

	// (此部分为修改后的最终逻辑)
	if len(cfg.RedisClusterAddrs) > 0 {
		universalClient = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:    cfg.RedisClusterAddrs,
			Password: cfg.RedisPass,
		})
	} else if len(cfg.RedisSentinelAddrs) > 0 && cfg.RedisMasterName != "" {
		universalClient = redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:    cfg.RedisMasterName,
			SentinelAddrs: cfg.RedisSentinelAddrs,
			Password:      cfg.RedisPass,
			DB:            cfg.RedisDB,
		})
	} else if cfg.RedisAddr != "" {
		universalClient = redis.NewClient(&redis.Options{
			Addr:     cfg.RedisAddr,
			Password: cfg.RedisPass,
			DB:       cfg.RedisDB,
		})
	} else {
		return nil, errors.New("无效的 Redis 配置")
	}

	if err := universalClient.Ping(ctx).Err(); err != nil {
		return nil, err
	}

	return &StorageManager{
		redisClient: universalClient,
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
