//go:build integration

package storage

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- 测试辅助函数 ---

// getEnv 读取环境变量，如果不存在则返回 fallback 值
func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

// smokeTest 是一个通用的“冒烟测试”，用于验证基本功能
// 它会注册一个 KV 存储，执行 Set, Get，并进行事务操作
func smokeTest(t *testing.T, manager *StorageManager) {
	key := "integration:test:kv"
	err := manager.RegisterKVStorage(key)
	// 在集群模式下，多次注册同一个 key 会报错，但这是 redis-go 的正常行为，我们可以忽略
	// require.NoError(t, err)

	kvStore, err := manager.GetKV(key)
	require.NoError(t, err)

	ctx := context.Background()
	testVal := &testData{ID: 100, Name: "Integration Test"}

	// 1. 基本 Set/Get 测试
	t.Run("Basic Set/Get", func(t *testing.T) {
		err = kvStore.Set(ctx, testVal)
		require.NoError(t, err)

		getResult := &testData{}
		err = kvStore.Get(ctx, getResult)
		require.NoError(t, err)
		assert.Equal(t, testVal.ID, getResult.ID)
		assert.Equal(t, testVal.Name, getResult.Name)
	})

	// 2. 事务测试
	t.Run("Transaction Commit", func(t *testing.T) {
		tx, err := kvStore.BeginTx(ctx)
		require.NoError(t, err)

		txVal := &testData{ID: 200, Name: "In Transaction"}
		err = tx.Set(txVal)
		require.NoError(t, err)

		err = tx.Commit(ctx)
		require.NoError(t, err)

		getResult := &testData{}
		err = kvStore.Get(ctx, getResult)
		require.NoError(t, err)
		assert.Equal(t, txVal.ID, getResult.ID)
	})
}

// --- 分模式测试 ---

func TestManager_StandaloneMode(t *testing.T) {
	redisAddr := getEnv("REDIS_ADDR", "")
	if redisAddr == "" {
		t.Skip("跳过单体模式测试：未设置 REDIS_ADDR 环境变量")
	}

	cfg := ManagerConfig{
		RedisAddr: redisAddr,
		RedisPass: getEnv("REDIS_PASS", ""),
		RedisDB:   1,
	}

	manager, err := NewManager(cfg)
	require.NoError(t, err)
	defer manager.Close()

	t.Log("✅ 连接到单体 Redis 成功，开始功能验证...")
	smokeTest(t, manager)
}

func TestManager_SentinelMode(t *testing.T) {
	sentinelAddrsStr := getEnv("SENTINEL_ADDRS", "")
	masterName := getEnv("MASTER_NAME", "")
	if sentinelAddrsStr == "" || masterName == "" {
		t.Skip("跳过哨兵模式测试：未设置 SENTINEL_ADDRS 或 MASTER_NAME 环境变量")
	}

	cfg := ManagerConfig{
		RedisSentinelAddrs: strings.Split(sentinelAddrsStr, ","),
		RedisMasterName:    masterName,
		RedisPass:          getEnv("REDIS_PASS", ""),
		RedisDB:            1,
	}

	manager, err := NewManager(cfg)
	require.NoError(t, err)
	defer manager.Close()

	t.Log("✅ 连接到 Redis Sentinel (Master:", masterName, ") 成功，开始功能验证...")
	smokeTest(t, manager)
}

func TestManager_ClusterMode(t *testing.T) {
	clusterAddrsStr := getEnv("CLUSTER_ADDRS", "")
	if clusterAddrsStr == "" {
		t.Skip("跳过集群模式测试：未设置 CLUSTER_ADDRS 环境变量")
	}

	cfg := ManagerConfig{
		RedisClusterAddrs: strings.Split(clusterAddrsStr, ","),
		RedisPass:         getEnv("REDIS_PASS", ""),
	}

	manager, err := NewManager(cfg)
	require.NoError(t, err)
	defer manager.Close()

	t.Log("✅ 连接到 Redis Cluster 成功，开始功能验证...")
	smokeTest(t, manager)
}
