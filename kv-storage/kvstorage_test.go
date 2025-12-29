package kvstorage

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
)

// TestConfig Redis 测试配置
var TestConfig = RedisConfig{
	RedisAddr: "127.0.0.1:6379",
	RedisPass: "123456",
	RedisDB:   2,
}

// AuthToken 测试实现 Item 接口的对象
type AuthToken struct {
	Token     string
	ExpiresIn time.Duration
}

func (a AuthToken) GetValue() any         { return a.Token }
func (a AuthToken) GetTTL() time.Duration { return a.ExpiresIn }

// SlowItem 用于测试锁竞争
type SlowItem struct {
	Val   any
	Delay time.Duration
}

func (s SlowItem) GetValue() any {
	time.Sleep(s.Delay)
	return s.Val
}
func (s SlowItem) GetTTL() time.Duration { return 0 }

func setupRedis(t *testing.T) Store {
	// 连接 Redis
	rdb := redis.NewClient(&redis.Options{
		Addr:     TestConfig.RedisAddr,
		Password: TestConfig.RedisPass,
		DB:       TestConfig.RedisDB,
	})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		if t != nil {
			t.Skipf("Skipping Redis test: cannot connect to redis: %v", err)
		}
		return nil
	}

	// 清空 DB，防止测试间数据污染
	rdb.FlushDB(context.Background())
	rdb.Close()

	s, err := NewRedisStore(TestConfig)
	if err != nil {
		if t != nil {
			t.Fatal(err)
		}
		return nil
	}
	return s
}

func setupMem(t *testing.T) Store {
	s, err := NewMemStore(MemConfig{})
	if err != nil {
		if t != nil {
			t.Fatalf("Failed to init MemStore: %v", err)
		}
		return nil
	}
	return s
}

func runBackends(t *testing.T, testFunc func(*testing.T, Store)) {
	backends := []struct {
		name  string
		setup func(*testing.T) Store
	}{
		{"Redis", setupRedis},
		{"Mem", setupMem},
	}
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			s := b.setup(t)
			if s == nil {
				return
			}
			defer s.Close()
			testFunc(t, s)
		})
	}
}

func TestDistributedLock(t *testing.T) {
	// 1. 准备配置
	redisCfg := RedisConfig{
		RedisAddr: "127.0.0.1:6379",
		RedisPass: "123456", // 请确保密码正确
		RedisDB:   2,
	}

	// 2. 模拟 "节点A" (Client A)
	nodeA, err := NewRedisStore(redisCfg)
	if err != nil {
		t.Skipf("Skipping test: cannot connect to redis: %v", err)
		return
	}
	defer nodeA.Close()

	// 3. 模拟 "节点B" (Client B)
	nodeB, err := NewRedisStore(redisCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer nodeB.Close()

	// 4. 准备辅助连接 (System)
	ctx := context.Background()
	testKey := "resource:shared_order"
	lockKey := testKey + ":lock"

	rdb := redis.NewClient(&redis.Options{
		Addr:     redisCfg.RedisAddr,
		Password: redisCfg.RedisPass,
		DB:       redisCfg.RedisDB,
	})
	defer rdb.Close() // <--- 修正：使用 defer 关闭，而不是立即关闭

	// 清理环境
	rdb.Del(ctx, lockKey)

	// --- 场景开始 ---

	// 5. [System] 手动在 Redis 上制造一把锁 (模拟 Node A 已经持有了锁)
	// 有效期 3 秒
	isLocked, err := rdb.SetNX(ctx, lockKey, "node-a-token", 3*time.Second).Result()
	if err != nil {
		t.Fatalf("测试环境准备失败: Redis连接错误 %v", err)
	}
	if !isLocked {
		// 可能是上次测试残留，强制清理重试
		rdb.Del(ctx, lockKey)
		isLocked, _ = rdb.SetNX(ctx, lockKey, "node-a-token", 3*time.Second).Result()
		if !isLocked {
			t.Fatal("测试环境准备失败: 无法创建模拟锁")
		}
	}
	fmt.Println("[System] 模拟 Node A 已在 Redis 上持有分布式锁 (SetNX 成功)")

	// 6. [Node A] 开启一个 Goroutine 尝试获取锁
	// 预期：因为它和 System 抢同一个 Key，且 System 还没释放，它应该失败
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		fmt.Println("[Node A] 尝试获取锁...")
		// 这里的 WithLock 会尝试去 Redis SetNX，但应该发现 key 已存在
		err := nodeA.Put(ctx, testKey, "field1", Entry{Val: "A-Data"}, WithLock(2*time.Second))
		if err == nil {
			fmt.Println("[Node A] 竟然获取锁成功了 (意外)")
		} else {
			fmt.Printf("[Node A] 获取锁失败 (预期内): %v\n", err)
		}
	}()

	// 7. [Node B] 主线程尝试获取锁
	// 预期：锁被 System 占用，Node B 应该拿到 "lock busy" 错误
	fmt.Println("[Node B] 尝试获取锁...")
	start := time.Now()

	err = nodeB.Put(ctx, testKey, "field2", Entry{Val: "B-Data"}, WithLock(1*time.Second))

	if err == nil {
		// 如果进到这里，说明分布式锁失效了
		t.Fatal("[Node B] 致命错误：在锁被占用时，Node B 依然写入成功！分布式锁失效。")
	} else {
		// 验证错误类型
		if err.Error() == "kvstorage: lock busy" {
			fmt.Printf("[Node B] 获取锁失败，符合预期 (耗时: %v)\n", time.Since(start))
		} else {
			t.Fatalf("[Node B] 失败但错误类型不对: %v", err)
		}
	}

	// 8. 等待锁过期后再次尝试
	fmt.Println("[System] 等待 3.5 秒让锁过期...")
	time.Sleep(3500 * time.Millisecond)

	fmt.Println("[Node B] 再次尝试获取锁...")
	err = nodeB.Put(ctx, testKey, "field2", Entry{Val: "B-Data"}, WithLock(1*time.Second))
	if err != nil {
		t.Fatalf("[Node B] 锁应该已释放，但依然失败: %v", err)
	}
	fmt.Println("[Node B] 成功获取锁并写入")

	wg.Wait()
}

// TestSingleton 测试全局单例模式
func TestSingleton(t *testing.T) {
	s := setupMem(t)
	if s == nil {
		return
	}

	// 1. 设置全局实例
	SetGlobal(s)

	// 2. 获取并验证
	g := Global()
	if g == nil {
		t.Fatal("Global store should not be nil")
	}
	if g != s {
		t.Error("Global store does not match the instance set")
	}

	// 3. 验证功能可用性
	ctx := context.Background()
	key := "test:singleton"
	if err := g.Put(ctx, key, "f1", Entry{Val: "v1"}); err != nil {
		t.Fatal(err)
	}

	item, err := g.Get(ctx, key, "f1")
	if err != nil {
		t.Fatal(err)
	}
	if item.GetValue() != "v1" {
		t.Error("Value mismatch in singleton test")
	}
}

// TestEntryLogic 纯单元测试，专门覆盖 Entry 的方法逻辑
func TestEntryLogic(t *testing.T) {
	val := "test-val"
	e0 := Entry{Val: val}
	if e0.GetValue() != val {
		t.Errorf("GetValue failed, expected %v got %v", val, e0.GetValue())
	}

	e1 := Entry{TTL: time.Hour}
	if e1.GetTTL() != time.Hour {
		t.Errorf("GetTTL failed for explicit TTL")
	}

	// 统一使用 UnixMilli
	future := time.Now().Add(time.Hour).UnixMilli()
	e2 := Entry{ExpiresAt: future}
	ttl := e2.GetTTL()
	if ttl <= 0 || ttl > time.Hour {
		t.Errorf("GetTTL failed for future ExpiresAt, got %v", ttl)
	}

	past := time.Now().Add(-time.Hour).UnixMilli()
	e3 := Entry{ExpiresAt: past}
	if e3.GetTTL() != 0 {
		t.Errorf("GetTTL should return 0 for expired time, got %v", e3.GetTTL())
	}

	e4 := Entry{}
	if e4.GetTTL() != 0 {
		t.Errorf("GetTTL should return 0 for zero entry")
	}
}

func TestInitError(t *testing.T) {
	_, err := NewRedisStore(RedisConfig{})
	if !errors.Is(err, ErrConfig) {
		t.Errorf("Expected ErrConfig for empty addr, got %v", err)
	}
}

// TestGetWithFetch 测试 Cache Aside 模式 (自动回填)
func TestGetWithFetch(t *testing.T) {
	runBackends(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		key := "test:fetch"
		field := "f1"

		// 1. 确保数据不存在
		_, err := s.Get(ctx, key, field)
		if !errors.Is(err, ErrMiss) {
			t.Fatalf("Expected ErrMiss, got %v", err)
		}

		// 2. 带 Fetch 回调的 Get
		fetchCalled := false
		expectedVal := "fetched-val"

		// 适配新的 FetchFunc 签名
		fetchFn := func(missingFields []string) (map[string]Item, error) {
			fetchCalled = true
			if len(missingFields) != 1 || missingFields[0] != field {
				t.Errorf("FetchFunc received wrong fields: %v", missingFields)
			}
			return map[string]Item{
				field: Entry{Val: expectedVal, TTL: time.Minute},
			}, nil
		}

		item, err := s.Get(ctx, key, field, WithFetch(fetchFn))
		if err != nil {
			t.Fatalf("Get with fetch failed: %v", err)
		}
		if !fetchCalled {
			t.Error("FetchFunc was not called")
		}
		if item.GetValue() != expectedVal {
			t.Errorf("Expected %v, got %v", expectedVal, item.GetValue())
		}

		// 3. 再次 Get，应该直接命中缓存，不再调用 fetchFn
		fetchCalled = false
		item, err = s.Get(ctx, key, field, WithFetch(fetchFn))
		if err != nil {
			t.Fatal(err)
		}
		if fetchCalled {
			t.Error("FetchFunc should NOT be called again (cache hit expected)")
		}
		if item.GetValue() != expectedVal {
			t.Errorf("Value mismatch from cache")
		}
	})
}

// TestBatchOps 测试批量操作 PutBatch / GetBatch
func TestBatchOps(t *testing.T) {
	runBackends(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		key := "test:batch"

		// 1. PutBatch
		items := map[string]Item{
			"f1": Entry{Val: "v1", TTL: time.Minute},
			"f2": Entry{Val: "v2", TTL: time.Minute},
		}
		err := s.PutBatch(ctx, key, items)
		if err != nil {
			t.Fatalf("PutBatch failed: %v", err)
		}

		// 2. GetBatch (部分存在，部分不存在)
		fields := []string{"f1", "f2", "f3"}

		// 模拟回源函数
		fetchFn := func(missing []string) (map[string]Item, error) {
			res := make(map[string]Item)
			for _, f := range missing {
				if f == "f3" {
					res[f] = Entry{Val: "v3", TTL: time.Minute}
				}
			}
			return res, nil
		}

		res, err := s.GetBatch(ctx, key, fields, WithFetch(fetchFn))
		if err != nil {
			t.Fatalf("GetBatch failed: %v", err)
		}

		if len(res) != 3 {
			t.Errorf("Expected 3 items, got %d", len(res))
		}
		if res["f1"].GetValue() != "v1" {
			t.Errorf("f1 value mismatch")
		}
		if res["f3"].GetValue() != "v3" {
			t.Errorf("f3 should be fetched")
		}

		// 3. 再次 GetBatch，f3 应该在缓存中
		res2, err := s.GetBatch(ctx, key, []string{"f3"})
		if err != nil {
			t.Fatal(err)
		}
		if val, ok := res2["f3"]; !ok || val.GetValue() != "v3" {
			t.Error("f3 not cached properly")
		}
	})
}

// TestGetAll 测试 GetAll 及其回源逻辑
func TestGetAll(t *testing.T) {
	runBackends(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		key := "test:getall"

		// 1. 初始化数据
		// f1: 有效
		// f2: 马上过期
		// f3: 有效
		s.Put(ctx, key, "f1", Entry{Val: "v1", TTL: time.Minute})
		s.Put(ctx, key, "f2", Entry{Val: "v2", TTL: time.Millisecond}) // 极短 TTL
		s.Put(ctx, key, "f3", Entry{Val: "v3", TTL: time.Minute})

		time.Sleep(10 * time.Millisecond) // 等待 f2 过期

		// 2. 普通 GetAll (不带 Fetch)
		res, err := s.GetAll(ctx, key)
		if err != nil {
			t.Fatalf("GetAll failed: %v", err)
		}
		if len(res) != 2 {
			t.Errorf("Expected 2 valid items (f1, f3), got %d", len(res))
		}
		if _, ok := res["f2"]; ok {
			t.Error("f2 should be expired and not returned")
		}

		// 3. 带 Fetch 的 GetAll
		// 定义回源函数：只针对过期或丢失的字段 f2 进行补救
		fetchCalled := false
		fetchFn := func(missing []string) (map[string]Item, error) {
			fetchCalled = true
			ret := make(map[string]Item)
			for _, f := range missing {
				if f == "f2" {
					ret[f] = Entry{Val: "v2-refreshed", TTL: time.Minute}
				}
			}
			return ret, nil
		}

		res2, err := s.GetAll(ctx, key, WithFetch(fetchFn))
		if err != nil {
			t.Fatalf("GetAll with fetch failed: %v", err)
		}

		if !fetchCalled {
			t.Error("FetchFunc should be called for expired field f2")
		}

		if len(res2) != 3 {
			t.Errorf("Expected 3 items after fetch, got %d", len(res2))
		}
		if val := res2["f2"].GetValue(); val != "v2-refreshed" {
			t.Errorf("f2 value should be refreshed, got %v", val)
		}

		// 4. 再次验证回填是否持久化
		item, err := s.Get(ctx, key, "f2")
		if err != nil {
			t.Errorf("Get f2 failed after fetch: %v", err)
		}
		if item.GetValue() != "v2-refreshed" {
			t.Errorf("Persistence check failed")
		}
	})
}

func TestNilValue(t *testing.T) {
	runBackends(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		key := "test:nil"
		field := "null_val"

		// 使用 Entry 包装 nil
		if err := s.Put(ctx, key, field, Entry{Val: nil}); err != nil {
			t.Fatalf("Put nil failed: %v", err)
		}

		item, err := s.Get(ctx, key, field)
		if err != nil {
			t.Fatalf("Get nil failed: %v", err)
		}
		if item.GetValue() != nil {
			t.Errorf("Expected nil, got %v", item.GetValue())
		}
	})
}

func TestRemoveWithOption(t *testing.T) {
	runBackends(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		key := "test:remove_opt"

		s.Put(ctx, key, "f1", Entry{Val: "val"})

		if err := s.Remove(ctx, key, []string{"f1"}, WithLock(time.Second)); err != nil {
			t.Fatalf("Remove with lock failed: %v", err)
		}

		if has, _ := s.Has(ctx, key, "f1"); has {
			t.Error("Field should be removed")
		}
	})
}

func TestBasicOps(t *testing.T) {
	runBackends(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		key := "test:basic"

		val := "test-string-value"
		// 使用 Entry 包装
		if err := s.Put(ctx, key, "u1", Entry{Val: val}); err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		if has, err := s.Has(ctx, key, "u1"); err != nil || !has {
			t.Errorf("Has failed or returns false")
		}

		item, err := s.Get(ctx, key, "u1")
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if item.GetValue() != val {
			t.Errorf("Get mismatch: want %v, got %v", val, item.GetValue())
		}

		if err := s.Remove(ctx, key, []string{"u1"}); err != nil {
			t.Fatalf("Remove failed: %v", err)
		}
		if has, _ := s.Has(ctx, key, "u1"); has {
			t.Errorf("Remove failed, field still exists")
		}
	})
}

func TestExpiration(t *testing.T) {
	runBackends(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		key := "test:expire"

		token := AuthToken{Token: "abc", ExpiresIn: 1 * time.Second}
		if err := s.Put(ctx, key, "item_ttl", token); err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		if has, _ := s.Has(ctx, key, "item_ttl"); !has {
			t.Error("Item should exist immediately")
		}

		// 检查存活时的 TTL
		aliveItem, err := s.Get(ctx, key, "item_ttl")
		if err != nil {
			t.Fatalf("Failed to get alive item: %v", err)
		}
		if aliveItem.GetTTL() <= 0 {
			t.Errorf("Alive item should have positive TTL, got %v", aliveItem.GetTTL())
		}

		time.Sleep(1500 * time.Millisecond)

		_, err = s.Get(ctx, key, "item_ttl")
		if !errors.Is(err, ErrMiss) {
			t.Errorf("Expected ErrMiss for item_ttl, got %v", err)
		}

		if has, _ := s.Has(ctx, key, "item_ttl"); has {
			t.Error("Has should return false after expiration")
		}
	})
}

func TestPackError(t *testing.T) {
	// 1. Redis: 应该失败 (channel 不能 json)
	t.Run("Redis", func(t *testing.T) {
		s := setupRedis(t)
		if s == nil {
			return
		}
		defer s.Close()
		ctx := context.Background()
		// 使用 Entry 包装 channel
		err := s.Put(ctx, "test:err", "f1", Entry{Val: make(chan int)})
		if err == nil {
			t.Error("Expected error when marshaling channel for Redis, got nil")
		}
	})

	// 2. Mem: 应该成功 (直接存储)
	t.Run("Mem", func(t *testing.T) {
		s := setupMem(t)
		if s == nil {
			return
		}
		defer s.Close()
		ctx := context.Background()
		ch := make(chan int)
		if err := s.Put(ctx, "test:mem:chan", "f1", Entry{Val: ch}); err != nil {
			t.Errorf("Mem put channel failed: %v", err)
		}
		item, err := s.Get(ctx, "test:mem:chan", "f1")
		if err != nil {
			t.Errorf("Mem get channel failed: %v", err)
		}
		if _, ok := item.GetValue().(chan int); !ok {
			t.Error("Retrieved value is not channel")
		}
	})
}

func TestMissOps(t *testing.T) {
	runBackends(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		key := "test:miss"
		_, err := s.Get(ctx, key, "non-exist")
		if !errors.Is(err, ErrMiss) {
			t.Errorf("Expected ErrMiss, got %v", err)
		}
		if has, _ := s.Has(ctx, key, "non-exist"); has {
			t.Error("Has returned true for non-exist")
		}
		if err := s.Remove(ctx, key, []string{"non-exist"}); err != nil {
			t.Error(err)
		}
	})
}

func TestTTLZero(t *testing.T) {
	runBackends(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		key := "test:ttl_zero"
		// 使用 Entry{TTL: 0}
		if err := s.Put(ctx, key, "f1", Entry{Val: "val"}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
		item, err := s.Get(ctx, key, "f1")
		if err != nil {
			t.Error(err)
		}
		if item.GetValue() != "val" {
			t.Error("value mismatch")
		}
	})
}

func TestLockBusy(t *testing.T) {
	ctx := context.Background()
	key := "test:lock:busy"
	field := "f1"
	lockTTL := 10 * time.Second

	t.Run("Redis", func(t *testing.T) {
		s := setupRedis(t)
		if s == nil {
			return
		}
		defer s.Close()
		rdb := redis.NewClient(&redis.Options{Addr: TestConfig.RedisAddr, Password: TestConfig.RedisPass, DB: TestConfig.RedisDB})
		defer rdb.Close()
		rdb.SetNX(ctx, key+":lock", "holder", lockTTL)

		if err := s.Put(ctx, key, field, Entry{Val: "val"}, WithLock(time.Second)); err == nil {
			t.Error("Expected lock busy")
		}
	})

	t.Run("Mem", func(t *testing.T) {
		s := setupMem(t)
		if s == nil {
			return
		}
		defer s.Close()

		go func() {
			// 在 Put 操作中使用 SlowItem 强制持有锁
			_ = s.Put(ctx, key, field, SlowItem{Val: "s", Delay: 1 * time.Second}, WithLock(lockTTL))
		}()
		time.Sleep(50 * time.Millisecond)

		if err := s.Put(ctx, key, field, Entry{Val: "val"}, WithLock(time.Second)); err == nil {
			t.Error("Expected lock busy")
		}
	})
}

func TestRedisCorruptData(t *testing.T) {
	s := setupRedis(t)
	if s == nil {
		return
	}
	defer s.Close()
	ctx := context.Background()
	key := "test:corrupt"

	rdb := redis.NewClient(&redis.Options{Addr: TestConfig.RedisAddr, Password: TestConfig.RedisPass, DB: TestConfig.RedisDB})
	defer rdb.Close()
	rdb.HSet(ctx, key, "f1", "{bad-json")

	if _, err := s.Get(ctx, key, "f1"); err == nil {
		t.Error("Expected error")
	}
}

func TestLockExpirationManual(t *testing.T) {
	ctx := context.Background()
	key := "test:lock:manual_expire"
	field := "f1"
	lockTTL := 200 * time.Millisecond

	t.Run("Mem", func(t *testing.T) {
		s := setupMem(t)
		if s == nil {
			return
		}
		defer s.Close()

		go func() {
			_ = s.Put(ctx, key, "holder", SlowItem{Val: "v", Delay: lockTTL + 100*time.Millisecond}, WithLock(lockTTL))
		}()

		time.Sleep(50 * time.Millisecond)
		if err := s.Put(ctx, key, field, Entry{Val: "val"}, WithLock(time.Second)); err == nil {
			t.Error("Expected busy error")
		}

		time.Sleep(lockTTL + 100*time.Millisecond)
		if err := s.Put(ctx, key, field, Entry{Val: "val"}, WithLock(time.Second)); err != nil {
			t.Errorf("Failed to acquire expired lock: %v", err)
		}
	})
}

func TestLock(t *testing.T) {
	runBackends(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		key := "test:lock"
		if err := s.Put(ctx, key, "f1", Entry{Val: "val"}, WithLock(2*time.Second)); err != nil {
			t.Fatal(err)
		}
		item, err := s.Get(ctx, key, "f1", WithLock(2*time.Second))
		if err != nil {
			t.Fatal(err)
		}
		if item.GetValue() != "val" {
			t.Error("Mismatch")
		}
	})
}

// --- Benchmarks ---

func BenchmarkRedisPut(b *testing.B) {
	s := setupRedis(nil)
	if s == nil {
		b.Skip("Redis not available")
	}
	defer s.Close()
	ctx := context.Background()
	key := "bench:put"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.Put(ctx, key, fmt.Sprintf("f:%d", i), Entry{Val: "val"})
	}
}

func BenchmarkRedisGet(b *testing.B) {
	s := setupRedis(nil)
	if s == nil {
		b.Skip("Redis not available")
	}
	defer s.Close()
	ctx := context.Background()
	key := "bench:get"
	_ = s.Put(ctx, key, "fixed", Entry{Val: "val"})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = s.Get(ctx, key, "fixed")
	}
}

func BenchmarkMemPut(b *testing.B) {
	s := setupMem(nil)
	defer s.Close()
	ctx := context.Background()
	key := "bench:put"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.Put(ctx, key, fmt.Sprintf("f:%d", i), Entry{Val: "val"})
	}
}

func BenchmarkMemGet(b *testing.B) {
	s := setupMem(nil)
	defer s.Close()
	ctx := context.Background()
	key := "bench:get"
	_ = s.Put(ctx, key, "fixed", Entry{Val: "val"})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = s.Get(ctx, key, "fixed")
	}
}
