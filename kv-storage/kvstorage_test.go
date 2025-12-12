package kvstorage

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
)

// TestConfig Redis 测试配置
var TestConfig = RedisConfig{
	Addr:         "127.0.0.1:6379",
	Password:     "123456",
	DB:           2,
	PoolSize:     50,
	MinIdleConns: 10,
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
	s, err := New(TestConfig)
	if err != nil {
		if t != nil {
			t.Skipf("Skipping Redis test: cannot connect to redis: %v", err)
		}
		return nil
	}
	return s
}

func setupMem(t *testing.T) Store {
	s, err := NewMem(MemConfig{})
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

	future := time.Now().Add(time.Hour).UnixNano()
	e2 := Entry{ExpiresAt: future}
	ttl := e2.GetTTL()
	if ttl <= 0 || ttl > time.Hour {
		t.Errorf("GetTTL failed for future ExpiresAt, got %v", ttl)
	}

	past := time.Now().Add(-time.Hour).UnixNano()
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
	_, err := New(RedisConfig{})
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
		fetchFn := func() (Item, error) {
			fetchCalled = true
			return Entry{Val: expectedVal, TTL: time.Minute}, nil
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
		rdb := redis.NewClient(&redis.Options{Addr: TestConfig.Addr, Password: TestConfig.Password, DB: TestConfig.DB})
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

	rdb := redis.NewClient(&redis.Options{Addr: TestConfig.Addr, Password: TestConfig.Password, DB: TestConfig.DB})
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
