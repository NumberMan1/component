package storage

import (
	"context"
	"errors"
	"sync"

	"github.com/go-redis/redis/v8"
)

// redisKV 实现了 KVTransactional，绑定一个固定 key。
type redisKV struct {
	client *redis.Client
	key    string
}

// NewRedisKV 根据传入的 Redis 客户端和 key 返回存储实例。
func NewRedisKV(client *redis.Client, key string) KVTransactional {
	return &redisKV{
		client: client,
		key:    key,
	}
}

func (r *redisKV) Set(ctx context.Context, value StorageData) error {
	b, err := value.MarshalBinary()
	if err != nil {
		return err
	}
	return r.client.Set(ctx, r.key, b, 0).Err()
}

func (r *redisKV) Get(ctx context.Context, dest StorageData) error {
	b, err := r.client.Get(ctx, r.key).Bytes()
	if errors.Is(err, redis.Nil) {
		return ErrFieldNotFound
	}
	if err != nil {
		return err
	}
	return dest.UnmarshalBinary(b)
}

func (r *redisKV) BeginTx(ctx context.Context) (KVTransaction, error) {
	b, err := r.client.Get(ctx, r.key).Bytes()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	// b 为 nil 表示 key 不存在，snapshot 存为 nil
	return &inMemoryKVTx{
		base:     r,
		snapshot: b,
		done:     false,
		mu:       sync.RWMutex{},
	}, nil
}

type inMemoryKVTx struct {
	base     *redisKV
	snapshot []byte
	write    []byte
	written  bool
	done     bool
	mu       sync.RWMutex
}

func (tx *inMemoryKVTx) Set(value StorageData) error {
	b, err := value.MarshalBinary()
	if err != nil {
		return err
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	tx.write = b
	tx.written = true
	return nil
}

func (tx *inMemoryKVTx) Get(dest StorageData) error {
	tx.mu.RLock()
	data := tx.snapshot
	if tx.written {
		data = tx.write
	}
	tx.mu.RUnlock()
	if data == nil {
		return errors.New("key not found")
	}
	return dest.UnmarshalBinary(data)
}

func (tx *inMemoryKVTx) Commit(ctx context.Context) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done {
		return errors.New("transaction already finished")
	}

	if !tx.written {
		return nil // 如果没有写操作，则无需提交
	}

	// [FIX] 使用 Watch 实现乐观锁，必须比较当前 Redis 中的值与 BeginTx 时的 snapshot 是否一致
	err := tx.base.client.Watch(ctx, func(txRedis *redis.Tx) error {
		// 1. 获取当前值 (CAS 检查)
		currentVal, err := txRedis.Get(ctx, tx.base.key).Bytes()
		if err != nil && !errors.Is(err, redis.Nil) {
			return err
		}

		// 比较逻辑：
		// 1. Snapshot 是 nil (BeginTx 时不存在)，Current 是有值 -> 冲突
		// 2. Snapshot 是有值，Current 是 nil (期间被删) -> 冲突
		// 3. 都有值，但内容不同 -> 冲突
		snapshotIsNil := len(tx.snapshot) == 0
		currentIsNil := errors.Is(err, redis.Nil)

		if snapshotIsNil != currentIsNil {
			return ErrTransactionConflict
		}
		if !snapshotIsNil && string(currentVal) != string(tx.snapshot) {
			return ErrTransactionConflict
		}

		// 2. 值未变，执行事务写入
		_, err = txRedis.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, tx.base.key, tx.write, 0)
			return nil
		})
		return err
	}, tx.base.key)

	if err != nil {
		// Watch 失败（监控期间变化）或者我们手动返回的冲突错误
		if errors.Is(err, redis.TxFailedErr) {
			return ErrTransactionConflict
		}
		return err
	}

	tx.done = true
	return nil
}

func (tx *inMemoryKVTx) Rollback() {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	tx.done = true
}
