package storage

import (
	"bytes"
	"context"
	"errors"
	"sync"

	"github.com/go-redis/redis/v8"
)

// redisKV 实现了 KVTransactional，绑定一个固定 key。
type redisKV struct {
	client redis.UniversalClient
	key    string
}

// NewRedisKV 根据传入的 Redis 客户端和 key 返回存储实例。
func NewRedisKV(client redis.UniversalClient, key string) KVTransactional {
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

	err := tx.base.client.Watch(ctx, func(txRedis *redis.Tx) error {
		// --- 检查阶段 ---
		currentVal, err := txRedis.Get(ctx, tx.base.key).Bytes()
		if err != nil && !errors.Is(err, redis.Nil) {
			return err // 如果是连接错误等，直接返回
		}

		// 比较 Redis 当前值和我们事务开始时的快照
		// 如果 key 在 Redis 中不存在 (err == redis.Nil)，currentVal 会是空 []byte
		// 如果我们的快照是 nil (表示事务开始时 key 也不存在)，bytes.Equal 依然能正确工作
		if !bytes.Equal(currentVal, tx.snapshot) {
			// 值已经被修改，我们必须中止事务
			return redis.TxFailedErr
		}
		// --- 检查结束 ---

		// --- 设置阶段 ---
		// 在事务中，原子性地执行 SET
		_, err = txRedis.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, tx.base.key, tx.write, 0)
			return nil
		})
		return err
	}, tx.base.key)

	if err != nil {
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
