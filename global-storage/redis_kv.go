package storage

import (
	"context"
	"errors"
	"sync"

	"github.com/go-redis/redis/v8"
)

// redisKV 实现了 KVTransactional，绑定一个固定 key。
type redisKV struct {
	// 使用通用接口 redis.Cmdable
	client redis.Cmdable
	key    string
}

// NewRedisKV 构造函数现在接收 redis.Cmdable 接口
func NewRedisKV(client redis.Cmdable, key string) KVTransactional {
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
	// 如果 key 不存在，snapshot 为 nil，这是正常情况
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
	snapshot []byte // 事务开始时的快照
	write    []byte // 事务期间的写操作
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
		return ErrFieldNotFound
	}
	return dest.UnmarshalBinary(data)
}

// Commit 使用 Lua 脚本实现原子性的 CAS (Compare-and-Set) 操作
func (tx *inMemoryKVTx) Commit(ctx context.Context) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done {
		return errors.New("transaction already finished")
	}
	tx.done = true

	if !tx.written {
		return nil // 如果没有写操作，则无需提交
	}

	// Lua 脚本：
	// KEYS[1]: a chave a ser modificada (key)
	// ARGV[1]: o valor original esperado (snapshot)
	// ARGV[2]: o novo valor a ser definido (new value)
	//
	// O script verifica se o valor atual da chave é igual ao snapshot.
	// Se for, atualiza para o novo valor e retorna 1.
	// Se não for, não faz nada e retorna 0 (conflito).
	// Se a chave não existir e o snapshot for 'false' (string), ele define o novo valor.
	const script = `
        local current = redis.call('get', KEYS[1])
        local snapshot = ARGV[1]
        
        if current == snapshot or (current == false and snapshot == '__NIL__') then
            redis.call('set', KEYS[1], ARGV[2])
            return 1
        else
            return 0
        end
    `

	// 如果快照为空(key不存在), 使用一个特殊值来表示
	snapshotArg := tx.snapshot
	if snapshotArg == nil {
		snapshotArg = []byte("__NIL__")
	}

	res, err := tx.base.client.Eval(ctx, script, []string{tx.base.key}, snapshotArg, tx.write).Result()
	if err != nil {
		return err
	}

	// 检查 Lua 脚本的返回值
	if val, ok := res.(int64); ok && val == 1 {
		return nil // 成功
	}

	return ErrTransactionConflict
}

func (tx *inMemoryKVTx) Rollback() {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	tx.done = true
}
