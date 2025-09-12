package storage

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"

	"github.com/go-redis/redis/v8"
)

type redisHash struct {
	client      redis.Cmdable
	key         string
	dataFactory StorageDataFactory
}

func NewRedisHash(client redis.Cmdable, key string, dataFactory StorageDataFactory) HashTransactional {
	return &redisHash{client: client, key: key, dataFactory: dataFactory}
}

func (r *redisHash) HSet(ctx context.Context, field string, value StorageData) error {
	b, err := value.MarshalBinary()
	if err != nil {
		return err
	}
	return r.client.HSet(ctx, r.key, field, b).Err()
}

func (r *redisHash) HGet(ctx context.Context, field string) (StorageData, error) {
	b, err := r.client.HGet(ctx, r.key, field).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, ErrFieldNotFound
	}
	if err != nil {
		return nil, err
	}
	storageData := r.dataFactory()
	err = storageData.UnmarshalBinary(b)
	if err != nil {
		return nil, err
	}
	return storageData, nil
}

func (r *redisHash) HGetAll(ctx context.Context) (map[string]StorageData, error) {
	all, err := r.client.HGetAll(ctx, r.key).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	res := make(map[string]StorageData, len(all))
	for f, v := range all {
		data := r.dataFactory()
		if err := data.UnmarshalBinary([]byte(v)); err != nil {
			return nil, err
		}
		res[f] = data
	}
	return res, nil
}

func (r *redisHash) HDel(ctx context.Context, fields ...string) error {
	err := r.client.HDel(ctx, r.key, fields...).Err()
	if errors.Is(err, redis.Nil) {
		return nil
	}
	return err
}

func (r *redisHash) BeginTx(ctx context.Context) (HashTransaction, error) {
	all, err := r.client.HGetAll(ctx, r.key).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	snap := make(map[string][]byte, len(all))
	for f, v := range all {
		snap[f] = []byte(v)
	}
	return newInMemoryHashTx(r, snap), nil
}

type hashOp struct {
	isSet bool
	field string
	value []byte
}

type inMemoryHashTx struct {
	base     *redisHash
	snapshot map[string][]byte
	opQueue  []hashOp
	done     bool
	mu       sync.Mutex
}

func newInMemoryHashTx(base *redisHash, snap map[string][]byte) *inMemoryHashTx {
	copySnap := make(map[string][]byte, len(snap))
	for k, v := range snap {
		copySnap[k] = append([]byte(nil), v...)
	}
	return &inMemoryHashTx{
		base:     base,
		snapshot: copySnap,
		opQueue:  make([]hashOp, 0),
		done:     false,
	}
}

func (tx *inMemoryHashTx) HSet(field string, value StorageData) error {
	b, err := value.MarshalBinary()
	if err != nil {
		return err
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	tx.opQueue = append(tx.opQueue, hashOp{isSet: true, field: field, value: b})
	return nil
}

// **FIXED**: 实现了 HGet 的事务版本
func (tx *inMemoryHashTx) HGet(field string, dest StorageData) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	// 从后往前遍历操作队列，找到对该 field 的最后一次操作
	for i := len(tx.opQueue) - 1; i >= 0; i-- {
		op := tx.opQueue[i]
		if op.field == field {
			if op.isSet {
				// 这是一个写操作，使用它的值
				return dest.UnmarshalBinary(op.value)
			}
			// 这是一个删除操作
			return ErrFieldNotFound
		}
	}

	// 如果操作队列中没有，则从快照中查找
	if data, found := tx.snapshot[field]; found {
		return dest.UnmarshalBinary(data)
	}

	return ErrFieldNotFound
}

// **FIXED**: 实现了 HGetAll 的事务版本
func (tx *inMemoryHashTx) HGetAll(newDataFn func() StorageData) (map[string]StorageData, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	// 先复制一份快照
	merged := make(map[string][]byte, len(tx.snapshot))
	for f, v := range tx.snapshot {
		merged[f] = v
	}

	// 按顺序应用所有操作
	for _, op := range tx.opQueue {
		if op.isSet {
			merged[op.field] = op.value
		} else {
			delete(merged, op.field)
		}
	}

	// 反序列化结果
	res := make(map[string]StorageData, len(merged))
	for f, v := range merged {
		data := newDataFn()
		if err := data.UnmarshalBinary(v); err != nil {
			return nil, err
		}
		res[f] = data
	}
	return res, nil
}

func (tx *inMemoryHashTx) HDel(fields ...string) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	for _, f := range fields {
		tx.opQueue = append(tx.opQueue, hashOp{isSet: false, field: f})
	}
	return nil
}

func (tx *inMemoryHashTx) Commit(ctx context.Context) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done {
		return errors.New("transaction already finished")
	}
	tx.done = true

	if len(tx.opQueue) == 0 {
		return nil
	}

	var scriptBuilder strings.Builder
	args := make([]interface{}, 0, len(tx.opQueue)*2)
	argIndex := 1 // Lua ARGV is 1-based

	// 为每个操作生成独立的、带绝对索引的 redis.call
	for _, op := range tx.opQueue {
		if op.isSet {
			// 生成: redis.call('hset', KEYS[1], ARGV[1], ARGV[2]);
			scriptBuilder.WriteString("redis.call('hset', KEYS[1], ARGV[" + strconv.Itoa(argIndex) + "], ARGV[" + strconv.Itoa(argIndex+1) + "]); ")
			args = append(args, op.field, op.value)
			argIndex += 2
		} else {
			// 生成: redis.call('hdel', KEYS[1], ARGV[3]);
			scriptBuilder.WriteString("redis.call('hdel', KEYS[1], ARGV[" + strconv.Itoa(argIndex) + "]); ")
			args = append(args, op.field)
			argIndex += 1
		}
	}

	if len(args) == 0 {
		return nil
	}

	err := tx.base.client.Eval(ctx, scriptBuilder.String(), []string{tx.base.key}, args...).Err()
	if err != nil && !errors.Is(err, redis.Nil) {
		return err
	}
	return nil
}

func (tx *inMemoryHashTx) Rollback() {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	tx.done = true
}
