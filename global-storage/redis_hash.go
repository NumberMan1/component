package storage

import (
	"context"
	"errors"
	"reflect"
	"sync"

	"github.com/go-redis/redis/v8"
)

// redisHash 实现 HashTransactional
type redisHash struct {
	client      redis.UniversalClient
	key         string
	dataFactory StorageDataFactory
}

// NewRedisHash 构造器
func NewRedisHash(client redis.UniversalClient, key string, dataFactory StorageDataFactory) HashTransactional {
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

func (tx *inMemoryHashTx) HGet(field string, dest StorageData) error {
	// 合并 snapshot 与 opQueue
	data, found := tx.snapshot[field]
	for _, op := range tx.opQueue {
		if op.field == field {
			if op.isSet {
				data = op.value
				found = true
			} else {
				found = false
			}
		}
	}
	if !found {
		return errors.New("field not found")
	}
	return dest.UnmarshalBinary(data)
}

func (tx *inMemoryHashTx) HGetAll(newDataFn func() StorageData) (map[string]StorageData, error) {
	// 合并 snapshot 与 opQueue，顺序应用
	merged := make(map[string][]byte, len(tx.snapshot))
	for f, v := range tx.snapshot {
		merged[f] = append([]byte(nil), v...)
	}
	for _, op := range tx.opQueue {
		if op.isSet {
			merged[op.field] = append([]byte(nil), op.value...)
		} else {
			delete(merged, op.field)
		}
	}
	// 构造输出
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

	if len(tx.opQueue) == 0 {
		return nil // 如果没有操作，则无需提交
	}

	err := tx.base.client.Watch(ctx, func(txRedis *redis.Tx) error {
		// --- 检查阶段 ---
		currentMap, err := txRedis.HGetAll(ctx, tx.base.key).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			return err
		}

		// 将快照转换为 map[string]string 以便比较
		snapshotMapStr := make(map[string]string, len(tx.snapshot))
		for k, v := range tx.snapshot {
			snapshotMapStr[k] = string(v)
		}

		// 使用 reflect.DeepEqual 比较两个 map
		if !reflect.DeepEqual(currentMap, snapshotMapStr) {
			// Hash 的状态已被修改，中止事务
			return redis.TxFailedErr
		}
		// --- 检查结束 ---

		// --- 设置阶段 ---
		_, err = txRedis.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			for _, op := range tx.opQueue {
				if op.isSet {
					pipe.HSet(ctx, tx.base.key, op.field, op.value)
				} else {
					pipe.HDel(ctx, tx.base.key, op.field)
				}
			}
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

func (tx *inMemoryHashTx) Rollback() {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	tx.done = true
}
