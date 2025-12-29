package storage

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/go-redis/redis/v8"
)

// redisHash 实现 HashTransactional
type redisHash struct {
	client      *redis.Client
	key         string
	dataFactory StorageDataFactory
}

// NewRedisHash 构造器
func NewRedisHash(client *redis.Client, key string, dataFactory StorageDataFactory) HashTransactional {
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

func (r *redisHash) HMGet(ctx context.Context, fields ...string) ([]StorageData, error) {
	if len(fields) == 0 {
		return []StorageData{}, nil
	}
	vals, err := r.client.HMGet(ctx, r.key, fields...).Result()
	if err != nil {
		return nil, err
	}
	results := make([]StorageData, len(vals))
	for i, v := range vals {
		if v == nil {
			results[i] = nil
			continue
		}

		var b []byte
		switch val := v.(type) {
		case string:
			b = []byte(val)
		case []byte:
			b = val
		default:
			return nil, fmt.Errorf("unexpected data type from redis HMGet: %T", v)
		}

		data := r.dataFactory()
		if err := data.UnmarshalBinary(b); err != nil {
			return nil, err
		}
		results[i] = data
	}
	return results, nil
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
	// [FIX] 不再进行 HGetAll，仅初始化空缓存，支持海量 Hash。
	// 捕获 Context 以便后续 HGet 能够进行 Lazy Loading 的网络请求。
	return &inMemoryHashTx{
		base:      r,
		ctx:       ctx,
		readCache: make(map[string][]byte),
		seen:      make(map[string]struct{}),
		opQueue:   make([]hashOp, 0),
		done:      false,
	}, nil
}

type hashOp struct {
	isSet bool
	field string
	value []byte
}

type inMemoryHashTx struct {
	base *redisHash
	ctx  context.Context // 必须持有 Context 才能在 HGet 中进行 IO

	readCache map[string][]byte   // 缓存从 Redis 读取的数据 (Snapshot)
	seen      map[string]struct{} // 记录哪些字段已被读取 (Read Set)
	loadedAll bool                // 标记是否调用了 HGetAll

	opQueue []hashOp
	done    bool
	mu      sync.Mutex
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
	tx.mu.Lock()
	defer tx.mu.Unlock()

	// 1. 检查写队列（OpQueue）中是否有最新的修改
	// 倒序查找，取最近一次操作
	for i := len(tx.opQueue) - 1; i >= 0; i-- {
		op := tx.opQueue[i]
		if op.field == field {
			if op.isSet {
				return dest.UnmarshalBinary(op.value)
			}
			return errors.New("field not found") // 已被删除
		}
	}

	// 2. 检查读缓存（Read Cache）
	if _, ok := tx.seen[field]; ok {
		val := tx.readCache[field]
		if val == nil {
			return errors.New("field not found")
		}
		return dest.UnmarshalBinary(val)
	}

	// 3. Lazy Loading: 从 Redis 读取
	// 注意：这里可能会破坏“快照隔离”，因为读取的是当前时间点的数据。
	// 但如果不允许 BeginTx 全量加载，这是唯一的办法。
	// 通过在 Commit 时 Watch 这些字段，我们可以保证“可重复读”和一致性。
	val, err := tx.base.client.HGet(tx.ctx, tx.base.key, field).Bytes()

	tx.seen[field] = struct{}{} // 标记为已读

	if errors.Is(err, redis.Nil) {
		tx.readCache[field] = nil // 记录为不存在
		return errors.New("field not found")
	}
	if err != nil {
		return err
	}

	tx.readCache[field] = val // 记录值
	return dest.UnmarshalBinary(val)
}

func (tx *inMemoryHashTx) HGetAll(newDataFn func() StorageData) (map[string]StorageData, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	// 如果用户显式调用 HGetAll，则必须承担全量加载的代价
	if !tx.loadedAll {
		all, err := tx.base.client.HGetAll(tx.ctx, tx.base.key).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			return nil, err
		}
		// 将全量数据合并入缓存，视为已读
		for k, v := range all {
			if _, alreadySeen := tx.seen[k]; !alreadySeen {
				tx.seen[k] = struct{}{}
				tx.readCache[k] = []byte(v)
			}
		}
		tx.loadedAll = true
	}

	// 构造合并视图：Cache + Ops
	merged := make(map[string][]byte)
	for k, v := range tx.readCache {
		if v != nil {
			merged[k] = v
		}
	}

	// 应用写操作
	for _, op := range tx.opQueue {
		if op.isSet {
			merged[op.field] = op.value
		} else {
			delete(merged, op.field)
		}
	}

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

	// [FIX] 冲突检测：仅针对 Read Set (tx.seen) 进行校验
	// 避免全量 HGetAll，支持海量 Hash。
	err := tx.base.client.Watch(ctx, func(txRedis *redis.Tx) error {
		// 1. 如果调用过 HGetAll，必须校验整个 Hash 长度是否变动
		if tx.loadedAll {
			lenRes, err := txRedis.HLen(ctx, tx.base.key).Result()
			if err != nil {
				return err
			}
			// 计算当前快照中存在的 Key 数量
			var snapshotCount int64
			for _, v := range tx.readCache {
				if v != nil {
					snapshotCount++
				}
			}
			// 如果 Redis 中的 Key 数量与快照不一致，说明有新增或删除，冲突
			if lenRes != snapshotCount {
				return ErrTransactionConflict
			}
		}

		// 收集需要校验的字段 (Read Set)
		checkFields := make([]string, 0, len(tx.seen))
		for k := range tx.seen {
			checkFields = append(checkFields, k)
		}

		if len(checkFields) > 0 {
			// 批量获取当前值
			currentVals, err := txRedis.HMGet(ctx, tx.base.key, checkFields...).Result()
			if err != nil {
				return err
			}

			// 对比 Snapshot
			for i, field := range checkFields {
				snapVal := tx.readCache[field] // nil 表示 Snapshot 中不存在
				currVal := currentVals[i]      // nil 表示 Redis 中不存在

				var currBytes []byte
				if currVal != nil {
					// go-redis HMGet 返回的是 interface{} (string or nil)
					if s, ok := currVal.(string); ok {
						currBytes = []byte(s)
					} else {
						return fmt.Errorf("unexpected type %T", currVal)
					}
				}

				// 比较
				// 1. 都不存在 -> OK
				if snapVal == nil && currBytes == nil {
					continue
				}
				// 2. 一个存在一个不存在 -> Conflict
				if (snapVal == nil) != (currBytes == nil) {
					return ErrTransactionConflict
				}
				// 3. 值不一致 -> Conflict
				if string(snapVal) != string(currBytes) {
					return ErrTransactionConflict
				}
			}
		}

		// 2. 执行写入
		pipe := txRedis.TxPipeline()
		for _, op := range tx.opQueue {
			if op.isSet {
				pipe.HSet(ctx, tx.base.key, op.field, op.value)
			} else {
				pipe.HDel(ctx, tx.base.key, op.field)
			}
		}
		_, err := pipe.Exec(ctx)
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
