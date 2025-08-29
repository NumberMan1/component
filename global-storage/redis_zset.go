package storage

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"sync"

	"github.com/go-redis/redis/v8"
)

// redisZSet 实现了 SortedSetTransactional，绑定一个固定 sorted set key。
type redisZSet struct {
	client  redis.UniversalClient
	key     string
	factory SortedSetDataFactory
}

// NewRedisZSet 构造 SortedSetTransactional，传入 factory 用于反序列化时创建实例。
func NewRedisZSet(client redis.UniversalClient, key string, factory SortedSetDataFactory) SortedSetTransactional {
	return &redisZSet{
		client:  client,
		key:     key,
		factory: factory,
	}
}

func (r *redisZSet) ZAdd(ctx context.Context, element SortedSetData) error {
	b, err := element.MarshalBinary()
	if err != nil {
		return err
	}
	return r.client.ZAdd(ctx, r.key, &redis.Z{Score: element.Score(), Member: b}).Err()
}

func (r *redisZSet) ZRem(ctx context.Context, element StorageData) error {
	b, err := element.MarshalBinary()
	if err != nil {
		return err
	}
	return r.client.ZRem(ctx, r.key, b).Err()
}

func (r *redisZSet) ZRange(ctx context.Context, start, stop int64) ([]SortedSetData, error) {
	zs, err := r.client.ZRangeWithScores(ctx, r.key, start, stop).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	var res []SortedSetData
	for _, z := range zs {
		elem := r.factory()
		if err := elem.UnmarshalBinary([]byte(z.Member.(string))); err != nil {
			return nil, err
		}
		elem.SetScore(z.Score)
		res = append(res, elem)
	}
	return res, nil
}

func (r *redisZSet) ZRevRangeByScore(ctx context.Context, max, min float64, offset, count int) ([]SortedSetData, error) {
	opt := &redis.ZRangeBy{
		Min:    strconv.FormatFloat(min, 'f', -1, 64),
		Max:    strconv.FormatFloat(max, 'f', -1, 64),
		Offset: int64(offset),
		Count:  int64(count),
	}
	zs, err := r.client.ZRevRangeByScoreWithScores(ctx, r.key, opt).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	out := make([]SortedSetData, 0, len(zs))
	for _, z := range zs {
		elem := r.factory()
		if err := elem.UnmarshalBinary([]byte(z.Member.(string))); err != nil {
			return nil, err
		}
		elem.SetScore(z.Score)
		out = append(out, elem)
	}
	return out, nil
}

// BeginTx 拉取一次全量 SortedSet 快照，返回事务句柄
func (r *redisZSet) BeginTx(ctx context.Context) (SortedSetTransaction, error) {
	zs, err := r.client.ZRangeWithScores(ctx, r.key, 0, -1).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	snap := make([]SortedSetData, 0, len(zs))
	for _, z := range zs {
		elem := r.factory()
		if err := elem.UnmarshalBinary([]byte(z.Member.(string))); err != nil {
			return nil, err
		}
		elem.SetScore(z.Score)
		snap = append(snap, elem)
	}
	return &inMemoryZSetTx{
		base:     r,
		snapshot: snap,
		ops:      make([]zsetOp, 0),
	}, nil
}

func (r *redisZSet) ZTrimByTopN(ctx context.Context, n int64) error {
	total, err := r.client.ZCard(ctx, r.key).Result()
	if err != nil {
		return err
	}
	if total <= n {
		return nil
	}
	return r.client.ZRemRangeByRank(ctx, r.key, n, -1).Err()
}

func (r *redisZSet) ZRevTrimByTopN(ctx context.Context, n int64) error {
	total, err := r.client.ZCard(ctx, r.key).Result()
	if err != nil {
		return err
	}
	if total <= n {
		return nil
	}
	return r.client.ZRemRangeByRank(ctx, r.key, 0, total-n-1).Err()
}

type zsetOp struct {
	isAdd   bool
	element SortedSetData // 用于新增
	member  []byte        // 用于删除
}

type inMemoryZSetTx struct {
	base     *redisZSet
	snapshot []SortedSetData
	ops      []zsetOp
	done     bool
	mu       sync.RWMutex
}

func (tx *inMemoryZSetTx) ZAdd(element SortedSetData) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	tx.ops = append(tx.ops, zsetOp{isAdd: true, element: element})
	return nil
}

func (tx *inMemoryZSetTx) ZRem(element StorageData) error {
	b, err := element.MarshalBinary()
	if err != nil {
		return err
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	tx.ops = append(tx.ops, zsetOp{isAdd: false, member: b})
	return nil
}

// ZRange 返回按分值升序的 [start, stop] 元素
func (tx *inMemoryZSetTx) ZRange(start, stop int64) ([]SortedSetData, error) {
	merged := tx.applyOps(true)
	sort.Slice(merged, func(i, j int) bool {
		return merged[i].Score() < merged[j].Score()
	})
	return tx.sliceRange(merged, start, stop), nil
}

// ZTrimByTopN 保留 score 升序前 N 条，其余标记删除
func (tx *inMemoryZSetTx) ZTrimByTopN(n int64) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	// 合并快照与操作，无需持有写锁
	merged := tx.applyOps(false)
	// 升序排序
	sort.Slice(merged, func(i, j int) bool {
		return merged[i].Score() < merged[j].Score()
	})
	total := int64(len(merged))
	if total <= n {
		return nil
	}
	// 添加删除操作
	for _, e := range merged[n:] {
		b, err := e.MarshalBinary()
		if err != nil {
			continue
		}
		tx.ops = append(tx.ops, zsetOp{isAdd: false, member: b})
	}
	return nil
}

// ZRevTrimByTopN 保留 score 倒序前 N 条，其余标记删除
func (tx *inMemoryZSetTx) ZRevTrimByTopN(n int64) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	merged := tx.applyOps(false)
	// 倒序排序
	sort.Slice(merged, func(i, j int) bool {
		return merged[i].Score() > merged[j].Score()
	})
	total := int64(len(merged))
	if total <= n {
		return nil
	}
	// 删除第 n 到 end
	for _, e := range merged[n:] {
		b, err := e.MarshalBinary()
		if err != nil {
			continue
		}
		tx.ops = append(tx.ops, zsetOp{isAdd: false, member: b})
	}
	return nil
}

// ZRevRangeByScore 倒序获取数据
func (tx *inMemoryZSetTx) ZRevRangeByScore(max, min float64, offset, count int) ([]SortedSetData, error) {
	merged := tx.applyOps(true)

	// 筛选分值在 [min, max] 范围内
	filtered := make([]SortedSetData, 0, len(merged))
	for _, e := range merged {
		s := e.Score()
		if s <= max && s >= min {
			filtered = append(filtered, e)
		}
	}

	// 倒序排序
	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].Score() > filtered[j].Score()
	})

	// 应用 offset/count
	if offset < 0 {
		offset = 0
	}
	if offset >= len(filtered) {
		return []SortedSetData{}, nil
	}
	end := offset + count
	if end > len(filtered) {
		end = len(filtered)
	}
	return filtered[offset:end], nil
}

// applyOps 合并快照与操作日志（不排序）
// lock: 是否在执行期间加锁
func (tx *inMemoryZSetTx) applyOps(lock bool) []SortedSetData {
	if lock {
		tx.mu.RLock()
		defer tx.mu.RUnlock()
	}

	// 复制初始快照
	cur := make([]SortedSetData, len(tx.snapshot))
	copy(cur, tx.snapshot)

	// 按序应用每条操作
	for _, op := range tx.ops {
		if op.isAdd {
			cur = append(cur, op.element)
		} else {
			filtered := make([]SortedSetData, 0, len(cur))
			for _, e := range cur {
				eb, _ := e.MarshalBinary()
				if string(eb) != string(op.member) {
					filtered = append(filtered, e)
				}
			}
			cur = filtered
		}
	}
	return cur
}

// sliceRange 对已排序的列表执行数组切片
func (tx *inMemoryZSetTx) sliceRange(arr []SortedSetData, start, stop int64) []SortedSetData {
	total := int64(len(arr))
	if start < 0 {
		start = 0
	}
	if stop < 0 || stop >= total {
		stop = total - 1
	}
	if start > stop || start >= total {
		return []SortedSetData{}
	}
	return arr[start : stop+1]
}

// Commit 使用 WATCH/MULTI/EXEC 实现乐观锁，批量提交所有操作。
// 如果在事务开始后，key 被其他客户端修改，此方法将返回 ErrTransactionConflict。
func (tx *inMemoryZSetTx) Commit(ctx context.Context) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done {
		return errors.New("transaction already finished")
	}

	// 使用 client.Watch 来执行一个原子性的 check-and-set 操作
	err := tx.base.client.Watch(ctx, func(txRedis *redis.Tx) error {
		// TxPipelined 会将所有操作包裹在 MULTI 和 EXEC 中
		_, err := txRedis.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			if len(tx.ops) == 0 {
				return nil // 如果没有操作，也需要一个成功的 pipeline
			}
			for _, op := range tx.ops {
				if op.isAdd {
					b, err := op.element.MarshalBinary()
					if err != nil {
						return err // 提前终止 pipeline
					}
					pipe.ZAdd(ctx, tx.base.key, &redis.Z{
						Score:  op.element.Score(),
						Member: b,
					})
				} else {
					pipe.ZRem(ctx, tx.base.key, op.member)
				}
			}
			return nil
		})
		return err
	}, tx.base.key)

	// 检查 Watch 返回的错误
	if err != nil {
		if errors.Is(err, redis.TxFailedErr) {
			return ErrTransactionConflict
		}
		return err
	}

	tx.done = true
	return nil
}

// Rollback 丢弃所有未提交的操作
func (tx *inMemoryZSetTx) Rollback() {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	tx.done = true
}
