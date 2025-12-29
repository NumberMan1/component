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
	client  *redis.Client
	key     string
	factory SortedSetDataFactory
}

// NewRedisZSet 构造 SortedSetTransactional，传入 factory 用于反序列化时创建实例。
func NewRedisZSet(client *redis.Client, key string, factory SortedSetDataFactory) SortedSetTransactional {
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

func (r *redisZSet) ZRangeByScore(ctx context.Context, min, max float64, offset, count int) ([]SortedSetData, error) {
	opt := &redis.ZRangeBy{
		Min:    strconv.FormatFloat(min, 'f', -1, 64),
		Max:    strconv.FormatFloat(max, 'f', -1, 64),
		Offset: int64(offset),
		Count:  int64(count),
	}
	zs, err := r.client.ZRangeByScoreWithScores(ctx, r.key, opt).Result()
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

func (tx *inMemoryZSetTx) ZRange(start, stop int64) ([]SortedSetData, error) {
	merged := tx.applyOps()
	sort.Slice(merged, func(i, j int) bool {
		return merged[i].Score() < merged[j].Score()
	})
	return tx.sliceRange(merged, start, stop), nil
}

func (tx *inMemoryZSetTx) ZTrimByTopN(n int64) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	merged := tx.applyOpsWithoutLock()
	sort.Slice(merged, func(i, j int) bool {
		return merged[i].Score() < merged[j].Score()
	})
	total := int64(len(merged))
	if total <= n {
		return nil
	}
	for _, e := range merged[n:] {
		b, err := e.MarshalBinary()
		if err != nil {
			continue
		}
		tx.ops = append(tx.ops, zsetOp{isAdd: false, member: b})
	}
	return nil
}

func (tx *inMemoryZSetTx) ZRevTrimByTopN(n int64) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	merged := tx.applyOpsWithoutLock()
	sort.Slice(merged, func(i, j int) bool {
		return merged[i].Score() > merged[j].Score()
	})
	total := int64(len(merged))
	if total <= n {
		return nil
	}
	for _, e := range merged[n:] {
		b, err := e.MarshalBinary()
		if err != nil {
			continue
		}
		tx.ops = append(tx.ops, zsetOp{isAdd: false, member: b})
	}
	return nil
}

func (tx *inMemoryZSetTx) ZRangeByScore(min, max float64, offset, count int) ([]SortedSetData, error) {
	merged := tx.applyOps()
	filtered := make([]SortedSetData, 0, len(merged))
	for _, e := range merged {
		s := e.Score()
		if s >= min && s <= max {
			filtered = append(filtered, e)
		}
	}
	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].Score() < filtered[j].Score()
	})
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

func (tx *inMemoryZSetTx) ZRevRangeByScore(max, min float64, offset, count int) ([]SortedSetData, error) {
	merged := tx.applyOps()
	filtered := make([]SortedSetData, 0, len(merged))
	for _, e := range merged {
		s := e.Score()
		if s <= max && s >= min {
			filtered = append(filtered, e)
		}
	}
	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].Score() > filtered[j].Score()
	})
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

func (tx *inMemoryZSetTx) applyOps() []SortedSetData {
	tx.mu.RLock()
	defer tx.mu.RUnlock()
	return tx.applyOpsWithoutLock()
}

func (tx *inMemoryZSetTx) applyOpsWithoutLock() []SortedSetData {
	cur := make([]SortedSetData, len(tx.snapshot))
	copy(cur, tx.snapshot)
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

func (tx *inMemoryZSetTx) Commit(ctx context.Context) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done {
		return errors.New("transaction already finished")
	}

	pipe := tx.base.client.TxPipeline()
	for _, op := range tx.ops {
		if op.isAdd {
			b, _ := op.element.MarshalBinary()
			pipe.ZAdd(ctx, tx.base.key, &redis.Z{
				Score:  op.element.Score(),
				Member: b,
			})
		} else {
			pipe.ZRem(ctx, tx.base.key, op.member)
		}
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}

	tx.done = true
	return nil
}

func (tx *inMemoryZSetTx) Rollback() {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	tx.done = true
}
