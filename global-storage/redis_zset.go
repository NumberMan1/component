package storage

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/go-redis/redis/v8"
)

// redisZSet 实现了 SortedSetTransactional，绑定一个固定 sorted set key。
type redisZSet struct {
	client  redis.Cmdable
	key     string
	factory SortedSetDataFactory
}

// NewRedisZSet 构造 SortedSetTransactional，传入 factory 用于反序列化时创建实例。
func NewRedisZSet(client redis.Cmdable, key string, factory SortedSetDataFactory) SortedSetTransactional {
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

// **FIXED**: 实现了 ZRange 方法
// ZRange 返回按分值升序的 [start, stop] 元素
func (tx *inMemoryZSetTx) ZRange(start, stop int64) ([]SortedSetData, error) {
	merged := tx.applyOps(true)
	sort.Slice(merged, func(i, j int) bool {
		return merged[i].Score() < merged[j].Score()
	})
	return tx.sliceRange(merged, start, stop), nil
}

// **FIXED**: 实现了 ZTrimByTopN 方法
// ZTrimByTopN 保留 score 升序前 N 条，其余标记删除
func (tx *inMemoryZSetTx) ZTrimByTopN(n int64) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	merged := tx.applyOps(false) // 在锁内应用操作，所以 applyOps 不需要再加锁
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
			// 在事务中，如果序列化失败，最好返回错误
			return err
		}
		tx.ops = append(tx.ops, zsetOp{isAdd: false, member: b})
	}
	return nil
}

// **FIXED**: 实现了 ZRevTrimByTopN 方法
// ZRevTrimByTopN 保留 score 倒序前 N 条，其余标记删除
func (tx *inMemoryZSetTx) ZRevTrimByTopN(n int64) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	merged := tx.applyOps(false) // 在锁内应用操作
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
			return err
		}
		tx.ops = append(tx.ops, zsetOp{isAdd: false, member: b})
	}
	return nil
}

// **FIXED**: 实现了 ZRevRangeByScore 方法
// ZRevRangeByScore 倒序获取数据
func (tx *inMemoryZSetTx) ZRevRangeByScore(max, min float64, offset, count int) ([]SortedSetData, error) {
	merged := tx.applyOps(true)

	// 筛选分值在 [min, max] 范围内
	filtered := make([]SortedSetData, 0)
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
	if count < 0 {
		count = len(filtered)
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

	// 使用 map 来处理成员的唯一性，同时保留最新的分数
	memberMap := make(map[string]SortedSetData)
	for _, item := range tx.snapshot {
		key, err := item.MarshalBinary()
		if err == nil { // 忽略序列化错误的旧数据
			memberMap[string(key)] = item
		}
	}

	// 按序应用每条操作
	for _, op := range tx.ops {
		if op.isAdd {
			key, err := op.element.MarshalBinary()
			if err == nil {
				memberMap[string(key)] = op.element
			}
		} else {
			delete(memberMap, string(op.member))
		}
	}

	// 将 map 转换回 slice
	result := make([]SortedSetData, 0, len(memberMap))
	for _, item := range memberMap {
		result = append(result, item)
	}
	return result
}

// sliceRange 对已排序的列表执行数组切片
func (tx *inMemoryZSetTx) sliceRange(arr []SortedSetData, start, stop int64) []SortedSetData {
	total := int64(len(arr))
	if start < 0 {
		start += total
	}
	if stop < 0 {
		stop += total
	}
	if start < 0 {
		start = 0
	}
	if stop >= total {
		stop = total - 1
	}
	if start > stop {
		return []SortedSetData{}
	}
	return arr[start : stop+1]
}

// Commit 使用绝对索引重写 Commit 方法中的 Lua 脚本生成
func (tx *inMemoryZSetTx) Commit(ctx context.Context) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done {
		return errors.New("transaction already finished")
	}
	tx.done = true

	if len(tx.ops) == 0 {
		return nil
	}

	var scriptBuilder strings.Builder
	args := make([]interface{}, 0, len(tx.ops)*2)
	argIndex := 1 // Lua ARGV is 1-based

	for _, op := range tx.ops {
		if op.isAdd {
			b, err := op.element.MarshalBinary()
			if err != nil {
				return err // 序列化失败，事务中断
			}
			// 生成: redis.call('zadd', KEYS[1], ARGV[1], ARGV[2]);
			scriptBuilder.WriteString("redis.call('zadd', KEYS[1], ARGV[" + strconv.Itoa(argIndex) + "], ARGV[" + strconv.Itoa(argIndex+1) + "]); ")
			args = append(args, op.element.Score(), b)
			argIndex += 2
		} else {
			// 生成: redis.call('zrem', KEYS[1], ARGV[3]);
			scriptBuilder.WriteString("redis.call('zrem', KEYS[1], ARGV[" + strconv.Itoa(argIndex) + "]); ")
			args = append(args, op.member)
			argIndex += 1
		}
	}

	err := tx.base.client.Eval(ctx, scriptBuilder.String(), []string{tx.base.key}, args...).Err()
	if err != nil && !errors.Is(err, redis.Nil) {
		return err
	}
	return nil
}

// Rollback 丢弃所有未提交的操作
func (tx *inMemoryZSetTx) Rollback() {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	tx.done = true
}
