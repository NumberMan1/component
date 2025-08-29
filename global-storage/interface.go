package storage

import (
	"context"
	"encoding"
)

type MemoryStorageData interface {
	SetValue(MemoryStorageData)
	Copy() MemoryStorageData
}

type MemoryTransactional interface {
	HSet(ctx context.Context, field string, value MemoryStorageData) error
	HGet(ctx context.Context, field string, dest MemoryStorageData) error
	BeginTx() (MemoryTransaction, error)
}

type MemoryTransaction interface {
	HSet(field string, value MemoryStorageData) error
	HGet(field string, dest MemoryStorageData) error
	Commit()
	Rollback()
}

// StorageDataFactory 创建新的 StorageData 实例。
type StorageDataFactory func() StorageData

// StorageData 是可序列化的数据接口，KV/Hash 使用。
type StorageData interface {
	encoding.BinaryMarshaler   // MarshalBinary() ([]byte, error)
	encoding.BinaryUnmarshaler // UnmarshalBinary(data []byte) error
}

// SortedSetDataFactory 创建新的 SortedSetData 实例。
type SortedSetDataFactory func() SortedSetData

// SortedSetData 除了二进制序列化能力外，还需提供自己的分值。
type SortedSetData interface {
	StorageData
	Score() float64
	SetScore(float64)
}

// KVTransactional 绑定单一 key 的 KV 操作。
type KVTransactional interface {
	Set(ctx context.Context, value StorageData) error
	Get(ctx context.Context, dest StorageData) error
	BeginTx(ctx context.Context) (KVTransaction, error)
}

// KVTransaction 定义 KV 事务快照操作。
type KVTransaction interface {
	Set(value StorageData) error
	Get(dest StorageData) error
	Commit(ctx context.Context) error
	Rollback()
}

// HashTransactional 绑定单一 hash key 的 Hash 操作。
type HashTransactional interface {
	HGetAll(ctx context.Context) (map[string]StorageData, error)
	HSet(ctx context.Context, field string, value StorageData) error
	HGet(ctx context.Context, field string) (StorageData, error)
	HDel(ctx context.Context, fields ...string) error
	BeginTx(ctx context.Context) (HashTransaction, error)
}

// HashTransaction 定义 Hash 事务快照操作。
type HashTransaction interface {
	HGetAll(newDataFn func() StorageData) (map[string]StorageData, error)
	HSet(field string, value StorageData) error
	HGet(field string, dest StorageData) error
	HDel(fields ...string) error
	Commit(ctx context.Context) error
	Rollback()
}

// SortedSetTransactional 绑定单一 sorted-set key 的有序集合操作。
type SortedSetTransactional interface {
	ZAdd(ctx context.Context, element SortedSetData) error
	ZRem(ctx context.Context, element StorageData) error
	ZRange(ctx context.Context, start, stop int64) ([]SortedSetData, error)
	ZRevRangeByScore(ctx context.Context, max, min float64, offset, count int) ([]SortedSetData, error)
	ZRevTrimByTopN(ctx context.Context, n int64) error
	ZTrimByTopN(ctx context.Context, n int64) error
	BeginTx(ctx context.Context) (SortedSetTransaction, error)
}

// SortedSetTransaction 定义有序集合事务快照操作。
// 若你也需要在事务层面支持 RevRangeByScore，可在这里同样添加方法签名。
type SortedSetTransaction interface {
	ZAdd(element SortedSetData) error
	ZRem(element StorageData) error
	ZRange(start, stop int64) ([]SortedSetData, error)
	ZRevRangeByScore(max, min float64, offset, count int) ([]SortedSetData, error)
	ZRevTrimByTopN(n int64) error
	ZTrimByTopN(n int64) error
	Commit(ctx context.Context) error
	Rollback()
}
