package kvstorage

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/go-redis/redis/v8"
)

// RedisConfig Redis 特有的配置参数
type RedisConfig struct {
	RedisAddr string `json:"redis_addr" yaml:"redis-addr"`
	RedisPass string `json:"redis_pass" yaml:"redis-pass"`
	RedisDB   int    `json:"redis_db" yaml:"redis-db"`
}

type redisStore struct {
	client *redis.Client
}

func NewRedisStore(cfg RedisConfig) (Store, error) {
	if cfg.RedisAddr == "" {
		return nil, ErrConfig
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPass,
		DB:       cfg.RedisDB,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, err
	}

	return &redisStore{client: rdb}, nil
}

// ---------------------------------------------------------
// 内部辅助方法
// ---------------------------------------------------------

func (r *redisStore) pack(value any, ttl time.Duration) ([]byte, error) {
	var expireAt int64
	if ttl > 0 {
		// 改为毫秒
		expireAt = time.Now().Add(ttl).UnixMilli()
	}

	item := Entry{
		Val:       value,
		ExpiresAt: expireAt,
	}

	return json.Marshal(item)
}

func (r *redisStore) unpack(data []byte) (Item, error) {
	var item Entry
	if err := json.Unmarshal(data, &item); err != nil {
		return nil, err
	}
	// 检查过期 (毫秒)
	if item.ExpiresAt > 0 && time.Now().UnixMilli() > item.ExpiresAt {
		return nil, ErrMiss
	}
	return item, nil
}

func (r *redisStore) withLockWrapper(ctx context.Context, key string, lockTTL time.Duration, op func() error) error {
	if lockTTL <= 0 {
		return op()
	}

	lockKey := key + ":lock"
	token := time.Now().String()
	acquired, err := r.client.SetNX(ctx, lockKey, token, lockTTL).Result()
	if err != nil {
		return err
	}
	if !acquired {
		return errors.New("kvstorage: lock busy")
	}

	defer func() {
		val, err := r.client.Get(ctx, lockKey).Result()
		if err == nil && val == token {
			r.client.Del(ctx, lockKey)
		}
	}()

	return op()
}

// ---------------------------------------------------------
// 字段 (Field) 操作实现
// ---------------------------------------------------------

func (r *redisStore) Put(ctx context.Context, key, field string, item Item, opts ...Option) error {
	// Put 本质上是单字段的 PutBatch
	return r.PutBatch(ctx, key, map[string]Item{field: item}, opts...)
}

func (r *redisStore) PutBatch(ctx context.Context, key string, items map[string]Item, opts ...Option) error {
	o := &options{}
	for _, opt := range opts {
		opt(o)
	}

	return r.withLockWrapper(ctx, key, o.lockTTL, func() error {
		// 构建 HSet 参数: key, f1, v1, f2, v2...
		values := make([]interface{}, 0, len(items)*2)
		for field, item := range items {
			bytes, err := r.pack(item.GetValue(), item.GetTTL())
			if err != nil {
				return err
			}
			values = append(values, field, bytes)
		}
		if len(values) == 0 {
			return nil
		}
		return r.client.HSet(ctx, key, values...).Err()
	})
}

func (r *redisStore) Get(ctx context.Context, key, field string, opts ...Option) (Item, error) {
	// 复用 GetBatch 逻辑
	resMap, err := r.GetBatch(ctx, key, []string{field}, opts...)
	if err != nil {
		return nil, err
	}
	if item, ok := resMap[field]; ok {
		return item, nil
	}
	return nil, ErrMiss
}

func (r *redisStore) GetBatch(ctx context.Context, key string, fields []string, opts ...Option) (map[string]Item, error) {
	o := &options{}
	for _, opt := range opts {
		opt(o)
	}

	result := make(map[string]Item)
	var missingFields []string

	err := r.withLockWrapper(ctx, key, o.lockTTL, func() error {
		// 1. 批量读取
		if len(fields) == 0 {
			return nil
		}
		vals, err := r.client.HMGet(ctx, key, fields...).Result()
		if err != nil {
			return err
		}

		// 2. 解析与过期检查
		for i, v := range vals {
			field := fields[i]
			if v == nil {
				missingFields = append(missingFields, field)
				continue
			}

			// HMGet 返回的是 string 或 nil (如果 v != nil)
			if s, ok := v.(string); ok {
				item, err := r.unpack([]byte(s))
				if err == nil {
					result[field] = item
				} else {
					// 解析失败或已过期 (unpack 返回 ErrMiss)
					missingFields = append(missingFields, field)
				}
			} else {
				missingFields = append(missingFields, field)
			}
		}

		// 3. 回源处理
		if len(missingFields) > 0 && o.fetchFunc != nil {
			fetchedItems, err := o.fetchFunc(missingFields)
			if err != nil {
				return err
			}

			// 如果有获取到数据，回填并合并
			if len(fetchedItems) > 0 {
				// 注意：这里调用 PutBatch，它是原子操作吗？
				// 如果外层有锁 (lockTTL > 0)，则当前还持有锁，递归调用 PutBatch 需要小心。
				// 此时 PutBatch 会再次尝试 SetNX。
				// 问题：SetNX 不可重入！
				// 解决：我们需要一个内部无锁的 putBatchInternal。

				// 构建回填数据参数
				values := make([]interface{}, 0, len(fetchedItems)*2)
				for f, item := range fetchedItems {
					bytes, err := r.pack(item.GetValue(), item.GetTTL())
					if err != nil {
						return err
					}
					values = append(values, f, bytes)

					// 合并到结果集
					result[f] = item
				}

				// 执行回填 (直接调用 redis client，绕过锁检查，因为我们已经在锁里了)
				if len(values) > 0 {
					if err := r.client.HSet(ctx, key, values...).Err(); err != nil {
						return err
					}
				}
			}
		}

		return nil
	})

	return result, err
}

func (r *redisStore) GetAll(ctx context.Context, key string, opts ...Option) (map[string]Item, error) {
	o := &options{}
	for _, opt := range opts {
		opt(o)
	}

	result := make(map[string]Item)
	var missingFields []string

	err := r.withLockWrapper(ctx, key, o.lockTTL, func() error {
		// 1. 获取所有字段 (HGetAll)
		vals, err := r.client.HGetAll(ctx, key).Result()
		if err != nil {
			return err
		}

		// 2. 解析与过期检查
		for field, rawVal := range vals {
			item, err := r.unpack([]byte(rawVal))
			if err == nil {
				// 有效
				result[field] = item
			} else {
				// 解析失败或已过期 (unpack 返回 ErrMiss)
				// 记录下来以便回源
				missingFields = append(missingFields, field)
			}
		}

		// 3. 回源处理 (针对已存在但过期的字段)
		if len(missingFields) > 0 && o.fetchFunc != nil {
			fetchedItems, err := o.fetchFunc(missingFields)
			if err != nil {
				return err
			}

			// 如果有获取到数据，回填并合并
			if len(fetchedItems) > 0 {
				values := make([]interface{}, 0, len(fetchedItems)*2)
				for f, item := range fetchedItems {
					bytes, err := r.pack(item.GetValue(), item.GetTTL())
					if err != nil {
						return err
					}
					values = append(values, f, bytes)
					result[f] = item
				}

				if len(values) > 0 {
					if err := r.client.HSet(ctx, key, values...).Err(); err != nil {
						return err
					}
				}
			}
		}

		return nil
	})

	return result, err
}

func (r *redisStore) Remove(ctx context.Context, key string, fields []string, opts ...Option) error {
	o := &options{}
	for _, opt := range opts {
		opt(o)
	}
	return r.withLockWrapper(ctx, key, o.lockTTL, func() error {
		return r.client.HDel(ctx, key, fields...).Err()
	})
}

func (r *redisStore) Has(ctx context.Context, key, field string, opts ...Option) (bool, error) {
	o := &options{}
	for _, opt := range opts {
		opt(o)
	}

	var exists bool
	err := r.withLockWrapper(ctx, key, o.lockTTL, func() error {
		val, err := r.client.HGet(ctx, key, field).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				return nil
			}
			return err
		}
		if _, err := r.unpack([]byte(val)); err == nil {
			exists = true
		}
		return nil
	})

	return exists, err
}

func (r *redisStore) Close() error {
	return r.client.Close()
}
