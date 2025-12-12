package kvstorage

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/go-redis/redis/v8"
)

type redisStore struct {
	client *redis.Client
}

func New(cfg RedisConfig) (Store, error) {
	if cfg.Addr == "" {
		return nil, ErrConfig
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:         cfg.Addr,
		Password:     cfg.Password,
		DB:           cfg.DB,
		PoolSize:     cfg.PoolSize,
		MinIdleConns: cfg.MinIdleConns,
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
		expireAt = time.Now().Add(ttl).UnixNano()
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
	if item.ExpiresAt > 0 && time.Now().UnixNano() > item.ExpiresAt {
		return nil, ErrMiss
	}
	return item, nil
}

// putInternal 内部无锁 Put，用于 Get 内部回填
func (r *redisStore) putInternal(ctx context.Context, key, field string, item Item) error {
	bytes, err := r.pack(item.GetValue(), item.GetTTL())
	if err != nil {
		return err
	}
	return r.client.HSet(ctx, key, field, bytes).Err()
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
		script := `
			if redis.call("get", KEYS[1]) == ARGV[1] then
				return redis.call("del", KEYS[1])
			else
				return 0
			end
		`
		r.client.Eval(ctx, script, []string{lockKey}, token)
	}()

	return op()
}

// ---------------------------------------------------------
// 字段 (Field) 操作实现
// ---------------------------------------------------------

func (r *redisStore) Put(ctx context.Context, key, field string, item Item, opts ...Option) error {
	o := &options{}
	for _, opt := range opts {
		opt(o)
	}

	return r.withLockWrapper(ctx, key, o.lockTTL, func() error {
		return r.putInternal(ctx, key, field, item)
	})
}

func (r *redisStore) Get(ctx context.Context, key, field string, opts ...Option) (Item, error) {
	o := &options{}
	for _, opt := range opts {
		opt(o)
	}

	var result Item
	err := r.withLockWrapper(ctx, key, o.lockTTL, func() error {
		// Lua 脚本负责读取、解析过期并删除
		script := `
			local val = redis.call("HGET", KEYS[1], ARGV[1])
			if not val then return nil end
			
			local item = cjson.decode(val)
			local exp = tonumber(item.e)
			local now = tonumber(ARGV[2])
			
			if exp and exp > 0 and now > exp then
				redis.call("HDEL", KEYS[1], ARGV[1])
				return nil
			end
			
			return val
		`
		nowNano := time.Now().UnixNano()
		val, err := r.client.Eval(ctx, script, []string{key}, field, nowNano).Result()

		// 1. 处理系统错误（网络问题等）
		if err != nil && !errors.Is(err, redis.Nil) {
			return err
		}

		// 2. 尝试处理命中逻辑
		// 如果 err 为 nil 且 val 为 string，说明命中缓存且未过期
		if err == nil {
			if s, ok := val.(string); ok {
				item, err := r.unpack([]byte(s))
				if err != nil {
					return err
				}
				result = item
				return nil
			}
		}

		// 3. 处理未命中逻辑 (err 为 redis.Nil 或者 val 不是 string)
		// 检查是否配置了 FetchFunc 进行回源
		if o.fetchFunc != nil {
			fetchedItem, err := o.fetchFunc()
			if err != nil {
				return err
			}
			if err := r.putInternal(ctx, key, field, fetchedItem); err != nil {
				return err
			}
			result = fetchedItem
			return nil
		}

		return ErrMiss
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
		script := `
			local val = redis.call("HGET", KEYS[1], ARGV[1])
			if not val then return 0 end
			
			local item = cjson.decode(val)
			local exp = tonumber(item.e)
			local now = tonumber(ARGV[2])
			
			if exp and exp > 0 and now > exp then
				redis.call("HDEL", KEYS[1], ARGV[1])
				return 0
			end
			
			return 1
		`
		nowNano := time.Now().UnixNano()
		res, err := r.client.Eval(ctx, script, []string{key}, field, nowNano).Result()
		if err != nil {
			return err
		}
		exists = res.(int64) == 1
		return nil
	})

	return exists, err
}

func (r *redisStore) Close() error {
	return r.client.Close()
}
