package kvstorage

import (
	"context"
	"errors"
	"sync"
	"time"
)

// MemConfig 内存实现配置
type MemConfig struct{}

type memStore struct {
	data map[string]map[string]Entry
	mu   sync.RWMutex

	locks   map[string]int64
	locksMu sync.Mutex
}

func NewMemStore(cfg MemConfig) (Store, error) {
	return &memStore{
		data:  make(map[string]map[string]Entry),
		locks: make(map[string]int64),
	}, nil
}

func (m *memStore) makeEntry(value any, ttl time.Duration) Entry {
	var expireAt int64
	if ttl > 0 {
		expireAt = time.Now().Add(ttl).UnixMilli()
	}
	return Entry{
		Val:       value,
		ExpiresAt: expireAt,
	}
}

func (m *memStore) withLockWrapper(key string, lockTTL time.Duration, op func() error) error {
	if lockTTL <= 0 {
		return op()
	}

	m.locksMu.Lock()
	lockKey := key
	now := time.Now().UnixNano() // 锁还是可以用 Nano 精度，内部实现不影响接口

	if exp, ok := m.locks[lockKey]; ok {
		if now < exp {
			m.locksMu.Unlock()
			return errors.New("kvstorage: lock busy")
		}
	}

	m.locks[lockKey] = time.Now().Add(lockTTL).UnixNano()
	m.locksMu.Unlock()

	defer func() {
		m.locksMu.Lock()
		delete(m.locks, lockKey)
		m.locksMu.Unlock()
	}()

	return op()
}

func (m *memStore) Put(ctx context.Context, key, field string, item Item, opts ...Option) error {
	return m.PutBatch(ctx, key, map[string]Item{field: item}, opts...)
}

func (m *memStore) PutBatch(ctx context.Context, key string, items map[string]Item, opts ...Option) error {
	o := &options{}
	for _, opt := range opts {
		opt(o)
	}

	return m.withLockWrapper(key, o.lockTTL, func() error {
		m.mu.Lock()
		defer m.mu.Unlock()

		if _, ok := m.data[key]; !ok {
			m.data[key] = make(map[string]Entry)
		}

		for field, item := range items {
			entry := m.makeEntry(item.GetValue(), item.GetTTL())
			m.data[key][field] = entry
		}
		return nil
	})
}

func (m *memStore) Get(ctx context.Context, key, field string, opts ...Option) (Item, error) {
	resMap, err := m.GetBatch(ctx, key, []string{field}, opts...)
	if err != nil {
		return nil, err
	}
	if item, ok := resMap[field]; ok {
		return item, nil
	}
	return nil, ErrMiss
}

func (m *memStore) GetBatch(ctx context.Context, key string, fields []string, opts ...Option) (map[string]Item, error) {
	o := &options{}
	for _, opt := range opts {
		opt(o)
	}

	result := make(map[string]Item)
	var missingFields []string

	err := m.withLockWrapper(key, o.lockTTL, func() error {
		// 1. 内存读取
		m.mu.RLock()
		group, ok := m.data[key]
		m.mu.RUnlock()

		for _, field := range fields {
			var found bool
			if ok {
				if entry, exists := group[field]; exists {
					// 检查过期 (毫秒)
					if entry.ExpiresAt > 0 && time.Now().UnixMilli() > entry.ExpiresAt {
						// 惰性删除 (原子升级锁)
						m.mu.Lock()
						if g, stillExists := m.data[key]; stillExists {
							if cur, ok := g[field]; ok && cur.ExpiresAt == entry.ExpiresAt {
								delete(g, field)
								if len(g) == 0 {
									delete(m.data, key)
								}
							}
						}
						m.mu.Unlock()
						// 视为 Missing
					} else {
						result[field] = entry
						found = true
					}
				}
			}
			if !found {
				missingFields = append(missingFields, field)
			}
		}

		// 2. 回源处理
		if len(missingFields) > 0 && o.fetchFunc != nil {
			fetchedItems, err := o.fetchFunc(missingFields)
			if err != nil {
				return err
			}

			if len(fetchedItems) > 0 {
				// 回填内存 (无需再调 PutBatch，直接操作 map 即可，因为在锁内)
				m.mu.Lock()
				if _, ok := m.data[key]; !ok {
					m.data[key] = make(map[string]Entry)
				}
				for f, item := range fetchedItems {
					entry := m.makeEntry(item.GetValue(), item.GetTTL())
					m.data[key][f] = entry
					// 合并结果
					result[f] = entry
				}
				m.mu.Unlock()
			}
		}

		return nil
	})

	return result, err
}

// GetAll 获取 Key 下所有字段
func (m *memStore) GetAll(ctx context.Context, key string, opts ...Option) (map[string]Item, error) {
	o := &options{}
	for _, opt := range opts {
		opt(o)
	}

	result := make(map[string]Item)
	var missingFields []string

	err := m.withLockWrapper(key, o.lockTTL, func() error {
		// 1. 内存读取
		m.mu.RLock()
		group, ok := m.data[key]
		// 复制一份 field 列表或直接遍历，注意不要在 RLock 期间修改 map (如惰性删除)
		// 这里我们先收集数据，过期的标记为 missing，不做复杂的原子惰性删除，依靠 fetch 覆盖或下次 remove
		if ok {
			for field, entry := range group {
				if entry.ExpiresAt > 0 && time.Now().UnixMilli() > entry.ExpiresAt {
					missingFields = append(missingFields, field)
				} else {
					result[field] = entry
				}
			}
		}
		m.mu.RUnlock()

		// 2. 回源处理
		if len(missingFields) > 0 && o.fetchFunc != nil {
			fetchedItems, err := o.fetchFunc(missingFields)
			if err != nil {
				return err
			}

			if len(fetchedItems) > 0 {
				m.mu.Lock()
				// 二次检查 key 是否存在 (可能被其他协程删了)
				if _, ok := m.data[key]; !ok {
					m.data[key] = make(map[string]Entry)
				}
				for f, item := range fetchedItems {
					entry := m.makeEntry(item.GetValue(), item.GetTTL())
					m.data[key][f] = entry
					result[f] = entry
				}
				m.mu.Unlock()
			}
		}

		return nil
	})

	return result, err
}

func (m *memStore) Remove(ctx context.Context, key string, fields []string, opts ...Option) error {
	o := &options{}
	for _, opt := range opts {
		opt(o)
	}

	return m.withLockWrapper(key, o.lockTTL, func() error {
		m.mu.Lock()
		defer m.mu.Unlock()

		if group, ok := m.data[key]; ok {
			for _, f := range fields {
				delete(group, f)
			}
			if len(group) == 0 {
				delete(m.data, key)
			}
		}
		return nil
	})
}

func (m *memStore) Has(ctx context.Context, key, field string, opts ...Option) (bool, error) {
	o := &options{}
	for _, opt := range opts {
		opt(o)
	}

	var exists bool
	err := m.withLockWrapper(key, o.lockTTL, func() error {
		m.mu.RLock()
		group, ok := m.data[key]
		if !ok {
			m.mu.RUnlock()
			return nil
		}
		entry, ok := group[field]
		m.mu.RUnlock()

		if !ok {
			return nil
		}

		if entry.ExpiresAt > 0 && time.Now().UnixMilli() > entry.ExpiresAt {
			m.mu.Lock()
			if g, ok := m.data[key]; ok {
				if cur, ok := g[field]; ok && cur.ExpiresAt == entry.ExpiresAt {
					delete(g, field)
					if len(g) == 0 {
						delete(m.data, key)
					}
				}
			}
			m.mu.Unlock()
			return nil
		}

		exists = true
		return nil
	})

	return exists, err
}

func (m *memStore) Close() error {
	m.mu.Lock()
	m.data = nil
	m.locks = nil
	m.mu.Unlock()
	return nil
}
