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

func NewMem(cfg MemConfig) (Store, error) {
	return &memStore{
		data:  make(map[string]map[string]Entry),
		locks: make(map[string]int64),
	}, nil
}

func (m *memStore) makeEntry(value any, ttl time.Duration) Entry {
	var expireAt int64
	if ttl > 0 {
		expireAt = time.Now().Add(ttl).UnixNano()
	}
	return Entry{
		Val:       value,
		ExpiresAt: expireAt,
	}
}

// putInternal 内部无锁 Put
func (m *memStore) putInternal(key, field string, item Item) {
	entry := m.makeEntry(item.GetValue(), item.GetTTL())
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.data[key]; !ok {
		m.data[key] = make(map[string]Entry)
	}
	m.data[key][field] = entry
}

func (m *memStore) withLockWrapper(key string, lockTTL time.Duration, op func() error) error {
	if lockTTL <= 0 {
		return op()
	}

	m.locksMu.Lock()
	lockKey := key
	now := time.Now().UnixNano()

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
	o := &options{}
	for _, opt := range opts {
		opt(o)
	}

	return m.withLockWrapper(key, o.lockTTL, func() error {
		m.putInternal(key, field, item)
		return nil
	})
}

func (m *memStore) Get(ctx context.Context, key, field string, opts ...Option) (Item, error) {
	o := &options{}
	for _, opt := range opts {
		opt(o)
	}

	var result Item
	err := m.withLockWrapper(key, o.lockTTL, func() error {
		// 1. 尝试从内存读取
		m.mu.RLock()
		fields, ok := m.data[key]
		var entry Entry
		var exists bool
		if ok {
			entry, exists = fields[field]
		}
		m.mu.RUnlock()

		// 2. 如果存在，检查是否过期
		if exists {
			if entry.ExpiresAt > 0 && time.Now().UnixNano() > entry.ExpiresAt {
				// 已过期：执行惰性删除
				m.mu.Lock()
				// 双重检查：防止在 RUnlock 和 Lock 之间被其他协程更新了新值
				if g, ok := m.data[key]; ok {
					if cur, ok := g[field]; ok && cur.ExpiresAt == entry.ExpiresAt {
						delete(g, field)
						if len(g) == 0 {
							delete(m.data, key)
						}
					}
				}
				m.mu.Unlock()
				// 标记为不存在，以便进入后续的回源逻辑
				exists = false
			} else {
				// 未过期：直接返回
				result = entry
				return nil
			}
		}

		// 3. 缓存未命中（不存在或已过期），检查是否配置了 FetchFunc
		if o.fetchFunc != nil {
			fetchedItem, err := o.fetchFunc()
			if err != nil {
				return err
			}
			// 回填缓存 (putInternal 内部会加 m.mu.Lock)
			m.putInternal(key, field, fetchedItem)
			result = fetchedItem
			return nil
		}

		return ErrMiss
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

		if entry.ExpiresAt > 0 && time.Now().UnixNano() > entry.ExpiresAt {
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
