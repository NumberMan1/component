package storage

import (
	"context"
	"errors"
	"sync"
)

// NewMemoryStore creates a new in-memory transactional store.
func NewMemoryStore() MemoryTransactional {
	return &memoryStore{
		data: make(map[string]MemoryStorageData),
	}
}

// memoryStore is the base in-memory implementation of MemoryTransactional.
type memoryStore struct {
	mu   sync.RWMutex
	data map[string]MemoryStorageData
}

func (m *memoryStore) HSet(ctx context.Context, field string, value MemoryStorageData) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[field] = value.Copy()
	return nil
}

func (m *memoryStore) HGet(ctx context.Context, field string, dest MemoryStorageData) error {
	m.mu.RLock()
	v, ok := m.data[field]
	m.mu.RUnlock()
	if !ok {
		return errors.New("hash field not found")
	}
	dest.SetValue(v.Copy())
	return nil
}

func (m *memoryStore) BeginTx() (MemoryTransaction, error) {
	m.mu.RLock()
	snap := make(map[string]MemoryStorageData, len(m.data))
	for k, v := range m.data {
		snap[k] = v.Copy()
	}
	m.mu.RUnlock()

	return &memoryTx{
		base:     m,
		snapshot: snap,
		writes:   make(map[string]MemoryStorageData),
	}, nil
}

// memoryTx buffers reads/writes against a snapshot of the store.
type memoryTx struct {
	base     *memoryStore
	snapshot map[string]MemoryStorageData
	writes   map[string]MemoryStorageData

	mu   sync.RWMutex
	done bool
}

func (tx *memoryTx) HSet(field string, value MemoryStorageData) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done {
		return errors.New("transaction already finished")
	}
	tx.writes[field] = value.Copy()
	return nil
}

func (tx *memoryTx) HGet(field string, dest MemoryStorageData) error {
	tx.mu.RLock()
	if v, ok := tx.writes[field]; ok {
		tx.mu.RUnlock()
		dest.SetValue(v.Copy())
		return nil
	}
	v, ok := tx.snapshot[field]
	tx.mu.RUnlock()
	if !ok {
		return errors.New("hash field not found")
	}
	dest.SetValue(v.Copy())
	return nil
}

func (tx *memoryTx) Commit() {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done {
		return
	}
	tx.base.mu.Lock()
	for k, v := range tx.writes {
		tx.base.data[k] = v.Copy()
	}
	tx.base.mu.Unlock()
	tx.done = true
}

func (tx *memoryTx) Rollback() {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	tx.done = true
}
