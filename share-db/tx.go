package share_db

import (
	"context"
	"encoding/json"
	"errors"
	zaplog "github.com/NumberMan1/component/zap-logger"
	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"
)

var (
	ErrEmptyKeyValue = errors.New("key is empty")
	ErrTxNotFound    = errors.New("transaction not found")
	ErrTxFinished    = errors.New("transaction has been committed or rolled back")
)

type TxExecCommand interface {
	ExecCommand
	Commit() error
	Rollback() error
}

type txDB struct {
	db       *redisDB
	ctx      context.Context
	pipe     redis.Pipeliner
	logger   zaplog.Logger
	finished bool

	cache         map[string][]byte
	mapCache      map[string]map[string][]byte
	deleted       map[string]struct{}
	deletedFields map[string]map[string]struct{}
}

func newTxDB(db *redisDB, ctx context.Context) *txDB {
	tx := &txDB{
		db:     db,
		ctx:    ctx,
		logger: db.logger,
		pipe:   db.client.TxPipeline(),

		// 初始化缓存
		cache:         make(map[string][]byte),
		mapCache:      make(map[string]map[string][]byte),
		deleted:       make(map[string]struct{}),
		deletedFields: make(map[string]map[string]struct{}),
	}
	return tx
}

func (tx *txDB) checkState() error {
	if tx.db.closed.Load() {
		return context.Canceled
	}
	if tx.finished {
		return ErrTxFinished
	}
	return nil
}

func (tx *txDB) Set(ctx context.Context, key string, value any) error {
	if err := tx.checkState(); err != nil {
		return err
	}

	data, err := json.Marshal(value)
	if err != nil {
		return err
	}

	tx.cache[key] = data
	delete(tx.deleted, key)
	return tx.pipe.Set(ctx, key, data, 0).Err()
}

func (tx *txDB) Get(ctx context.Context, key string, result any) error {
	if err := tx.checkState(); err != nil {
		return err
	}

	if _, ok := tx.deleted[key]; ok {
		return ErrEmptyKeyValue
	}

	if data, ok := tx.cache[key]; ok {
		return json.Unmarshal(data, result)
	}

	val, err := tx.db.client.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return nil
	}
	if err != nil {
		return err
	}
	return json.Unmarshal([]byte(val), result)
}

func (tx *txDB) SetMap(ctx context.Context, key string, value map[string]any, formatFnArg ...func(any) ([]byte, error)) error {
	if err := tx.checkState(); err != nil {
		return err
	}

	if _, ok := tx.mapCache[key]; !ok {
		tx.mapCache[key] = make(map[string][]byte)
	}

	fields := make(map[string]interface{}, len(value))
	for k, v := range value {
		var data []byte
		var err error

		if len(formatFnArg) > 0 && formatFnArg[0] != nil {
			data, err = formatFnArg[0](v)
		} else {
			data, err = json.Marshal(v)
		}
		if err != nil {
			return err
		}

		fields[k] = data
		tx.mapCache[key][k] = data

		if fieldMap, ok := tx.deletedFields[key]; ok {
			delete(fieldMap, k)
		}
	}

	return tx.pipe.HSet(ctx, key, fields).Err()
}

func (tx *txDB) RangeMap(ctx context.Context, key string, handler func(field string, value any) error, parseFnArg ...func([]byte) (any, error)) error {
	if err := tx.checkState(); err != nil {
		return err
	}

	if _, ok := tx.deleted[key]; ok {
		return ErrEmptyKeyValue
	}

	fields, err := tx.db.client.HGetAll(ctx, key).Result()
	if err != nil {
		return err
	}

	mergedFields := make(map[string][]byte)
	for k, v := range fields {
		mergedFields[k] = []byte(v)
	}

	if cachedFields, ok := tx.mapCache[key]; ok {
		for k, v := range cachedFields {
			mergedFields[k] = v
		}
	}

	if deletedFields, ok := tx.deletedFields[key]; ok {
		for field := range deletedFields {
			delete(mergedFields, field)
		}
	}

	for k, v := range mergedFields {
		var parsedValue any
		var err error

		if len(parseFnArg) > 0 && parseFnArg[0] != nil {
			parsedValue, err = parseFnArg[0](v)
		} else {
			err = json.Unmarshal(v, &parsedValue)
		}

		if err != nil {
			tx.logger.Warn("Failed to parse field",
				zap.String("field", k),
				zap.String("raw_value", string(v)),
				zap.Error(err))
			continue
		}

		if err := handler(k, parsedValue); err != nil {
			return err
		}
	}

	return nil
}

func (tx *txDB) DeleteMapField(ctx context.Context, key string, fields ...string) error {
	if err := tx.checkState(); err != nil {
		return err
	}

	if _, ok := tx.deletedFields[key]; !ok {
		tx.deletedFields[key] = make(map[string]struct{})
	}
	for _, field := range fields {
		tx.deletedFields[key][field] = struct{}{}
		if cached, ok := tx.mapCache[key]; ok {
			delete(cached, field)
		}
	}

	return tx.pipe.HDel(ctx, key, fields...).Err()
}

func (tx *txDB) Delete(ctx context.Context, key string) error {
	if err := tx.checkState(); err != nil {
		return err
	}

	tx.deleted[key] = struct{}{}
	delete(tx.cache, key)
	delete(tx.mapCache, key)
	delete(tx.deletedFields, key)

	return tx.pipe.Del(ctx, key).Err()
}

func (tx *txDB) cleanup() {
	tx.pipe = nil
	tx.cache = nil
	tx.mapCache = nil
	tx.deleted = nil
	tx.deletedFields = nil
}

func (tx *txDB) Commit() error {
	if err := tx.checkState(); err != nil {
		return err
	}

	_, err := tx.pipe.Exec(tx.ctx)
	tx.finished = true
	tx.cleanup()
	return err
}

func (tx *txDB) Rollback() error {
	if err := tx.checkState(); err != nil {
		return err
	}

	err := tx.pipe.Discard()
	tx.finished = true
	tx.cleanup()
	return err
}
