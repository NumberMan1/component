package share_db

import (
	"context"
	"encoding/json"
	"errors"
	zaplog "github.com/NumberMan1/component/zap-logger"
	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"
	"sync/atomic"
	"time"
)

var shareDb DB

func InitShareDb(logger zaplog.Logger, config Config) (err error) {
	shareDb, err = NewShareDB(config.Address, config.Password, config.DB, logger)
	return
}

func GetShareDb() DB {
	return shareDb
}

type Config struct {
	Address  string `json:"address" yaml:"address"`
	Password string `json:"password" yaml:"password"`
	DB       int    `json:"db" yaml:"db"`
}

type DB interface {
	ExecCommand
	BeginTx(context.Context) TxExecCommand
	Close()
	SetExpiredAt(ctx context.Context, key string, expiredAt time.Time) error
}

type ExecCommand interface {
	Set(ctx context.Context, key string, value any) error
	Get(ctx context.Context, key string, result any) error
	SetMap(ctx context.Context, key string, value map[string]any, formatFnArg ...func(any) ([]byte, error)) (err error)
	RangeMap(ctx context.Context, key string, handler func(field string, value any) error, parseFnArg ...func(data []byte) (any, error)) error
	DeleteMapField(ctx context.Context, key string, fields ...string) error
	Delete(ctx context.Context, key string) error
}

type redisDB struct {
	client *redis.Client
	logger zaplog.Logger
	closed atomic.Bool
}

func NewShareDB(addr, password string, db int, logger zaplog.Logger) (DB, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})
	err := client.Ping(context.Background()).Err()
	if err != nil {
		return nil, err
	}
	return &redisDB{
		client: client,
		logger: logger,
	}, nil
}

func (r *redisDB) Set(ctx context.Context, key string, value any) error {
	if r.closed.Load() {
		return context.Canceled
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return r.client.Set(ctx, key, data, 0).Err()
}

func (r *redisDB) Get(ctx context.Context, key string, result any) error {
	if r.closed.Load() {
		return context.Canceled
	}
	val, err := r.client.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return nil
	}
	if err != nil {
		return err
	}
	return json.Unmarshal([]byte(val), result)
}

func (r *redisDB) SetMap(ctx context.Context, key string, value map[string]any, formatFnArg ...func(any) ([]byte, error)) (err error) {
	if r.closed.Load() {
		return context.Canceled
	}
	fields := make(map[string]interface{}, len(value))
	for k, v := range value {
		var data any
		if len(formatFnArg) > 0 && formatFnArg[0] != nil {
			data, err = formatFnArg[0](v)
		} else {
			data, err = json.Marshal(v)
		}
		if err != nil {
			return err
		}
		fields[k] = data
	}
	err = r.client.HSet(ctx, key, fields).Err()
	return
}

func (r *redisDB) RangeMap(ctx context.Context, key string, handler func(field string, value any) error, parseFnArg ...func([]byte) (any, error)) error {
	if r.closed.Load() {
		return context.Canceled
	}
	fields, err := r.client.HGetAll(ctx, key).Result()
	if err != nil {
		return err
	}
	for k, v := range fields {
		if k == "0" && v == "-1" {
			continue
		}
		var parsedValue any
		if len(parseFnArg) > 0 {
			parseFn := parseFnArg[0]
			parsedValue, err = parseFn([]byte(v))
		} else {
			err = json.Unmarshal([]byte(v), &parsedValue)
		}
		if err != nil {
			r.logger.Warn("Failed to parse field", zap.String("field", k), zap.String("raw_value", v), zap.Error(err))
			continue
		}
		if handlerErr := handler(k, parsedValue); handlerErr != nil {
			return handlerErr
		}
	}
	return nil
}

func (r *redisDB) DeleteMapField(ctx context.Context, key string, fields ...string) error {
	if r.closed.Load() {
		return context.Canceled
	}
	return r.client.HDel(ctx, key, fields...).Err()
}

func (r *redisDB) Delete(ctx context.Context, key string) error {
	if r.closed.Load() {
		return context.Canceled
	}
	return r.client.Del(ctx, key).Err()
}

func (r *redisDB) BeginTx(ctx context.Context) TxExecCommand {
	return newTxDB(r, ctx)
}

func (r *redisDB) SetExpiredAt(ctx context.Context, key string, expiredAt time.Time) error {
	if r.closed.Load() {
		return context.Canceled
	}
	return r.client.Expire(ctx, key, expiredAt.Sub(time.Now())).Err()
}

func (r *redisDB) Close() {
	if r.closed.CompareAndSwap(false, true) {
		_ = r.client.Close()
	}
}
