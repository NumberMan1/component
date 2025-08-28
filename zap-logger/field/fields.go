package field

import (
	"github.com/NumberMan1/numbox/utils"
	"go.uber.org/zap"
)

var (
	String  = zap.String
	Int     = zap.Int
	Any     = zap.Any
	Int64   = zap.Int64
	Uint64  = zap.Uint64
	Float64 = zap.Float64
	Int64s  = zap.Int64s
	Ints    = zap.Ints
	Int32   = zap.Int32
	Int32s  = zap.Int32s
	Strings = zap.Strings
	Error   = zap.Error
)

func NewFields(fields ...Field) Fields {
	var res Fields
	for _, field := range fields {
		res = append(res, field)
	}
	return res
}

type Fields []zap.Field

func (list Fields) With(fields ...Field) Fields {
	list = append(list, fields...)
	return list
}

type Field = zap.Field

func WithTraceId(traceId string) Field {
	return String("trace_id", traceId)
}

func WithSession(sessionId uint64) Field {
	return Uint64("session_id", sessionId)
}

func WithService(serverName string) Field {
	return String("server", serverName)
}

func WithTargetService(serverName string) Field {
	return String("target-server", serverName)
}

func WithPlayerId(playerId int64) Field {
	return Int64("player_id", playerId)
}

func WithPlayerIds(playerIds []int64) Field {
	return Int64s("player_ids", playerIds)
}

func WithMethod(methodName string) Field {
	return String("method", methodName)
}

func WithData(data any) Field {
	return String("data", utils.ToJsonString(data))
}

func WithError(err error) Field {
	return String("error", err.Error())
}

func WithCostUS(costTime int64) Field {
	return Int64("cost", costTime)
}

func WithBusinessCostUS(costTime int64) Field {
	return Int64("business_cost", costTime)
}

func WithAccountId(accountId int64) Field {
	return Int64("account_id", accountId)
}

func WithIndexKey(indexKey string) Field {
	return String("index_name", indexKey)
}

func AnyString(key string, data interface{}) Field {
	return String(key, utils.ToJsonString(data))
}

func WithSpanId(data string) Field {
	return String("span_id", data)
}

func WithParentSpanId(data string) Field {
	return String("parent_span_id", data)
}

func WithErrorStack(innerErr error, stack []byte) zap.Field {
	return String("error_stack", "error:"+innerErr.Error()+" stack:"+string(stack))
}

func WithStringsMap(key string, headersMap map[string]string) Field {
	var strings = make([]string, 0)
	for k, v := range headersMap {
		strings = append(strings, k+":"+v)
	}
	return Strings(key, strings)
}

func WithHeaders(headersMap map[string]string) Field {
	return WithStringsMap("headers", headersMap)
}
