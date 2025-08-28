package zap_logger

import (
	standarderrs "errors"
	"github.com/NumberMan1/component/zap-logger/field"
	"github.com/NumberMan1/general/context"
	"github.com/NumberMan1/general/sign"
	"github.com/NumberMan1/log"
	"github.com/NumberMan1/numbox/utils"
	"time"
)

var defaultLogger Logger

func InitLogger(nodeId int32, config Config) {
	nodeIdStr := utils.FormatIntString(nodeId)
	defaultLogger = NewZapLogger(
		log.WithAppName(config.Name+"-"+nodeIdStr),
		log.WithStdout(config.Stdout, config.StdoutTyp),
		log.WithLevel(config.Level),
		log.WithFileOut(config.OutputFile(), config.LogFilePath, true),
	)
}

func GetLoggerCtx(ctx context.Context) (Logger, error) {
	if lg := ctx.Value(sign.LOGGER); lg != nil {
		return lg.(Logger), nil
	}
	return nil, standarderrs.New("no logger found")
}

func MustGetLoggerCtx(ctx context.Context) Logger {
	logger, err := GetLoggerCtx(ctx)
	if err != nil {
		return DefaultLogger()
	}
	return logger
}

type Logger interface {
	With(field ...field.Field) Logger
	Debug(msg string, field ...field.Field)
	Info(msg string, field ...field.Field)
	Warn(msg string, field ...field.Field)
	Error(msg string, field ...field.Field)
	Clone() Logger
}

func NewZapLogger(logOpts ...log.Option) Logger {
	return &ZapLogger{
		Logger:  log.New(logOpts...).(*log.Logger),
		logOpts: logOpts,
	}
}

func DefaultLogger() Logger {
	if defaultLogger == nil {
		defaultLogger = NewZapLogger()
	}
	return defaultLogger
}

type ZapLogger struct {
	*log.Logger
	logOpts []log.Option
}

func (logger *ZapLogger) With(fields ...field.Field) Logger {
	return &ZapLogger{
		Logger:  logger.Logger.With(fields...).(*log.Logger),
		logOpts: logger.logOpts,
	}
}

func (logger *ZapLogger) Clone() Logger {
	return &ZapLogger{
		Logger:  log.New(logger.logOpts...).(*log.Logger),
		logOpts: logger.logOpts,
	}
}

func AddFieldsToCtxLogger(ctx context.Context, fields ...field.Field) bool {
	ctxLogger, err := GetLoggerCtx(ctx)
	if err != nil {
		return false
	}
	ctxLogger = ctxLogger.With(fields...)
	ctx.With(sign.LOGGER, ctxLogger)
	return true
}

func AddCostUsFieldToByCtx(ctx context.Context, fieldName string, fn func()) {
	fnStart := time.Now()
	fn()
	AddFieldsToCtxLogger(ctx, field.Int64(fieldName, time.Since(fnStart).Microseconds()))
}
