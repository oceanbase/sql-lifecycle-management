// Package logger holds the mostly used logger instance `GlobalLogger`
package logger

import (
	"github.com/oceanbase/sql-lifecycle-management/agent/pkg/log/util"
	"go.uber.org/zap"
)

var _globalLogger *zap.Logger
var _sugaredGlobalLogger *zap.SugaredLogger

func init() {
	SetLogger(util.GetDefault())
}

// SetLogger inject a logger instance. It is not strictly thread-safe. So there may still have logs
// output to the old logger within a short time.
func SetLogger(instance *zap.Logger) {
	theOldOne := _globalLogger

	_globalLogger = instance
	_sugaredGlobalLogger = instance.Sugar()

	// try our best to do a sync, ignore the error
	if theOldOne != nil {
		if err := theOldOne.Sync(); err != nil {
			return
		}
	}
}

// Debug logs a message at DebugLevel. The message includes any fields passed at the log site, as
// well as any fields accumulated on the logger.
func Debug(msg string, fields ...zap.Field) {
	_globalLogger.Debug(msg, fields...)
}

// Debugf wraps Debugf of sugared logger, it uses fmt.Sprintf to log a templated message.
func Debugf(template string, args ...interface{}) {
	_sugaredGlobalLogger.Debugf(template, args...)
}

// Info logs a message at InfoLevel. The message includes any fields passed at the log site, as well
// as any fields accumulated on the logger.
func Info(msg string, fields ...zap.Field) {
	_globalLogger.Info(msg, fields...)
}

// Infof wraps Infof of sugared logger, it uses fmt.Sprintf to log a templated message.
func Infof(template string, args ...interface{}) {
	_sugaredGlobalLogger.Infof(template, args...)
}

// Warn logs a message at WarnLevel. The message includes any fields passed at the log site, as well
// as any fields accumulated on the logger.
func Warn(msg string, fields ...zap.Field) {
	_globalLogger.Warn(msg, fields...)
}

// Warnf wraps Warnf of sugared logger, it uses fmt.Sprintf to log a templated message.
func Warnf(template string, args ...interface{}) {
	_sugaredGlobalLogger.Warnf(template, args...)
}

// Error logs a message at ErrorLevel. The message includes any fields passed at the log site, as
// well as any fields accumulated on the logger.
func Error(msg string, fields ...zap.Field) {
	_globalLogger.Error(msg, fields...)
}

// Errorf wraps Errorf of sugared logger, it uses fmt.Sprintf to log a templated message.
func Errorf(template string, args ...interface{}) {
	_sugaredGlobalLogger.Errorf(template, args...)
}

// Panic logs a message at PanicLevel. The message includes any fields passed at the log site, as
// well as any fields accumulated on the logger.
//
// The logger then panics, even if logging at PanicLevel is disabled.
func Panic(msg string, fields ...zap.Field) {
	_globalLogger.Panic(msg, fields...)
}

// Panicf wraps Panicf of sugared logger, it uses fmt.Sprintf to log a templated message. Other
// behaviors are similar to Panic.
func Panicf(template string, args ...interface{}) {
	_sugaredGlobalLogger.Panicf(template, args...)
}

// Fatal logs a message at FatalLevel. The message includes any fields passed at the log site, as
// well as any fields accumulated on the logger.
//
// The logger then calls os.Exit(1), even if logging at FatalLevel is disabled.
func Fatal(msg string, fields ...zap.Field) {
	_globalLogger.Fatal(msg, fields...)
}

// Fatalf wraps Fatalf of sugared logger, it uses fmt.Sprintf to log a templated message. Other
// behaviors are similar to Fatal.
func Fatalf(template string, args ...interface{}) {
	_sugaredGlobalLogger.Fatalf(template, args...)
}

// Sync calls the underlying Core's Sync method, flushing any buffered log
// entries. Applications should take care to call Sync before exiting.
func Sync() error {
	return _globalLogger.Sync()
}
