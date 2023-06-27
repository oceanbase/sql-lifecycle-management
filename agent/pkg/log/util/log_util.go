package util

import (
	"errors"
	"os"
	"path"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	lumberjack "gopkg.in/natefinch/lumberjack.v2"
)

// defaultConsoleLogger is the internal logger that uses `stdout` as the output channel.
// We can use this default logger as an expedient, until the official loggers are initialized with
// user-provided config. It is highly recommended that the developers initialize all official
// loggers immediately after the process's startup.
var defaultConsoleLogger *zap.Logger

func init() {
	initDefaultConsoleLogger()
}

// initDefaultConsoleLogger initializes the default console logger with a relatively reasonable
// config.
func initDefaultConsoleLogger() {
	var conf ConsoleConfig
	conf.LevelMax = "fatal"
	conf.LevelMin = "info"
	conf.ConsoleFD = "stdout"
	conf.verify()
	cc := &zapcore.EncoderConfig{
		// Keys can be anything except the empty string.
		TimeKey:        "time",
		LevelKey:       "level",
		NameKey:        "name",
		CallerKey:      "caller",
		MessageKey:     "message",
		StacktraceKey:  "stack",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.CapitalLevelEncoder,
		EncodeTime:     DefaultTimeEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   ShortCallerEncoder,
	}
	encoder := NewTextEncoder(cc, false, false)
	defaultCore := CreateConsoleCore(&conf, zapcore.InfoLevel, encoder)
	defaultConsoleLogger = zap.New(defaultCore)
}

// GetDefault returns the pre-inited default logger. Any code outside `internal/common/log` is
// not allowed to call this function. Because this function is intentionally designed for the
// offical logger APIs. They usually wrapps this default console logger as the underlying logger,
// so that they can output things before the real ones are initialized.
func GetDefault() *zap.Logger {
	return defaultConsoleLogger
}

// CreateFileCore creates a new zapcore instance whose underlying file descriptor is a lumberjack
// wrapped regular file.
func CreateFileCore(
	rootpath string,
	fileConf *FileConfig,
	level zapcore.Level,
	encoder zapcore.Encoder,
) (zapcore.Core, error) {
	logFile, err := initLogFile(rootpath, fileConf)
	if err != nil {
		return nil, err
	}
	// lumberjack wrapped logger already has an internal lock, so there's no need to call
	// `zapcore.Lock`
	writeSyncer := zapcore.AddSync(logFile)
	// minLevel = Max(level, fileConf._minLevel)
	minLevel := level
	if fileConf._minLevel > minLevel {
		minLevel = fileConf._minLevel
	}
	levelFunc := zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
		return lvl >= minLevel && lvl <= fileConf._maxLevel
	})
	return zapcore.NewCore(encoder, writeSyncer, levelFunc), nil
}

// CreateConsoleCore creates a new zapcore instance whose underlying file descriptor is stdout or
// stderr.
func CreateConsoleCore(
	consoleConf *ConsoleConfig,
	level zapcore.Level,
	encoder zapcore.Encoder,
) zapcore.Core {
	lowercase := strings.ToLower(consoleConf.ConsoleFD)
	var writeSyncer zapcore.WriteSyncer
	if lowercase == "stdout" {
		writeSyncer = zapcore.Lock(os.Stdout)
	} else if lowercase == "stderr" {
		writeSyncer = zapcore.Lock(os.Stderr)
	}
	// minLevel = Max(level, fileConf._minLevel)
	minLevel := level
	if consoleConf._minLevel > minLevel {
		minLevel = consoleConf._minLevel
	}
	levelFunc := zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
		return lvl >= minLevel && lvl <= consoleConf._maxLevel
	})
	return zapcore.NewCore(encoder, writeSyncer, levelFunc)
}

// initLogFile create an abstract log file managed by lumberjack. `lumberjack` handles everything
// about file rotation and retention policies.
func initLogFile(logRootPath string, cfg *FileConfig) (*lumberjack.Logger, error) {
	fullPath := path.Join(logRootPath, cfg.FileName)
	if st, err := os.Stat(cfg.FileName); err == nil {
		if st.IsDir() {
			return nil, errors.New("specify directory as file")
		}
	}
	return &lumberjack.Logger{
		Filename:   fullPath,
		MaxSize:    cfg.MaxSize,
		MaxBackups: cfg.MaxBackups,
		MaxAge:     cfg.MaxDays,
		LocalTime:  true,
	}, nil
}

// NewTextEncoderByConfig creates a fast, low-allocation Text encoder with config. The encoder
// appropriately escapes all field keys and values.
func NewTextEncoderByConfig(cfg *Config) zapcore.Encoder {
	cc := zapcore.EncoderConfig{
		// Keys can be anything except the empty string.
		TimeKey:        "time",
		LevelKey:       "level",
		NameKey:        "name",
		CallerKey:      "caller",
		MessageKey:     "message",
		StacktraceKey:  "stack",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.CapitalLevelEncoder,
		EncodeTime:     DefaultTimeEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   ShortCallerEncoder,
	}
	return &textEncoder{
		EncoderConfig:       &cc,
		buf:                 _pool.Get(),
		spaced:              false,
		disableErrorVerbose: cfg.DisableErrorVerbose,
	}
}
