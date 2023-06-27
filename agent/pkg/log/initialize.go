// Package log provides logger initialize utilities for the whole project. It will create and set
// proper logger instances according to config. The underlying logging tool is `zap`.  In this
// package, we config it and also make some extensions for it.
package log

import (
	"fmt"
	"sync"

	"github.com/oceanbase/sql-lifecycle-management/agent/pkg/log/logger"
	"github.com/oceanbase/sql-lifecycle-management/agent/pkg/log/util"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var once sync.Once

// InitLoggers initializes all logger instances according to user-provided configs
// Note that this function is protected by a `sync.Once`.
func InitLoggers(cfg *util.Config) {
	once.Do(func() {
		_initLoggers(cfg)
	})
}

func _initLoggers(cfg *util.Config) {
	if !cfg.IsVerified() {
		panic("logger config was not verified, cannot init loggers with it")
	}
	opts := cfg.BuildOpts()

	// init global logger
	if cfg.GlobalLogger == nil {
		return
	}
	coreSlice := make([]zapcore.Core, 0)
	fileConfs := cfg.GlobalLogger.Files
	for _, fileConf := range fileConfs {
		encoder := util.NewTextEncoderByConfig(cfg)
		core, err := util.CreateFileCore(cfg.RootPath, fileConf, cfg.GetLevel(), encoder)
		if err != nil {
			fmt.Printf("init log file errors, file-name: %s, error: %v", fileConf.FileName, err)
			continue
		}
		coreSlice = append(coreSlice, core)
	}

	consoleConfs := cfg.GlobalLogger.Consoles
	for _, consoleConf := range consoleConfs {
		encoder := util.NewTextEncoderByConfig(cfg)
		core := util.CreateConsoleCore(consoleConf, cfg.GetLevel(), encoder)
		coreSlice = append(coreSlice, core)
	}

	// combines multiple zapcore into one
	globalLoggerCore := zapcore.NewTee(coreSlice...)
	globalLogger := zap.New(globalLoggerCore, opts...)
	logger.SetLogger(globalLogger)
}
