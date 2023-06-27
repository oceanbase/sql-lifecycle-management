package log

import (
	"fmt"
	"os"
	"path"
	"testing"

	"gopkg.in/yaml.v2"

	"github.com/oceanbase/sql-lifecycle-management/agent/pkg/log/logger"
	"github.com/oceanbase/sql-lifecycle-management/agent/pkg/log/util"
	"go.uber.org/zap"
)

const (
	logConfYAML string = `
     level: info
     root-path: ./_logs
     disable-error-verbose: true
     development: false
     global-logger:
       file-confs:
         - file-name: global-error.log
           max-size: 300
           max-days: 5
           max-backups: 5
           level-max: error
           level-min: error
         - file-name: global-all.log
           max-size: 300
           max-days: 5
           max-backups: 5
           level-min: info
           level-max: fatal
       console-confs:
         - console-fd: stdout
           level-min: info
           level-max: fatal
`
)

func TestLoggingExample(_ *testing.T) {
	pwd, _ := os.Getwd()
	logRootPath := path.Join(pwd, "_logs")
	fmt.Println("log root path will be:", logRootPath)

	// do the real work
	_initializeWithConf()
	_printLogs()

	// clean up the log files
	// if you want to see the files and its content, comment off the _cleanup function
	_cleanup(logRootPath)
}

func _initializeWithConf() {
	var logConfStruct util.Config
	if err := yaml.Unmarshal([]byte(logConfYAML), &logConfStruct); err != nil {
		// here we can safely use logger API. Before we initialize loggers with the user's config,
		// there is a default underlying console logger.
		logger.Infof("unmarshal logger config errors: %v", err)
	}
	// validate all args
	logConfStruct.Verify()
	InitLoggers(&logConfStruct)
}

func _printLogs() {
	destStdout := "stdout"
	destFileLogAll := "global-all.log"
	destFileLogError := "global-info.log"

	// log a message without any fields
	logger.Debug("debug log will be ignored")
	logger.Info("info log will be printed to global-all.log and console")
	logger.Error("error log will be printed to global-all.log, global-error.log and console")

	// log a message with some fields, using zap-style API
	logger.Debug("debug log", zap.Int32("destination size", 0))
	logger.Info(
		"info log",
		zap.Int32("destination size", 2),
		zap.String("file", destFileLogAll),
		zap.String("console", destStdout),
	)
	logger.Error(
		"error log",
		zap.Int32("destination size", 3),
		zap.String("file", destFileLogAll),
		zap.String("file", destFileLogError),
		zap.String("console", destStdout),
	)

	// log a message with some fields, using printf-style API
	logger.Debugf("debug log, destination size: %d", 0)
	logger.Infof(
		"info log, destination size: %d, file: %s, console: %s",
		2,
		destFileLogAll,
		destStdout,
	)
	logger.Errorf(
		"error log, destination size: %d, file: %s, file: %s, console: %s",
		3,
		destFileLogAll,
		destFileLogError,
		destStdout,
	)
}

func _cleanup(rootPath string) {
	allDotLog := path.Join(rootPath, "global-all.log")
	errorDotLog := path.Join(rootPath, "global-error.log")
	if err := os.Remove(allDotLog); err != nil {
		fmt.Printf("remove log file fails, file = [%s], error = [%v]", allDotLog, err)
	}
	if err := os.Remove(errorDotLog); err != nil {
		fmt.Printf("remove log file fails, file = [%s], error = [%v]", errorDotLog, err)
	}
	// if the path contains other files, path wil not be removed, and other files will not be
	// affected
	if err := os.Remove(rootPath); err != nil {
		fmt.Printf("remove log root path fails, path = [%s], error = [%v]", rootPath, err)
	}
}
