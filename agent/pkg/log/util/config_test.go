package util

import (
	"os"
	"path"
	"testing"

	"gopkg.in/yaml.v2"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zapcore"
)

func TestCalcLogRootPath(t *testing.T) {
	currentDir, _ := os.Getwd()
	emptyArgResult, err0 := calcLogRootPath("")

	assert.Exactly(t, currentDir, emptyArgResult)
	assert.Exactly(t, nil, err0)

	expected := "/tmp/tatris"
	tmpDir1, err1 := calcLogRootPath("/tmp/tatris")
	tmpDir2, err2 := calcLogRootPath("/tmp/tatris/")
	tmpDir3, err3 := calcLogRootPath("/tmp/tatris/test/..")
	assert.Exactly(t, expected, tmpDir1)
	assert.Exactly(t, expected, tmpDir2)
	assert.Exactly(t, expected, tmpDir3)
	assert.Empty(t, err1)
	assert.Empty(t, err2)
	assert.Empty(t, err3)

	expectedJoined := path.Join(currentDir, "./test/target")
	relativeDir, err4 := calcLogRootPath("./test/target")
	assert.Exactly(t, expectedJoined, relativeDir)
	assert.Empty(t, err4)
}

const (
	validConfig1 string = `
     level: info
     root-path: /tmp/tatris/logs
     disable-error-verbose: true
     development: false
`
	validConfig2 string = `
     level: info
     root-path: /tmp/tatris/logs
     disable-error-verbose: true
     development: false
     global-logger:
       file-confs:
         - file-name: global-error.log
           max-size: 200
           max-days: 10
           max-backups: 20
           level-max: error
           level-min: error
         - file-name: global-all.log
           max-size: 200
           max-days: 10
           max-backups: 20
           level-min: info
       console-confs:
         - console-fd: stdout
           level-max: error
           level-min: info
`
)

func TestConfigUnmarshal(t *testing.T) {
	var validConfStruct1 Config
	err1 := yaml.Unmarshal([]byte(validConfig1), &validConfStruct1)
	assert.Empty(t, err1)
	validConfStruct1.Verify()

	var validConfStruct2 Config
	err2 := yaml.Unmarshal([]byte(validConfig2), &validConfStruct2)
	assert.Empty(t, err2)
	validConfStruct2.Verify()

	fileConf0 := validConfStruct2.GlobalLogger.Files[0]
	fileConf1 := validConfStruct2.GlobalLogger.Files[1]
	consoleConf1 := validConfStruct2.GlobalLogger.Consoles[0]

	// test invalid log level literal
	fileConf0.LevelMax = "info-x"
	assert.Panics(t, func() {
		fileConf0.verify()
	})

	// test disordered log level
	fileConf0.LevelMax = "info"
	fileConf0.LevelMin = "error"
	assert.Panics(t, func() {
		fileConf0.verify()
	})

	// test default level value
	fileConf1.verify()
	assert.Equal(t, zapcore.FatalLevel, fileConf1._maxLevel)

	// test file name
	fileConf1.FileName = "./test/file.log"
	assert.Panics(t, func() {
		fileConf1.verify()
	})

	// test stdout or stderr spelling
	consoleConf1.ConsoleFD = "stderr-x"
	assert.Panics(t, func() {
		consoleConf1.verify()
	})
}
