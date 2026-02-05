package config

import (
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const logFileName = "App.log"

var (
	globLogger *zap.Logger
	once       sync.Once
)

func GetLogger() *zap.Logger {
	once.Do(func() {
		globLogger = createLogger()
	})
	return globLogger
}

func createLogger() *zap.Logger {
	encoderCfg := zap.NewProductionEncoderConfig()
	encoderCfg.TimeKey = "timestamp"
	encoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder

	config := zap.Config{
		Level:             zap.NewAtomicLevelAt(zap.InfoLevel),
		Development:       false,
		DisableCaller:     false,
		DisableStacktrace: false,
		Sampling:          nil,
		Encoding:          "json",
		EncoderConfig:     encoderCfg,
		OutputPaths: []string{
			"stdout",
			logFileName,
		},
		ErrorOutputPaths: []string{
			"stdout",
		},
		InitialFields: map[string]any{},
	}

	return zap.Must(config.Build())
}
