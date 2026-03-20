package apputil

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// BuildLogger parses a level string and returns a development zap.Logger and AtomicLevel.
func BuildLogger(levelStr string) (*zap.Logger, zap.AtomicLevel, error) {
	var logLevel zapcore.Level
	if err := logLevel.UnmarshalText([]byte(levelStr)); err != nil {
		return nil, zap.AtomicLevel{}, fmt.Errorf("invalid log level %q: %w", levelStr, err)
	}
	atomicLevel := zap.NewAtomicLevelAt(logLevel)
	cfg := zap.NewDevelopmentConfig()
	cfg.Level = atomicLevel
	logger, err := cfg.Build()
	if err != nil {
		return nil, zap.AtomicLevel{}, err
	}
	return logger, atomicLevel, nil
}

// StartPPROF starts the pprof HTTP server in a goroutine; no-op if addr is empty.
func StartPPROF(addr string, logger *zap.Logger) {
	if addr == "" {
		return
	}
	go func() {
		logger.Info("pprof listening", zap.String("addr", addr))
		if err := http.ListenAndServe(addr, nil); err != nil {
			logger.Error("pprof server error", zap.Error(err))
		}
	}()
}
