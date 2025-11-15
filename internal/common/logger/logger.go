package logger

import (
	"log"

	"sync"

	"go.uber.org/zap"
)

var (
	logger   *zap.SugaredLogger
	initOnce sync.Once
)

// InitLogger initializes the logger once (thread-safe)
func InitLogger(env string) {
	initOnce.Do(func() {
		var cfg zap.Config

		if env == "prod" {
			cfg = zap.NewProductionConfig()
		} else {
			// Dev-friendly logging
			cfg = zap.NewDevelopmentConfig()
		}

		cfg.OutputPaths = []string{"stdout"}
		cfg.ErrorOutputPaths = []string{"stdout"}

		zl, err := cfg.Build(zap.AddCallerSkip(1))
		if err != nil {
			log.Fatalf("failed to initialize zap logger: %v", err)
		}

		logger = zl.Sugar()
	})
}

func Log() *zap.SugaredLogger {
	if logger == nil {
		// Lazy init with dev mode
		InitLogger("dev")
	}
	return logger
}

func Sync() {
	if logger != nil {
		_ = logger.Sync()
	}
}
