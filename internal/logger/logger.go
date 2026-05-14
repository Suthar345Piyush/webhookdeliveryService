// universal logger

package logger

import (
	"fmt"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// using Log package level logger

var Log *zap.Logger

// zap logger function

func Init() error {

	cfg := zap.NewProductionConfig()

	// standard timestamp in logs, easy to understand

	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	// message key

	cfg.EncoderConfig.MessageKey = "message"

	var err error

	Log, err = cfg.Build()

	if err != nil {
		return fmt.Errorf("logger: failed to build zap logger: %w", err)
	}

	return nil
}

// sync will clear any buffered entries

func Sync() {
	if Log != nil {
		_ = Log.Sync()
	}
}
