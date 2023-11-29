package utils

import (
	"fmt"
	"github.com/rs/zerolog"
	"os"
	"time"
)

var logger zerolog.Logger

func InitializeLogger() {
	zerolog.TimeFieldFormat = "2006-01-02 15:04:05.000000"
	logger = zerolog.New(os.Stdout).With().Timestamp().Logger()
}

func LogEvent(eventType string, message string) {
	switch eventType {
	case "debug":
		logger.Debug().Msg(message)
	case "info":
		logger.Info().Msg(message)
	case "warn":
		logger.Warn().Msg(message)
	case "error":
		logger.Error().Msg(message)
	case "panic":
		logger.Panic().Msg(message)
	case "fatal":
		logger.Fatal().Msg(message)
	case "trace":
		logger.Trace().Msg(message)
	default:
		logger.Info().Msg(message)
	}
}

func WithRetry(attempts int, sleep time.Duration, fn func() error) error {
	for i := 0; ; i++ {
		err := fn()
		if err == nil {
			return nil // success
		}

		if i >= (attempts - 1) {
			LogEvent("fatal", "Fatal: "+err.Error())
			return err // return the last error
		}

		LogEvent("warn", fmt.Sprintf("Attempt %d failed; retrying in %v", i+1, sleep))
		time.Sleep(sleep)
		sleep *= 2
	}
}
