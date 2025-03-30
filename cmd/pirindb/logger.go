package main

import (
	"github.com/phsym/console-slog"
	"log/slog"
	"os"
)

func getLogLevel(level string) slog.Level {
	switch level {
	case "DEBUG":
		return slog.LevelDebug
	case "INFO":
		return slog.LevelInfo
	case "WARNING":
		return slog.LevelWarn
	case "ERROR":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func createLogger(logLevel string) *slog.Logger {
	return slog.New(
		console.NewHandler(os.Stderr, &console.HandlerOptions{Level: getLogLevel(logLevel)}),
	)
}
