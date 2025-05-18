package main

import (
	"bytes"
	"github.com/phsym/console-slog"
	"log/slog"
	"os"
	"strings"
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

type slogWriter struct {
	logger *slog.Logger
}

func (w *slogWriter) Write(p []byte) (n int, err error) {
	line := string(bytes.TrimSpace(p)) // removes \n and leading/trailing spaces

	start := strings.Index(line, "[")
	end := strings.Index(line, "]")
	if start == -1 || end == -1 || end <= start+1 {
		// fallback if format unexpected
		w.logger.Info(line)
		return len(p), nil
	}

	levelStr := line[start+1 : end]
	msg := strings.TrimSpace(line[end+1:]) // cut timestamp + level, keep actual message

	switch strings.ToUpper(levelStr) {
	case "DEBUG":
		w.logger.Debug(msg)
	case "WARN", "WARNING":
		w.logger.Warn(msg)
	case "ERROR", "ERR":
		w.logger.Error(msg)
	default:
		w.logger.Info(msg)
	}

	return len(p), nil
}
