package storage

import (
	"encoding/binary"
	"github.com/phsym/console-slog"
	"log/slog"
	"os"
	"sync/atomic"
)

type atomicLogger struct {
	value atomic.Pointer[slog.Logger]
}

func newAtomicLogger(initial *slog.Logger) *atomicLogger {
	logger := &atomicLogger{}
	logger.value.Store(initial)
	return logger
}

func (l *atomicLogger) current() *slog.Logger {
	if logger := l.value.Load(); logger != nil {
		return logger
	}
	return slog.Default()
}

func (l *atomicLogger) Debug(msg string, args ...any) { l.current().Debug(msg, args...) }
func (l *atomicLogger) Info(msg string, args ...any)  { l.current().Info(msg, args...) }
func (l *atomicLogger) Warn(msg string, args ...any)  { l.current().Warn(msg, args...) }
func (l *atomicLogger) Error(msg string, args ...any) { l.current().Error(msg, args...) }

var logger = newAtomicLogger(slog.New(
	console.NewHandler(os.Stderr, &console.HandlerOptions{Level: slog.LevelWarn}),
))

func SetLogger(l *slog.Logger) {
	if l == nil {
		l = slog.Default()
	}
	logger.value.Store(l)
}

func itob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}
