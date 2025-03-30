package storage

import (
	"encoding/binary"
	"github.com/phsym/console-slog"
	"os"
)

import "log/slog"

var logger = slog.New(
	console.NewHandler(os.Stderr, &console.HandlerOptions{Level: slog.LevelWarn}),
)

func SetLogger(l *slog.Logger) {
	logger = l
}

func itob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func btoi(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}
