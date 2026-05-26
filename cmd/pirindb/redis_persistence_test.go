package main

import (
	"bufio"
	"net"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/timson/pirindb/storage"
)

func startPersistentRedisTestServer(t *testing.T, dbPath string, syncPolicy string) (*RedisServer, *storage.DB, net.Conn) {
	t.Helper()

	cfg := &Config{
		Server: &ServerConfig{
			Host:     "127.0.0.1",
			Port:     0,
			LogLevel: "ERROR",
		},
		Redis: &RedisConfig{
			Enabled: true,
			Host:    "127.0.0.1",
			Port:    0,
		},
		DB: &DatabaseConfig{
			Filename:               dbPath,
			SyncPolicy:             syncPolicy,
			CheckpointTxThreshold:  64,
			GroupCommitTxThreshold: 16,
			GroupCommitWindowMs:    1,
		},
	}

	logger := createLogger("ERROR")
	storage.SetLogger(logger)

	db, err := storage.Open(dbPath, cfg.DB.StorageOptions())
	require.NoError(t, err)

	server := NewRedisServer(cfg, db, logger)
	require.NoError(t, server.Start())

	conn, err := net.Dial("tcp", server.listener.Addr().String())
	require.NoError(t, err)

	return server, db, conn
}

func stopPersistentRedisTestServer(t *testing.T, server *RedisServer, db *storage.DB, conn net.Conn) {
	t.Helper()
	if conn != nil {
		_ = conn.Close()
	}
	require.NoError(t, server.Stop())
	require.NoError(t, db.Close())
}

func TestRedisGroupPolicyPersistsSetAcrossRestart(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "group-persist.db")

	server, db, conn := startPersistentRedisTestServer(t, dbPath, "group")
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "SELECT", "0")
	require.Equal(t, "OK", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "SET", "persist:key", "value")
	require.Equal(t, "OK", readRedisReply(t, reader))

	stopPersistentRedisTestServer(t, server, db, conn)

	server, db, conn = startPersistentRedisTestServer(t, dbPath, "group")
	defer stopPersistentRedisTestServer(t, server, db, conn)

	reader = bufio.NewReader(conn)
	writer = bufio.NewWriter(conn)
	writeRedisCommand(t, writer, "SELECT", "0")
	require.Equal(t, "OK", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "GET", "persist:key")
	require.Equal(t, "value", readRedisReply(t, reader))
}

func TestRedisGroupPolicyPersistsFlushDBAcrossRestart(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "group-flush.db")

	server, db, conn := startPersistentRedisTestServer(t, dbPath, "group")
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "SELECT", "0")
	require.Equal(t, "OK", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "SET", "persist:key", "value")
	require.Equal(t, "OK", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "FLUSHDB")
	require.Equal(t, "OK", readRedisReply(t, reader))

	stopPersistentRedisTestServer(t, server, db, conn)

	server, db, conn = startPersistentRedisTestServer(t, dbPath, "group")
	defer stopPersistentRedisTestServer(t, server, db, conn)

	reader = bufio.NewReader(conn)
	writer = bufio.NewWriter(conn)
	writeRedisCommand(t, writer, "SELECT", "0")
	require.Equal(t, "OK", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "KEYS", "*")
	require.Equal(t, []any{}, readRedisReply(t, reader))
}
