package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/timson/pirindb/storage"
)

type redisTestError string

var expectedRedisCommands = func() []any {
	commands := make([]any, 0, len(redisCommandRegistry))
	for _, spec := range redisCommandRegistry {
		commands = append(commands, strings.ToLower(spec.Name))
	}
	return commands
}()

func setupTestRedisServer(t *testing.T) (*RedisServer, net.Conn) {
	t.Helper()

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "redis-test.db")
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
			SyncPolicy:             "strict",
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

	t.Cleanup(func() {
		_ = conn.Close()
		require.NoError(t, server.Stop())
		require.NoError(t, db.Close())
		_ = os.Remove(dbPath)
		_ = os.Remove(db.GetOptions().TxLogPath)
	})

	return server, conn
}

func writeRedisCommand(t *testing.T, writer *bufio.Writer, args ...string) {
	t.Helper()

	_, err := writer.WriteString(fmt.Sprintf("*%d\r\n", len(args)))
	require.NoError(t, err)
	for _, arg := range args {
		_, err = writer.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg))
		require.NoError(t, err)
	}
	require.NoError(t, writer.Flush())
}

func appendRedisCommand(t testing.TB, buf *bytes.Buffer, args ...string) {
	t.Helper()

	_, err := buf.WriteString(fmt.Sprintf("*%d\r\n", len(args)))
	require.NoError(t, err)
	for _, arg := range args {
		_, err = buf.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg))
		require.NoError(t, err)
	}
}

func readRedisReply(t testing.TB, reader *bufio.Reader) any {
	t.Helper()

	prefix, err := reader.ReadByte()
	require.NoError(t, err)

	switch prefix {
	case '+':
		return readRESPLine(t, reader)
	case ':':
		n, err := strconv.ParseInt(readRESPLine(t, reader), 10, 64)
		require.NoError(t, err)
		return n
	case '$':
		size, err := strconv.Atoi(readRESPLine(t, reader))
		require.NoError(t, err)
		if size == -1 {
			return nil
		}
		buf := make([]byte, size+2)
		_, err = io.ReadFull(reader, buf)
		require.NoError(t, err)
		return string(buf[:size])
	case '*':
		size, err := strconv.Atoi(readRESPLine(t, reader))
		require.NoError(t, err)
		if size == -1 {
			return nil
		}
		values := make([]any, size)
		for i := 0; i < size; i++ {
			values[i] = readRedisReply(t, reader)
		}
		return values
	case '-':
		return redisTestError(readRESPLine(t, reader))
	default:
		t.Fatalf("unexpected RESP prefix %q", prefix)
	}
	return nil
}

func readRESPLine(t testing.TB, reader *bufio.Reader) string {
	t.Helper()

	line, err := reader.ReadString('\n')
	require.NoError(t, err)
	return strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
}

func TestRedisSetGetDel(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "SET", "foo", "bar")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "GET", "foo")
	require.Equal(t, "bar", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "DEL", "foo", "missing")
	require.Equal(t, int64(1), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "GET", "foo")
	require.Nil(t, readRedisReply(t, reader))
}

func TestRedisExistsAndUnlink(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "SET", "plain", "value")
	require.Equal(t, "OK", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "LPUSH", "list", "a")
	require.Equal(t, int64(1), readRedisReply(t, reader))
	writeRedisCommand(t, writer, "HSET", "hash", "field", "value")
	require.Equal(t, int64(1), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "EXISTS", "plain", "list", "hash", "missing", "plain")
	require.Equal(t, int64(4), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "UNLINK", "plain", "list", "missing")
	require.Equal(t, int64(2), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "EXISTS", "plain", "list", "hash")
	require.Equal(t, int64(1), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "TYPE", "hash")
	require.Equal(t, "hash", readRedisReply(t, reader))
}

func TestRedisSetOptions(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "SET", "lock", "v1", "NX")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SET", "lock", "v2", "NX")
	require.Nil(t, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "GET", "lock")
	require.Equal(t, "v1", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SET", "lock", "v3", "XX")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "GET", "lock")
	require.Equal(t, "v3", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SET", "missing", "value", "XX")
	require.Nil(t, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SET", "swap", "before")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SET", "swap", "after", "GET")
	require.Equal(t, "before", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "GET", "swap")
	require.Equal(t, "after", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SET", "missing-get", "value", "XX", "GET")
	require.Nil(t, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "GET", "missing-get")
	require.Nil(t, readRedisReply(t, reader))
}

func TestRedisSetEXAndGETWrongType(t *testing.T) {
	server, conn := setupTestRedisServer(t)
	currentTime := time.Unix(1_700_000_000, 0)
	server.nowFn = func() time.Time {
		return currentTime
	}

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "SET", "temp", "value", "EX", "5")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "TTL", "temp")
	require.Equal(t, int64(5), readRedisReply(t, reader))

	currentTime = currentTime.Add(2 * time.Second)

	writeRedisCommand(t, writer, "SET", "temp", "updated", "GET", "EX", "7")
	require.Equal(t, "value", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "TTL", "temp")
	require.Equal(t, int64(7), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "LPUSH", "queue", "job")
	require.Equal(t, int64(1), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SET", "queue", "plain", "GET")
	require.Equal(t, redisTestError("WRONGTYPE Operation against a key holding the wrong kind of value"), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "TYPE", "queue")
	require.Equal(t, "list", readRedisReply(t, reader))
}

func TestRedisSetOptionValidation(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "SET", "plain", "value", "NX", "XX")
	require.Equal(t, redisTestError("ERR set condition option was already specified"), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SET", "plain", "value", "EX")
	require.Equal(t, redisTestError("ERR syntax error"), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SET", "plain", "value", "EX", "0")
	require.Equal(t, redisTestError("ERR expire time must be a positive integer"), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SET", "plain", "value", "NOPE")
	require.Equal(t, redisTestError("ERR unsupported set option 'nope'"), readRedisReply(t, reader))
}

func TestRedisSelectAndHTTPNamespaceIsolation(t *testing.T) {
	server, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	require.NoError(t, Put(server.DB, "http-only", "http-value"))

	writeRedisCommand(t, writer, "GET", "http-only")
	require.Nil(t, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SET", "redis-only", "redis-value")
	require.Equal(t, "OK", readRedisReply(t, reader))

	httpValue, found := Get(server.DB, "redis-only")
	require.False(t, found)
	require.Empty(t, httpValue)

	writeRedisCommand(t, writer, "INFO")
	info := readRedisReply(t, reader).(string)
	require.Contains(t, info, "db0:keys=1")
	require.NotContains(t, info, "db0:keys=2")
}

func TestRedisSelectSwitchesDBAndDelIsScoped(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "SET", "shared", "db0")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SELECT", "1")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "GET", "shared")
	require.Nil(t, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SET", "shared", "db1")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "GET", "shared")
	require.Equal(t, "db1", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "DEL", "shared")
	require.Equal(t, int64(1), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "GET", "shared")
	require.Nil(t, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SELECT", "0")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "GET", "shared")
	require.Equal(t, "db0", readRedisReply(t, reader))
}

func TestRedisRenamePreservesTTLAndOverwriteSemantics(t *testing.T) {
	server, conn := setupTestRedisServer(t)
	currentTime := time.Unix(1_700_000_000, 0)
	server.nowFn = func() time.Time {
		return currentTime
	}

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "SET", "source", "value")
	require.Equal(t, "OK", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "EXPIRE", "source", "5")
	require.Equal(t, int64(1), readRedisReply(t, reader))
	writeRedisCommand(t, writer, "SET", "destination", "old")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "RENAME", "source", "destination")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "GET", "source")
	require.Nil(t, readRedisReply(t, reader))
	writeRedisCommand(t, writer, "GET", "destination")
	require.Equal(t, "value", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "TTL", "destination")
	require.Equal(t, int64(5), readRedisReply(t, reader))

	currentTime = currentTime.Add(2 * time.Second)
	writeRedisCommand(t, writer, "TTL", "destination")
	require.Equal(t, int64(3), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "RENAME", "destination", "destination")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "RENAME", "missing", "other")
	require.Equal(t, redisTestError("ERR no such key"), readRedisReply(t, reader))
}

func TestRedisRenameNXAndDBSizeAreScoped(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "SET", "plain", "db0")
	require.Equal(t, "OK", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "LPUSH", "queue", "job")
	require.Equal(t, int64(1), readRedisReply(t, reader))
	writeRedisCommand(t, writer, "HSET", "hash", "field", "value")
	require.Equal(t, int64(1), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "DBSIZE")
	require.Equal(t, int64(3), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "RENAMENX", "hash", "hash2")
	require.Equal(t, int64(1), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "RENAMENX", "hash2", "queue")
	require.Equal(t, int64(0), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "TYPE", "hash2")
	require.Equal(t, "hash", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SELECT", "1")
	require.Equal(t, "OK", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "SET", "plain", "db1")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "DBSIZE")
	require.Equal(t, int64(1), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SELECT", "0")
	require.Equal(t, "OK", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "DBSIZE")
	require.Equal(t, int64(3), readRedisReply(t, reader))
}

func TestRedisSelectRejectsInvalidValuesAndPipeline(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "SELECT", "-1")
	require.Equal(t, redisTestError("ERR db index is out of range, only 0..9 supported"), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SELECT", "10")
	require.Equal(t, redisTestError("ERR db index is out of range, only 0..9 supported"), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SELECT", "wat")
	require.Equal(t, redisTestError("ERR db index must be an integer"), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "PIRIN.BATCH")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SELECT", "1")
	require.Equal(t, redisTestError("ERR command 'select' is not queueable"), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "PIRIN.DISCARD")
	require.Equal(t, "OK", readRedisReply(t, reader))
}

func TestRedisTypeCommand(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "TYPE", "missing")
	require.Equal(t, "none", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SET", "plain", "value")
	require.Equal(t, "OK", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "TYPE", "plain")
	require.Equal(t, "string", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "LPUSH", "queue", "job-1")
	require.Equal(t, int64(1), readRedisReply(t, reader))
	writeRedisCommand(t, writer, "TYPE", "queue")
	require.Equal(t, "list", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "HSET", "hash", "field", "value")
	require.Equal(t, int64(1), readRedisReply(t, reader))
	writeRedisCommand(t, writer, "TYPE", "hash")
	require.Equal(t, "hash", readRedisReply(t, reader))
}

func TestRedisTypeCommandRespectsSelectedDB(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "SET", "shared", "db0")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SELECT", "1")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "TYPE", "shared")
	require.Equal(t, "none", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "HSET", "shared", "field", "db1")
	require.Equal(t, int64(1), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "TYPE", "shared")
	require.Equal(t, "hash", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SELECT", "0")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "TYPE", "shared")
	require.Equal(t, "string", readRedisReply(t, reader))
}

func TestRedisExpireTTLAndPersist(t *testing.T) {
	server, conn := setupTestRedisServer(t)
	currentTime := time.Unix(1_700_000_000, 0)
	server.nowFn = func() time.Time {
		return currentTime
	}

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "SET", "plain", "value")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "EXPIRE", "plain", "5")
	require.Equal(t, int64(1), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "TTL", "plain")
	require.Equal(t, int64(5), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "PTTL", "plain")
	require.Equal(t, int64(5000), readRedisReply(t, reader))

	currentTime = currentTime.Add(3500 * time.Millisecond)

	writeRedisCommand(t, writer, "TTL", "plain")
	require.Equal(t, int64(1), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "PTTL", "plain")
	require.Equal(t, int64(1500), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "PERSIST", "plain")
	require.Equal(t, int64(1), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "TTL", "plain")
	require.Equal(t, int64(-1), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "PEXPIRE", "plain", "500")
	require.Equal(t, int64(1), readRedisReply(t, reader))

	currentTime = currentTime.Add(600 * time.Millisecond)

	writeRedisCommand(t, writer, "GET", "plain")
	require.Nil(t, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "TTL", "plain")
	require.Equal(t, int64(-2), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "TYPE", "plain")
	require.Equal(t, "none", readRedisReply(t, reader))
}

func TestRedisTTLForListsAndHashesPreservesOnMutation(t *testing.T) {
	server, conn := setupTestRedisServer(t)
	currentTime := time.Unix(1_700_000_000, 0)
	server.nowFn = func() time.Time {
		return currentTime
	}

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "LPUSH", "queue", "job-1")
	require.Equal(t, int64(1), readRedisReply(t, reader))
	writeRedisCommand(t, writer, "EXPIRE", "queue", "5")
	require.Equal(t, int64(1), readRedisReply(t, reader))

	currentTime = currentTime.Add(2 * time.Second)

	writeRedisCommand(t, writer, "LPUSH", "queue", "job-2")
	require.Equal(t, int64(2), readRedisReply(t, reader))
	writeRedisCommand(t, writer, "TTL", "queue")
	require.Equal(t, int64(3), readRedisReply(t, reader))

	currentTime = currentTime.Add(4 * time.Second)

	writeRedisCommand(t, writer, "LPOP", "queue")
	require.Nil(t, readRedisReply(t, reader))
	writeRedisCommand(t, writer, "TYPE", "queue")
	require.Equal(t, "none", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "HSET", "hash", "field", "value")
	require.Equal(t, int64(1), readRedisReply(t, reader))
	writeRedisCommand(t, writer, "EXPIRE", "hash", "5")
	require.Equal(t, int64(1), readRedisReply(t, reader))

	currentTime = currentTime.Add(2 * time.Second)

	writeRedisCommand(t, writer, "HSET", "hash", "field2", "value2")
	require.Equal(t, int64(1), readRedisReply(t, reader))
	writeRedisCommand(t, writer, "TTL", "hash")
	require.Equal(t, int64(3), readRedisReply(t, reader))

	currentTime = currentTime.Add(4 * time.Second)

	writeRedisCommand(t, writer, "HGET", "hash", "field")
	require.Nil(t, readRedisReply(t, reader))
	writeRedisCommand(t, writer, "TYPE", "hash")
	require.Equal(t, "none", readRedisReply(t, reader))
}

func TestRedisExpireImmediateDeleteAndDBIsolation(t *testing.T) {
	server, conn := setupTestRedisServer(t)
	currentTime := time.Unix(1_700_000_000, 0)
	server.nowFn = func() time.Time {
		return currentTime
	}

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "SET", "shared", "db0")
	require.Equal(t, "OK", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "EXPIRE", "shared", "0")
	require.Equal(t, int64(1), readRedisReply(t, reader))
	writeRedisCommand(t, writer, "GET", "shared")
	require.Nil(t, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SET", "db0-only", "value")
	require.Equal(t, "OK", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "EXPIRE", "db0-only", "1")
	require.Equal(t, int64(1), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SELECT", "1")
	require.Equal(t, "OK", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "SET", "db1-only", "value")
	require.Equal(t, "OK", readRedisReply(t, reader))

	currentTime = currentTime.Add(2 * time.Second)

	writeRedisCommand(t, writer, "GET", "db1-only")
	require.Equal(t, "value", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SELECT", "0")
	require.Equal(t, "OK", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "GET", "db0-only")
	require.Nil(t, readRedisReply(t, reader))
}

func TestRedisExpireAffectsKeysScanInfoAndPipeline(t *testing.T) {
	server, conn := setupTestRedisServer(t)
	currentTime := time.Unix(1_700_000_000, 0)
	server.nowFn = func() time.Time {
		return currentTime
	}

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "SET", "live", "value")
	require.Equal(t, "OK", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "SET", "old", "value")
	require.Equal(t, "OK", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "EXPIRE", "old", "1")
	require.Equal(t, int64(1), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "PIRIN.BATCH")
	require.Equal(t, "OK", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "SET", "pipe", "value")
	require.Equal(t, "QUEUED", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "EXPIRE", "pipe", "10")
	require.Equal(t, "QUEUED", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "TTL", "pipe")
	require.Equal(t, "QUEUED", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "PIRIN.EXEC")
	require.Equal(t, []any{"OK", int64(1), int64(10)}, readRedisReply(t, reader))

	currentTime = currentTime.Add(2 * time.Second)

	writeRedisCommand(t, writer, "KEYS", "*")
	require.Equal(t, []any{"live", "pipe"}, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SCAN", "0", "COUNT", "10")
	require.Equal(t, []any{"0", []any{"live", "pipe"}}, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "INFO")
	info := readRedisReply(t, reader).(string)
	require.Contains(t, info, "db0:keys=2")
	require.NotContains(t, info, "db0:keys=3")

	writeRedisCommand(t, writer, "EXISTS", "live", "old", "pipe")
	require.Equal(t, int64(2), readRedisReply(t, reader))
}

func TestRedisFlushAllClearsAllRedisDBsButPreservesHTTPNamespace(t *testing.T) {
	server, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	require.NoError(t, Put(server.DB, "http-only", "http-value"))

	writeRedisCommand(t, writer, "SET", "plain", "db0")
	require.Equal(t, "OK", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "LPUSH", "queue", "job-0")
	require.Equal(t, int64(1), readRedisReply(t, reader))
	writeRedisCommand(t, writer, "HSET", "hash", "field", "db0")
	require.Equal(t, int64(1), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SELECT", "1")
	require.Equal(t, "OK", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "SET", "plain", "db1")
	require.Equal(t, "OK", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "LPUSH", "queue", "job-1")
	require.Equal(t, int64(1), readRedisReply(t, reader))
	writeRedisCommand(t, writer, "HSET", "hash", "field", "db1")
	require.Equal(t, int64(1), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "FLUSHALL")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "GET", "plain")
	require.Nil(t, readRedisReply(t, reader))
	writeRedisCommand(t, writer, "TYPE", "queue")
	require.Equal(t, "none", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "TYPE", "hash")
	require.Equal(t, "none", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SELECT", "0")
	require.Equal(t, "OK", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "GET", "plain")
	require.Nil(t, readRedisReply(t, reader))
	writeRedisCommand(t, writer, "TYPE", "queue")
	require.Equal(t, "none", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "TYPE", "hash")
	require.Equal(t, "none", readRedisReply(t, reader))

	httpValue, found := Get(server.DB, "http-only")
	require.True(t, found)
	require.Equal(t, "http-value", httpValue)
}

func TestRedisFlushDBClearsOnlySelectedDB(t *testing.T) {
	server, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	require.NoError(t, Put(server.DB, "http-only", "http-value"))

	writeRedisCommand(t, writer, "SET", "plain", "db0")
	require.Equal(t, "OK", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "LPUSH", "queue", "job-0")
	require.Equal(t, int64(1), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SELECT", "1")
	require.Equal(t, "OK", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "HSET", "hash", "field", "db1")
	require.Equal(t, int64(1), readRedisReply(t, reader))
	writeRedisCommand(t, writer, "SET", "plain", "db1")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "FLUSHDB")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "DBSIZE")
	require.Equal(t, int64(0), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SELECT", "0")
	require.Equal(t, "OK", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "DBSIZE")
	require.Equal(t, int64(2), readRedisReply(t, reader))
	writeRedisCommand(t, writer, "TYPE", "queue")
	require.Equal(t, "list", readRedisReply(t, reader))

	httpValue, found := Get(server.DB, "http-only")
	require.True(t, found)
	require.Equal(t, "http-value", httpValue)
}

func TestRedisFlushAllInsidePipeline(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "SET", "plain", "value")
	require.Equal(t, "OK", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "HSET", "hash", "field", "value")
	require.Equal(t, int64(1), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "PIRIN.BATCH")
	require.Equal(t, "OK", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "FLUSHALL")
	require.Equal(t, "QUEUED", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "TYPE", "plain")
	require.Equal(t, "QUEUED", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "PIRIN.EXEC")
	require.Equal(t, []any{"OK", "none"}, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "TYPE", "hash")
	require.Equal(t, "none", readRedisReply(t, reader))
}

func TestRedisPipelineRenameFlushDBAndDBSize(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "PIRIN.BATCH")
	require.Equal(t, "OK", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "SET", "plain", "value")
	require.Equal(t, "QUEUED", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "RENAME", "plain", "renamed")
	require.Equal(t, "QUEUED", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "DBSIZE")
	require.Equal(t, "QUEUED", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "FLUSHDB")
	require.Equal(t, "QUEUED", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "DBSIZE")
	require.Equal(t, "QUEUED", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "PIRIN.EXEC")
	require.Equal(t, []any{"OK", "OK", int64(1), "OK", int64(0)}, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "GET", "renamed")
	require.Nil(t, readRedisReply(t, reader))
}

func TestRedisMSetMGet(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "MSET", "foo", "bar", "baz", "qux", "empty", "")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "MGET", "foo", "missing", "baz", "empty")
	require.Equal(t, []any{"bar", nil, "qux", ""}, readRedisReply(t, reader))
}

func TestRedisGetSetAndCounterCommands(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "INCR", "counter")
	require.Equal(t, int64(1), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "INCRBY", "counter", "4")
	require.Equal(t, int64(5), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "DECR", "counter")
	require.Equal(t, int64(4), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "DECRBY", "counter", "2")
	require.Equal(t, int64(2), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "GET", "counter")
	require.Equal(t, "2", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "GETSET", "counter", "9")
	require.Equal(t, "2", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "GET", "counter")
	require.Equal(t, "9", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "GETSET", "created", "value")
	require.Nil(t, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "GET", "created")
	require.Equal(t, "value", readRedisReply(t, reader))
}

func TestRedisGetSetAndCounterCommandErrors(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "SET", "plain", "not-a-number")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "INCR", "plain")
	require.Equal(t, redisTestError("ERR value is not an integer or out of range"), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SET", "max", strconv.FormatInt(math.MaxInt64, 10))
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "INCR", "max")
	require.Equal(t, redisTestError("ERR value is not an integer or out of range"), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "INCRBY", "plain", "nope")
	require.Equal(t, redisTestError("ERR value is not an integer or out of range"), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "LPUSH", "list", "x")
	require.Equal(t, int64(1), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "INCR", "list")
	require.Equal(t, redisTestError("WRONGTYPE Operation against a key holding the wrong kind of value"), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "HSET", "hash", "field", "value")
	require.Equal(t, int64(1), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "GETSET", "hash", "plain-now")
	require.Equal(t, redisTestError("WRONGTYPE Operation against a key holding the wrong kind of value"), readRedisReply(t, reader))
}

func TestRedisGetSetAndCounterTTLBehavior(t *testing.T) {
	server, conn := setupTestRedisServer(t)
	currentTime := time.Unix(1_700_000_000, 0)
	server.nowFn = func() time.Time {
		return currentTime
	}

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "SET", "counter", "5")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "EXPIRE", "counter", "5")
	require.Equal(t, int64(1), readRedisReply(t, reader))

	currentTime = currentTime.Add(2 * time.Second)

	writeRedisCommand(t, writer, "INCR", "counter")
	require.Equal(t, int64(6), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "TTL", "counter")
	require.Equal(t, int64(3), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SET", "swap", "before")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "EXPIRE", "swap", "5")
	require.Equal(t, int64(1), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "GETSET", "swap", "after")
	require.Equal(t, "before", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "TTL", "swap")
	require.Equal(t, int64(-1), readRedisReply(t, reader))
}

func TestRedisListCommands(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "LPUSH", "list", "a", "b")
	require.Equal(t, int64(2), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "RPUSH", "list", "c", "d")
	require.Equal(t, int64(4), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "LPOP", "list")
	require.Equal(t, "b", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "RPOP", "list")
	require.Equal(t, "d", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "LPOP", "list")
	require.Equal(t, "a", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "RPOP", "list")
	require.Equal(t, "c", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "LPOP", "list")
	require.Nil(t, readRedisReply(t, reader))
}

func TestRedisListIndexRangeAndLen(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "RPUSH", "list", "a", "b", "c", "d", "e")
	require.Equal(t, int64(5), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "LLEN", "list")
	require.Equal(t, int64(5), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "LINDEX", "list", "0")
	require.Equal(t, "a", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "LINDEX", "list", "-1")
	require.Equal(t, "e", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "LINDEX", "list", "10")
	require.Nil(t, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "LRANGE", "list", "1", "3")
	require.Equal(t, []any{"b", "c", "d"}, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "LRANGE", "list", "-3", "-1")
	require.Equal(t, []any{"c", "d", "e"}, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "LRANGE", "list", "10", "20")
	require.Equal(t, []any{}, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "LLEN", "missing")
	require.Equal(t, int64(0), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "LINDEX", "missing", "0")
	require.Nil(t, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "LRANGE", "missing", "0", "-1")
	require.Equal(t, []any{}, readRedisReply(t, reader))
}

func TestRedisListSetTrimAndRem(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "RPUSH", "list", "a", "b", "c", "d", "e")
	require.Equal(t, int64(5), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "LSET", "list", "-2", "x")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "LRANGE", "list", "0", "-1")
	require.Equal(t, []any{"a", "b", "c", "x", "e"}, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "LTRIM", "list", "1", "3")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "LRANGE", "list", "0", "-1")
	require.Equal(t, []any{"b", "c", "x"}, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "RPUSH", "rem", "a", "b", "c", "b", "b", "d")
	require.Equal(t, int64(6), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "LREM", "rem", "2", "b")
	require.Equal(t, int64(2), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "LRANGE", "rem", "0", "-1")
	require.Equal(t, []any{"a", "c", "b", "d"}, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "RPUSH", "rem", "b", "b")
	require.Equal(t, int64(6), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "LREM", "rem", "-1", "b")
	require.Equal(t, int64(1), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "LRANGE", "rem", "0", "-1")
	require.Equal(t, []any{"a", "c", "b", "d", "b"}, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "LREM", "rem", "0", "b")
	require.Equal(t, int64(2), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "LRANGE", "rem", "0", "-1")
	require.Equal(t, []any{"a", "c", "d"}, readRedisReply(t, reader))
}

func TestRedisListMutationErrorsAndTTL(t *testing.T) {
	server, conn := setupTestRedisServer(t)
	currentTime := time.Unix(1_700_000_000, 0)
	server.nowFn = func() time.Time {
		return currentTime
	}

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "LSET", "missing", "0", "value")
	require.Equal(t, redisTestError("ERR no such key"), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "RPUSH", "list", "a", "b", "c")
	require.Equal(t, int64(3), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "LSET", "list", "9", "value")
	require.Equal(t, redisTestError("ERR index out of range"), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SET", "plain", "value")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "LLEN", "plain")
	require.Equal(t, redisTestError("WRONGTYPE Operation against a key holding the wrong kind of value"), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "EXPIRE", "list", "5")
	require.Equal(t, int64(1), readRedisReply(t, reader))

	currentTime = currentTime.Add(2 * time.Second)

	writeRedisCommand(t, writer, "LSET", "list", "1", "x")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "TTL", "list")
	require.Equal(t, int64(3), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "LTRIM", "list", "1", "-1")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "TTL", "list")
	require.Equal(t, int64(3), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "LREM", "list", "0", "x")
	require.Equal(t, int64(1), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "TTL", "list")
	require.Equal(t, int64(3), readRedisReply(t, reader))
}

func TestRedisListWrongType(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "SET", "plain", "value")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "LPUSH", "plain", "x")
	require.Equal(t, redisTestError("WRONGTYPE Operation against a key holding the wrong kind of value"), readRedisReply(t, reader))
}

func TestRedisHashCommands(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "HSET", "hash", "a", "1", "b", "2")
	require.Equal(t, int64(2), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "HSET", "hash", "b", "20", "c", "3")
	require.Equal(t, int64(1), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "HGET", "hash", "b")
	require.Equal(t, "20", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "HEXISTS", "hash", "a")
	require.Equal(t, int64(1), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "HEXISTS", "hash", "missing")
	require.Equal(t, int64(0), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "HLEN", "hash")
	require.Equal(t, int64(3), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "HKEYS", "hash")
	require.Equal(t, []any{"a", "b", "c"}, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "HVALS", "hash")
	require.Equal(t, []any{"1", "20", "3"}, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "HGETALL", "hash")
	require.Equal(t, []any{"a", "1", "b", "20", "c", "3"}, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "HDEL", "hash", "a", "missing")
	require.Equal(t, int64(1), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "HDEL", "hash", "b", "c")
	require.Equal(t, int64(2), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "HGETALL", "hash")
	require.Equal(t, []any{}, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "HGET", "hash", "a")
	require.Nil(t, readRedisReply(t, reader))
}

func TestRedisListHashAndScanIsolationAcrossDBs(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "SET", "alpha:0", "zero")
	require.Equal(t, "OK", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "LPUSH", "queue", "db0-job")
	require.Equal(t, int64(1), readRedisReply(t, reader))
	writeRedisCommand(t, writer, "HSET", "hash", "field", "db0")
	require.Equal(t, int64(1), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SELECT", "1")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "KEYS", "alpha:*")
	require.Equal(t, []any{}, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SCAN", "0", "MATCH", "alpha:*", "COUNT", "10")
	require.Equal(t, []any{"0", []any{}}, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "LPOP", "queue")
	require.Nil(t, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "HGET", "hash", "field")
	require.Nil(t, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SET", "alpha:1", "one")
	require.Equal(t, "OK", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "RPUSH", "queue", "db1-job")
	require.Equal(t, int64(1), readRedisReply(t, reader))
	writeRedisCommand(t, writer, "HSET", "hash", "field", "db1")
	require.Equal(t, int64(1), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "KEYS", "alpha:*")
	require.Equal(t, []any{"alpha:1"}, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SCAN", "0", "MATCH", "alpha:*", "COUNT", "10")
	require.Equal(t, []any{"0", []any{"alpha:1"}}, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "LPOP", "queue")
	require.Equal(t, "db1-job", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "HGET", "hash", "field")
	require.Equal(t, "db1", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SELECT", "0")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "KEYS", "alpha:*")
	require.Equal(t, []any{"alpha:0"}, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "LPOP", "queue")
	require.Equal(t, "db0-job", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "HGET", "hash", "field")
	require.Equal(t, "db0", readRedisReply(t, reader))
}

func TestRedisHashLargeValue(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	value := strings.Repeat("v", 1100)
	writeRedisCommand(t, writer, "HSET", "hash", "large", value)
	require.Equal(t, int64(1), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "HGET", "hash", "large")
	require.Equal(t, value, readRedisReply(t, reader))
}

func TestRedisHashWrongTypeAndSetOverwrite(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "SET", "plain", "value")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "HGET", "plain", "field")
	require.Equal(t, redisTestError("WRONGTYPE Operation against a key holding the wrong kind of value"), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "LPUSH", "list", "x")
	require.Equal(t, int64(1), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "HSET", "list", "field", "value")
	require.Equal(t, redisTestError("WRONGTYPE Operation against a key holding the wrong kind of value"), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "HSET", "hash", "field", "value")
	require.Equal(t, int64(1), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "GET", "hash")
	require.Equal(t, redisTestError("WRONGTYPE Operation against a key holding the wrong kind of value"), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "LPUSH", "hash", "x")
	require.Equal(t, redisTestError("WRONGTYPE Operation against a key holding the wrong kind of value"), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SET", "hash", "plain-now")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "GET", "hash")
	require.Equal(t, "plain-now", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "HGET", "hash", "field")
	require.Equal(t, redisTestError("WRONGTYPE Operation against a key holding the wrong kind of value"), readRedisReply(t, reader))
}

func TestRedisDelRemovesHash(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "HSET", "hash", "field", "value")
	require.Equal(t, int64(1), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "DEL", "hash")
	require.Equal(t, int64(1), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "HGET", "hash", "field")
	require.Nil(t, readRedisReply(t, reader))
}

func TestRedisBLPopReturnsKeyAndValue(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "RPUSH", "queue", "job-1")
	require.Equal(t, int64(1), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "BLPOP", "queue", "0.1")
	require.Equal(t, []any{"queue", "job-1"}, readRedisReply(t, reader))
}

func TestRedisBRPopBlocksUntilPush(t *testing.T) {
	server, consumerConn := setupTestRedisServer(t)
	consumerReader := bufio.NewReader(consumerConn)
	consumerWriter := bufio.NewWriter(consumerConn)

	producerConn, err := net.Dial("tcp", server.listener.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = producerConn.Close()
	})
	producerReader := bufio.NewReader(producerConn)
	producerWriter := bufio.NewWriter(producerConn)

	writeRedisCommand(t, consumerWriter, "BRPOP", "queue", "1")

	require.NoError(t, consumerConn.SetReadDeadline(time.Now().Add(40*time.Millisecond)))
	_, err = consumerReader.Peek(1)
	netErr, ok := err.(net.Error)
	require.True(t, ok)
	require.True(t, netErr.Timeout())
	require.NoError(t, consumerConn.SetReadDeadline(time.Time{}))

	writeRedisCommand(t, producerWriter, "LPUSH", "queue", "job-1")
	require.Equal(t, int64(1), readRedisReply(t, producerReader))
	require.Equal(t, []any{"queue", "job-1"}, readRedisReply(t, consumerReader))
}

func TestRedisBlockingCommandsRespectSelectedDB(t *testing.T) {
	server, consumerConn := setupTestRedisServer(t)
	consumerReader := bufio.NewReader(consumerConn)
	consumerWriter := bufio.NewWriter(consumerConn)

	producerConn, err := net.Dial("tcp", server.listener.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = producerConn.Close()
	})
	producerReader := bufio.NewReader(producerConn)
	producerWriter := bufio.NewWriter(producerConn)

	writeRedisCommand(t, consumerWriter, "SELECT", "1")
	require.Equal(t, "OK", readRedisReply(t, consumerReader))
	writeRedisCommand(t, producerWriter, "SELECT", "0")
	require.Equal(t, "OK", readRedisReply(t, producerReader))

	writeRedisCommand(t, consumerWriter, "BRPOP", "queue", "1")

	writeRedisCommand(t, producerWriter, "LPUSH", "queue", "db0-job")
	require.Equal(t, int64(1), readRedisReply(t, producerReader))

	require.NoError(t, consumerConn.SetReadDeadline(time.Now().Add(40*time.Millisecond)))
	_, err = consumerReader.Peek(1)
	netErr, ok := err.(net.Error)
	require.True(t, ok)
	require.True(t, netErr.Timeout())
	require.NoError(t, consumerConn.SetReadDeadline(time.Time{}))

	writeRedisCommand(t, producerWriter, "SELECT", "1")
	require.Equal(t, "OK", readRedisReply(t, producerReader))
	writeRedisCommand(t, producerWriter, "LPUSH", "queue", "db1-job")
	require.Equal(t, int64(1), readRedisReply(t, producerReader))

	require.Equal(t, []any{"queue", "db1-job"}, readRedisReply(t, consumerReader))
}

func TestRedisRenameOfListWakesBlockingConsumers(t *testing.T) {
	server, consumerConn := setupTestRedisServer(t)
	consumerReader := bufio.NewReader(consumerConn)
	consumerWriter := bufio.NewWriter(consumerConn)

	producerConn, err := net.Dial("tcp", server.listener.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = producerConn.Close()
	})
	producerReader := bufio.NewReader(producerConn)
	producerWriter := bufio.NewWriter(producerConn)

	writeRedisCommand(t, producerWriter, "RPUSH", "queue-src", "job-1")
	require.Equal(t, int64(1), readRedisReply(t, producerReader))

	writeRedisCommand(t, consumerWriter, "BRPOP", "queue-dst", "1")

	require.NoError(t, consumerConn.SetReadDeadline(time.Now().Add(40*time.Millisecond)))
	_, err = consumerReader.Peek(1)
	netErr, ok := err.(net.Error)
	require.True(t, ok)
	require.True(t, netErr.Timeout())
	require.NoError(t, consumerConn.SetReadDeadline(time.Time{}))

	writeRedisCommand(t, producerWriter, "RENAME", "queue-src", "queue-dst")
	require.Equal(t, "OK", readRedisReply(t, producerReader))
	require.Equal(t, []any{"queue-dst", "job-1"}, readRedisReply(t, consumerReader))
}

func TestRedisBLPopTimeoutReturnsNullArray(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	start := time.Now()
	writeRedisCommand(t, writer, "BLPOP", "queue", "0.05")
	require.Nil(t, readRedisReply(t, reader))
	require.GreaterOrEqual(t, time.Since(start), 40*time.Millisecond)
}

func TestRedisRPopLPushMovesAtomically(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "RPUSH", "queue", "job-1", "job-2")
	require.Equal(t, int64(2), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "RPOPLPUSH", "queue", "processing")
	require.Equal(t, "job-2", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "RPOP", "queue")
	require.Equal(t, "job-1", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "LPOP", "processing")
	require.Equal(t, "job-2", readRedisReply(t, reader))
}

func TestRedisRPopLPushRollbackOnWrongType(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "RPUSH", "queue", "job-1")
	require.Equal(t, int64(1), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SET", "plain", "value")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "RPOPLPUSH", "queue", "plain")
	require.Equal(t, redisTestError("WRONGTYPE Operation against a key holding the wrong kind of value"), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "RPOP", "queue")
	require.Equal(t, "job-1", readRedisReply(t, reader))
}

func TestRedisBRPopLPushBlocksUntilPush(t *testing.T) {
	server, consumerConn := setupTestRedisServer(t)
	consumerReader := bufio.NewReader(consumerConn)
	consumerWriter := bufio.NewWriter(consumerConn)

	producerConn, err := net.Dial("tcp", server.listener.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = producerConn.Close()
	})
	producerReader := bufio.NewReader(producerConn)
	producerWriter := bufio.NewWriter(producerConn)

	writeRedisCommand(t, consumerWriter, "BRPOPLPUSH", "queue", "processing", "1")

	require.NoError(t, consumerConn.SetReadDeadline(time.Now().Add(40*time.Millisecond)))
	_, err = consumerReader.Peek(1)
	netErr, ok := err.(net.Error)
	require.True(t, ok)
	require.True(t, netErr.Timeout())
	require.NoError(t, consumerConn.SetReadDeadline(time.Time{}))

	writeRedisCommand(t, producerWriter, "LPUSH", "queue", "job-1")
	require.Equal(t, int64(1), readRedisReply(t, producerReader))
	require.Equal(t, "job-1", readRedisReply(t, consumerReader))

	writeRedisCommand(t, producerWriter, "LPOP", "processing")
	require.Equal(t, "job-1", readRedisReply(t, producerReader))
}

func TestRedisBRPopLPushTimeoutReturnsNullBulk(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	start := time.Now()
	writeRedisCommand(t, writer, "BRPOPLPUSH", "queue", "processing", "0.05")
	require.Nil(t, readRedisReply(t, reader))
	require.GreaterOrEqual(t, time.Since(start), 40*time.Millisecond)
}

func TestRedisKeysAndScan(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	for _, kv := range [][2]string{
		{"alpha:1", "a"},
		{"alpha:2", "b"},
		{"beta:1", "c"},
	} {
		writeRedisCommand(t, writer, "SET", kv[0], kv[1])
		require.Equal(t, "OK", readRedisReply(t, reader))
	}

	writeRedisCommand(t, writer, "KEYS", "alpha:*")
	require.Equal(t, []any{"alpha:1", "alpha:2"}, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "KEYS", "alpha:1")
	require.Equal(t, []any{"alpha:1"}, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SCAN", "0", "MATCH", "alpha:*", "COUNT", "1")
	reply := readRedisReply(t, reader).([]any)
	require.Len(t, reply, 2)
	firstCursor := reply[0].(string)
	require.NotEqual(t, "0", firstCursor)
	require.Equal(t, []any{"alpha:1"}, reply[1])

	writeRedisCommand(t, writer, "SCAN", firstCursor, "MATCH", "alpha:*", "COUNT", "1")
	reply = readRedisReply(t, reader).([]any)
	require.Equal(t, []any{"alpha:2"}, reply[1])
	if reply[0] == "0" {
		return
	}

	writeRedisCommand(t, writer, "SCAN", reply[0].(string), "MATCH", "alpha:*", "COUNT", "1")
	reply = readRedisReply(t, reader).([]any)
	require.Equal(t, "0", reply[0])
	require.Equal(t, []any{}, reply[1])
}

func TestRedisKeysAndScanIncludeListsAndHashes(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "SET", "plain", "value")
	require.Equal(t, "OK", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "LPUSH", "queue", "job-1")
	require.Equal(t, int64(1), readRedisReply(t, reader))
	writeRedisCommand(t, writer, "HSET", "hash", "field", "value")
	require.Equal(t, int64(1), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "KEYS", "*")
	require.Equal(t, []any{"hash", "plain", "queue"}, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SCAN", "0", "COUNT", "1")
	reply := readRedisReply(t, reader).([]any)
	require.Len(t, reply, 2)
	require.NotEqual(t, "0", reply[0])
	require.Equal(t, []any{"hash"}, reply[1])

	writeRedisCommand(t, writer, "SCAN", reply[0].(string), "COUNT", "1")
	reply = readRedisReply(t, reader).([]any)
	require.NotEqual(t, "0", reply[0])
	require.Equal(t, []any{"plain"}, reply[1])

	writeRedisCommand(t, writer, "SCAN", reply[0].(string), "COUNT", "1")
	reply = readRedisReply(t, reader).([]any)
	require.Equal(t, "0", reply[0])
	require.Equal(t, []any{"queue"}, reply[1])
}

func TestRedisSScanIsNotAdvertisedWithoutSetType(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	for _, key := range []string{"cache:1", "cache:2", "queue:1"} {
		writeRedisCommand(t, writer, "SET", key, "value")
		require.Equal(t, "OK", readRedisReply(t, reader))
	}

	writeRedisCommand(t, writer, "SSCAN", "ignored", "0", "MATCH", "cache:*", "COUNT", "10")
	require.Equal(t, redisTestError("ERR unsupported command 'sscan'"), readRedisReply(t, reader))
}

func TestRedisScanResumeDoesNotRepeatSimilarKeys(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	keys := []string{
		"disk:0",
		"disk:1",
		"disk:10",
		"disk:100",
		"disk:1000",
		"disk:10000",
		"disk:100000",
		"disk:1000000",
		"disk:1000001",
		"disk:1000002",
		"disk:1000003",
		"disk:1000004",
		"disk:1000005",
	}
	for _, key := range keys {
		writeRedisCommand(t, writer, "SET", key, "value")
		require.Equal(t, "OK", readRedisReply(t, reader))
	}

	writeRedisCommand(t, writer, "SCAN", "0", "COUNT", "10")
	reply := readRedisReply(t, reader).([]any)
	require.Len(t, reply, 2)
	require.NotEqual(t, "0", reply[0])
	require.Equal(t, []any{
		"disk:0",
		"disk:1",
		"disk:10",
		"disk:100",
		"disk:1000",
		"disk:10000",
		"disk:100000",
		"disk:1000000",
		"disk:1000001",
		"disk:1000002",
	}, reply[1])

	writeRedisCommand(t, writer, "SCAN", reply[0].(string), "COUNT", "10")
	reply = readRedisReply(t, reader).([]any)
	require.Equal(t, "0", reply[0])
	require.Equal(t, []any{
		"disk:1000003",
		"disk:1000004",
		"disk:1000005",
	}, reply[1])
}

func TestRedisKeysAndScanPrefixPatternFastPath(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	for _, key := range []string{
		"disk:100000",
		"disk:1000000",
		"disk:1000001",
		"disk:1000002",
		"disk:1000003",
		"disk:1000010",
		"disk:10001",
		"disk:99999",
	} {
		writeRedisCommand(t, writer, "SET", key, "value")
		require.Equal(t, "OK", readRedisReply(t, reader))
	}

	writeRedisCommand(t, writer, "KEYS", "disk:100000*")
	require.Equal(t, []any{
		"disk:100000",
		"disk:1000000",
		"disk:1000001",
		"disk:1000002",
		"disk:1000003",
	}, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SCAN", "0", "MATCH", "disk:100000*", "COUNT", "2")
	reply := readRedisReply(t, reader).([]any)
	require.NotEqual(t, "0", reply[0])
	require.Equal(t, []any{"disk:100000", "disk:1000000"}, reply[1])

	writeRedisCommand(t, writer, "SCAN", reply[0].(string), "MATCH", "disk:100000*", "COUNT", "2")
	reply = readRedisReply(t, reader).([]any)
	require.NotEqual(t, "0", reply[0])
	require.Equal(t, []any{"disk:1000001", "disk:1000002"}, reply[1])

	writeRedisCommand(t, writer, "SCAN", reply[0].(string), "MATCH", "disk:100000*", "COUNT", "2")
	reply = readRedisReply(t, reader).([]any)
	require.Equal(t, "0", reply[0])
	require.Equal(t, []any{"disk:1000003"}, reply[1])
}

func TestRedisSScanDoesNotExposeOtherRedisTypes(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "SET", "cache:string", "value")
	require.Equal(t, "OK", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "LPUSH", "cache:list", "job")
	require.Equal(t, int64(1), readRedisReply(t, reader))
	writeRedisCommand(t, writer, "HSET", "cache:hash", "field", "value")
	require.Equal(t, int64(1), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SSCAN", "ignored", "0", "MATCH", "cache:*", "COUNT", "10")
	require.Equal(t, redisTestError("ERR unsupported command 'sscan'"), readRedisReply(t, reader))
}

func TestRedisInfo(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "SET", "info:key", "value")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "INFO")
	info := readRedisReply(t, reader).(string)
	require.Contains(t, info, "# Server")
	require.Contains(t, info, "pirindb_version:")
	require.Contains(t, info, "redis_mode:standalone\r\n")
	require.Contains(t, info, "cluster_enabled:0\r\n")
	require.Contains(t, info, "db0:keys=1")
}

func TestRedisCommand(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "COMMAND")
	metadata := readRedisReply(t, reader).([]any)
	require.Len(t, metadata, len(expectedRedisCommands))
	require.Len(t, metadata[0].([]any), 10)

	writeRedisCommand(t, writer, "COMMAND", "LIST")
	require.Equal(t, expectedRedisCommands, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "COMMAND", "COUNT")
	require.Equal(t, int64(len(expectedRedisCommands)), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "COMMAND", "INFO", "GET", "SSCAN")
	info := readRedisReply(t, reader).([]any)
	require.Equal(t, "get", info[0].([]any)[0])
	require.Nil(t, info[1])
}

func TestRedisConfig(t *testing.T) {
	server, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "CONFIG", "GET", "databases")
	require.Equal(t, []any{"databases", "10"}, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "CONFIG", "GET", "pirindb-*")
	require.Equal(t, []any{
		"pirindb-sync-policy", "strict",
		"pirindb-checkpoint-tx-threshold", "64",
		"pirindb-group-commit-tx-threshold", "16",
		"pirindb-group-commit-window-ms", "1",
		"pirindb-expiry-sweep-interval-ms", "0",
		"pirindb-expiry-sweep-batch-size", "0",
		"pirindb-expiry-sweep-max-batches", "0",
		"pirindb-gc-sweep-interval-ms", "0",
		"pirindb-gc-sweep-batch-size", "0",
		"pirindb-gc-sweep-batch-bytes", "0",
		"pirindb-gc-max-batches", "0",
		"pirindb-migration-batch-size", "0",
		"pirindb-migration-batch-bytes", "0",
		"pirindb-pipeline-max-commands", "0",
		"pirindb-pipeline-max-bytes", "0",
	}, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "CONFIG", "GET", "dir")
	require.Equal(t, []any{"dir", filepath.Dir(server.Config.DB.Filename)}, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "CONFIG", "RESETSTAT")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "CONFIG", "SET", "save", "")
	require.Equal(t, redisTestError("ERR CONFIG SET is not supported"), readRedisReply(t, reader))
}

func TestRedisPipelineExecCommitsAtomically(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "PIRIN.BATCH")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SET", "pipe:a", "1")
	require.Equal(t, "QUEUED", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SET", "pipe:b", "2")
	require.Equal(t, "QUEUED", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "GET", "pipe:a")
	require.Equal(t, "QUEUED", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "PIRIN.EXEC")
	require.Equal(t, []any{"OK", "OK", "1"}, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "GET", "pipe:b")
	require.Equal(t, "2", readRedisReply(t, reader))
}

func TestRedisPipelineMSetMGet(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "PIRIN.BATCH")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "MSET", "batch:a", "1", "batch:b", "2")
	require.Equal(t, "QUEUED", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "MGET", "batch:a", "batch:b", "batch:c")
	require.Equal(t, "QUEUED", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "PIRIN.EXEC")
	require.Equal(t, []any{"OK", []any{"1", "2", nil}}, readRedisReply(t, reader))
}

func TestRedisPipelineGetSetAndCounterCommands(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "PIRIN.BATCH")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SET", "count", "1")
	require.Equal(t, "QUEUED", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "INCRBY", "count", "4")
	require.Equal(t, "QUEUED", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "GETSET", "count", "9")
	require.Equal(t, "QUEUED", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "PIRIN.EXEC")
	require.Equal(t, []any{"OK", int64(5), "5"}, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "GET", "count")
	require.Equal(t, "9", readRedisReply(t, reader))
}

func TestRedisPipelineExistsUnlinkAndSetOptions(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "PIRIN.BATCH")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SET", "lock", "a", "NX", "EX", "5")
	require.Equal(t, "QUEUED", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "EXISTS", "lock", "missing")
	require.Equal(t, "QUEUED", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SET", "lock", "b", "NX", "GET")
	require.Equal(t, "QUEUED", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "UNLINK", "lock")
	require.Equal(t, "QUEUED", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "PIRIN.EXEC")
	require.Equal(t, []any{"OK", int64(1), "a", int64(1)}, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "GET", "lock")
	require.Nil(t, readRedisReply(t, reader))
}

func TestRedisPipelineHashCommands(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "PIRIN.BATCH")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "HSET", "hash", "a", "1", "b", "2")
	require.Equal(t, "QUEUED", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "HGET", "hash", "a")
	require.Equal(t, "QUEUED", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "HLEN", "hash")
	require.Equal(t, "QUEUED", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "PIRIN.EXEC")
	require.Equal(t, []any{int64(2), "1", int64(2)}, readRedisReply(t, reader))
}

func TestRedisPipelineListCommands(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "PIRIN.BATCH")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "RPUSH", "list", "a", "b", "c", "d", "e")
	require.Equal(t, "QUEUED", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "LSET", "list", "-2", "x")
	require.Equal(t, "QUEUED", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "LTRIM", "list", "1", "3")
	require.Equal(t, "QUEUED", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "LRANGE", "list", "0", "-1")
	require.Equal(t, "QUEUED", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "LLEN", "list")
	require.Equal(t, "QUEUED", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "PIRIN.EXEC")
	require.Equal(t, []any{int64(5), "OK", "OK", []any{"b", "c", "x"}, int64(3)}, readRedisReply(t, reader))
}

func TestRedisPipelineRollbackOnError(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	oversizedKey := strings.Repeat("k", storage.MaxKeySize)

	writeRedisCommand(t, writer, "PIRIN.BATCH")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SET", "rollback:key", "value")
	require.Equal(t, "QUEUED", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SET", oversizedKey, "boom")
	require.Equal(t, "QUEUED", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "PIRIN.EXEC")
	reply := readRedisReply(t, reader)
	require.Equal(t, redisTestError("ERR PirinDB batch aborted at command 2 (set): key too large"), reply)

	writeRedisCommand(t, writer, "GET", "rollback:key")
	require.Nil(t, readRedisReply(t, reader))
}

func TestRedisPipelineHashRollbackOnWrongType(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "PIRIN.BATCH")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "HSET", "hash", "a", "1")
	require.Equal(t, "QUEUED", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "LPUSH", "hash", "x")
	require.Equal(t, "QUEUED", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "PIRIN.EXEC")
	reply := readRedisReply(t, reader)
	require.Equal(t, redisTestError("ERR PirinDB batch aborted at command 2 (lpush): WRONGTYPE Operation against a key holding the wrong kind of value"), reply)

	writeRedisCommand(t, writer, "HGET", "hash", "a")
	require.Nil(t, readRedisReply(t, reader))
}

func TestRedisMultiDiscard(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "MULTI")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SET", "discard:key", "value")
	require.Equal(t, "QUEUED", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "DISCARD")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "GET", "discard:key")
	require.Nil(t, readRedisReply(t, reader))
}

func TestRedisMultiRuntimeErrorsRemainInExecArray(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "SET", "string-key", "value")
	require.Equal(t, "OK", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "MULTI")
	require.Equal(t, "OK", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "HSET", "string-key", "field", "value")
	require.Equal(t, "QUEUED", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "SET", "after-error", "committed")
	require.Equal(t, "QUEUED", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "EXEC")
	replies := readRedisReply(t, reader).([]any)
	require.Equal(t, redisTestError("WRONGTYPE Operation against a key holding the wrong kind of value"), replies[0])
	require.Equal(t, "OK", replies[1])

	writeRedisCommand(t, writer, "GET", "after-error")
	require.Equal(t, "committed", readRedisReply(t, reader))
}

func TestRedisMultiQueueErrorAbortsExec(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "MULTI")
	require.Equal(t, "OK", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "SET", "missing-value")
	require.Equal(t, redisTestError("ERR wrong number of arguments for 'set' command"), readRedisReply(t, reader))
	writeRedisCommand(t, writer, "SET", "must-not-commit", "value")
	require.Equal(t, "QUEUED", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "EXEC")
	require.Equal(t, redisTestError("ERR EXECABORT Transaction discarded because of previous errors"), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "GET", "must-not-commit")
	require.Nil(t, readRedisReply(t, reader))
}

func TestRedisPipelineSet1000(t *testing.T) {
	const batchSize = 1000

	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	value := strings.Repeat("v", 64)

	var payload bytes.Buffer
	appendRedisCommand(t, &payload, "PIRIN.BATCH")
	for i := 0; i < batchSize; i++ {
		appendRedisCommand(t, &payload, "SET", fmt.Sprintf("pipe-bench:%04d", i), value)
	}
	appendRedisCommand(t, &payload, "PIRIN.EXEC")

	start := time.Now()
	_, err := conn.Write(payload.Bytes())
	require.NoError(t, err)

	require.Equal(t, "OK", readRedisReply(t, reader))
	for i := 0; i < batchSize; i++ {
		require.Equal(t, "QUEUED", readRedisReply(t, reader))
	}

	execReply := readRedisReply(t, reader).([]any)
	elapsed := time.Since(start)
	writesPerSecond := float64(batchSize) / elapsed.Seconds()
	t.Logf("redis pipeline write: %d SETs + EXEC, value_size=%dB, elapsed=%s, writes/sec=%.0f",
		batchSize, len(value), elapsed, writesPerSecond)

	require.Len(t, execReply, batchSize)
	require.Equal(t, "OK", execReply[0])
	require.Equal(t, "OK", execReply[len(execReply)-1])

	verifyWriter := bufio.NewWriter(conn)
	writeRedisCommand(t, verifyWriter, "GET", "pipe-bench:0000")
	require.Equal(t, value, readRedisReply(t, reader))
	writeRedisCommand(t, verifyWriter, "GET", fmt.Sprintf("pipe-bench:%04d", batchSize-1))
	require.Equal(t, value, readRedisReply(t, reader))
}

func TestRedisStandardPipelineSharesOneGroupCommit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "redis-group-pipeline.db")
	opts := storage.DefaultOptions().
		WithSyncPolicy(storage.SyncPolicyGroup).
		WithGroupCommitTxThreshold(16).
		WithGroupCommitWindow(time.Second)
	db, err := storage.Open(path, opts)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	cfg := &Config{Redis: &RedisConfig{Enabled: true, PipelineMaxCommands: 64, PipelineMaxBytes: 1024 * 1024}}
	srv := NewRedisServer(cfg, db, createLogger("ERROR"))
	serverConn, clientConn := net.Pipe()
	done := make(chan struct{})
	go func() {
		srv.handleConn(serverConn)
		close(done)
	}()
	t.Cleanup(func() {
		_ = clientConn.Close()
		<-done
	})

	var payload bytes.Buffer
	for idx := 0; idx < 16; idx++ {
		appendRedisCommand(t, &payload, "SET", fmt.Sprintf("pipeline:%02d", idx), "value")
	}
	writeDone := make(chan error, 1)
	go func() {
		_, writeErr := clientConn.Write(payload.Bytes())
		writeDone <- writeErr
	}()
	reader := bufio.NewReader(clientConn)
	for idx := 0; idx < 16; idx++ {
		require.Equal(t, "OK", readRedisReply(t, reader))
	}
	require.NoError(t, <-writeDone)
	require.Equal(t, uint64(1), db.GroupCommitBatchCount())
}
