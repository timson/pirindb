package main

import (
	"bufio"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRedisTopKReserveAddIncrByListInfoAndType(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "TOPK.RESERVE", "heavy", "2", "2000", "7", "0.9")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "TOPK.INFO", "heavy")
	require.Equal(t, []any{"k", int64(2), "width", int64(2000), "depth", int64(7), "decay", "0.9"}, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "TOPK.ADD", "heavy", "alpha", "beta", "alpha", "gamma", "gamma", "gamma")
	require.Equal(t, []any{nil, nil, nil, nil, "beta", nil}, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "TOPK.LIST", "heavy")
	require.Equal(t, []any{"gamma", "alpha"}, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "TOPK.LIST", "heavy", "WITHCOUNT")
	require.Equal(t, []any{"gamma", int64(3), "alpha", int64(2)}, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "TOPK.QUERY", "heavy", "alpha", "beta", "gamma")
	require.Equal(t, []any{int64(1), int64(0), int64(1)}, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "TOPK.COUNT", "heavy", "alpha", "beta", "gamma")
	require.Equal(t, []any{int64(2), int64(1), int64(3)}, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "TOPK.INCRBY", "heavy", "delta", "4")
	require.Equal(t, []any{"alpha"}, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "TOPK.LIST", "heavy", "WITHCOUNT")
	require.Equal(t, []any{"delta", int64(4), "gamma", int64(3)}, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "TYPE", "heavy")
	require.Equal(t, "topk", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "GET", "heavy")
	require.Equal(t, redisTestError("WRONGTYPE Operation against a key holding the wrong kind of value"), readRedisReply(t, reader))
}

func TestRedisTopKTTLKeyspaceAndOverwrite(t *testing.T) {
	server, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	current := time.Unix(1_700_000_000, 0)
	server.nowFn = func() time.Time {
		return current
	}

	writeRedisCommand(t, writer, "TOPK.RESERVE", "tk", "3")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "TOPK.ADD", "tk", "alpha")
	require.Equal(t, []any{nil}, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "DBSIZE")
	require.Equal(t, int64(1), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "KEYS", "*")
	require.Equal(t, []any{"tk"}, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SCAN", "0", "COUNT", "10")
	require.Equal(t, []any{"0", []any{"tk"}}, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "EXPIRE", "tk", "10")
	require.Equal(t, int64(1), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "TTL", "tk")
	require.Equal(t, int64(10), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "TOPK.ADD", "tk", "beta")
	require.Equal(t, []any{nil}, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "TTL", "tk")
	require.Equal(t, int64(10), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SET", "tk", "plain")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "TYPE", "tk")
	require.Equal(t, "string", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "TTL", "tk")
	require.Equal(t, int64(-1), readRedisReply(t, reader))
}

func TestRedisTopKDBIsolationAndMissingKeyErrors(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "TOPK.RESERVE", "shared", "2")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "TOPK.ADD", "shared", "alpha")
	require.Equal(t, []any{nil}, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SELECT", "1")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "TOPK.QUERY", "shared", "alpha")
	require.Equal(t, redisTestError("ERR no such key"), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "TOPK.ADD", "missing", "alpha")
	require.Equal(t, redisTestError("ERR no such key"), readRedisReply(t, reader))
}

func TestRedisTopKPipelineRollbackOnWrongType(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "TOPK.RESERVE", "heavy", "2")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "PIPELINE")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "TOPK.ADD", "heavy", "alpha")
	require.Equal(t, "QUEUED", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "HSET", "heavy", "field", "value")
	require.Equal(t, "QUEUED", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "EXEC")
	require.Equal(t, redisTestError("ERR pipeline aborted at command 2 (hset): WRONGTYPE Operation against a key holding the wrong kind of value"), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "TOPK.LIST", "heavy")
	require.Equal(t, []any{}, readRedisReply(t, reader))
}

func TestRedisTopKHashingDeterministic(t *testing.T) {
	fingerprint1, columns1 := redisTopKFingerprintAndColumns([]byte("alpha"), 1024, 7)
	fingerprint2, columns2 := redisTopKFingerprintAndColumns([]byte("alpha"), 1024, 7)

	require.Equal(t, fingerprint1, fingerprint2)
	require.Equal(t, columns1, columns2)
	require.NotZero(t, fingerprint1)
	require.Len(t, columns1, 7)
	for _, column := range columns1 {
		require.Less(t, column, uint64(1024))
	}
}
