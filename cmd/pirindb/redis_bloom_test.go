package main

import (
	"bufio"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/timson/pirindb/storage"
)

func TestRedisBloomReserveAddExists(t *testing.T) {
	server, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "BF.RESERVE", "filter", "0.01", "100")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "BF.ADD", "filter", "alice")
	require.Equal(t, int64(1), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "BF.ADD", "filter", "alice")
	require.Equal(t, int64(0), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "BF.EXISTS", "filter", "alice")
	require.Equal(t, int64(1), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "BF.EXISTS", "filter", "bob")
	require.Equal(t, int64(0), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "BF.RESERVE", "filter", "0.01", "100")
	require.Equal(t, redisTestError("ERR item exists"), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "TYPE", "filter")
	require.Equal(t, "bf", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "GET", "filter")
	require.Equal(t, redisTestError("WRONGTYPE Operation against a key holding the wrong kind of value"), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SET", "plain", "value")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "BF.ADD", "plain", "x")
	require.Equal(t, redisTestError("WRONGTYPE Operation against a key holding the wrong kind of value"), readRedisReply(t, reader))

	_ = server
}

func TestRedisBloomMAddMExistsAndAutoCreate(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "BF.MADD", "auto", "alpha", "beta", "alpha")
	require.Equal(t, []any{int64(1), int64(1), int64(0)}, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "BF.MEXISTS", "auto", "alpha", "beta", "gamma")
	require.Equal(t, []any{int64(1), int64(1), int64(0)}, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "BF.EXISTS", "missing", "x")
	require.Equal(t, int64(0), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "BF.MEXISTS", "missing", "x", "y")
	require.Equal(t, []any{int64(0), int64(0)}, readRedisReply(t, reader))
}

func TestRedisBloomKeyspaceAndTTL(t *testing.T) {
	server, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	current := time.Unix(1_700_000_000, 0)
	server.nowFn = func() time.Time {
		return current
	}

	writeRedisCommand(t, writer, "BF.ADD", "filter", "alpha")
	require.Equal(t, int64(1), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "DBSIZE")
	require.Equal(t, int64(1), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "KEYS", "*")
	require.Equal(t, []any{"filter"}, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SCAN", "0", "COUNT", "10")
	require.Equal(t, []any{"0", []any{"filter"}}, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "EXPIRE", "filter", "10")
	require.Equal(t, int64(1), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "TTL", "filter")
	require.Equal(t, int64(10), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "BF.ADD", "filter", "beta")
	require.Equal(t, int64(1), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "TTL", "filter")
	require.Equal(t, int64(10), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SET", "filter", "plain")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "TYPE", "filter")
	require.Equal(t, "string", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "TTL", "filter")
	require.Equal(t, int64(-1), readRedisReply(t, reader))
}

func TestRedisBloomDBIsolationAndNonScaling(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "BF.ADD", "shared", "alpha")
	require.Equal(t, int64(1), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SELECT", "1")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "BF.EXISTS", "shared", "alpha")
	require.Equal(t, int64(0), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "BF.MEXISTS", "shared", "alpha", "beta")
	require.Equal(t, []any{int64(0), int64(0)}, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "BF.RESERVE", "tiny", "0.000000001", "1", "NONSCALING")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "BF.ADD", "tiny", "a")
	require.Equal(t, int64(1), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "BF.ADD", "tiny", "b")
	require.Equal(t, redisTestError("ERR nonscaling bloom filter is full"), readRedisReply(t, reader))
}

func TestRedisBloomPipelineRollbackOnWrongType(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "PIPELINE")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "BF.ADD", "filter", "alpha")
	require.Equal(t, "QUEUED", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "HSET", "filter", "field", "value")
	require.Equal(t, "QUEUED", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "EXEC")
	require.Equal(t, redisTestError("ERR pipeline aborted at command 2 (hset): WRONGTYPE Operation against a key holding the wrong kind of value"), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "TYPE", "filter")
	require.Equal(t, "none", readRedisReply(t, reader))
}

func TestRedisBloomScalesAcrossSubFilters(t *testing.T) {
	server, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "BF.RESERVE", "scale", "0.000000001", "1", "EXPANSION", "2")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "BF.ADD", "scale", "a")
	require.Equal(t, int64(1), readRedisReply(t, reader))
	writeRedisCommand(t, writer, "BF.ADD", "scale", "b")
	require.Equal(t, int64(1), readRedisReply(t, reader))
	writeRedisCommand(t, writer, "BF.ADD", "scale", "c")
	require.Equal(t, int64(1), readRedisReply(t, reader))

	err := server.DB.View(func(tx *storage.Tx) error {
		meta, found, err := loadRedisBloomMetaTx(tx, redisNamespaceForDB(0), []byte("scale"))
		require.NoError(t, err)
		require.True(t, found)
		require.Greater(t, meta.SubFilterCount, uint64(1))
		return nil
	})
	require.NoError(t, err)
}

func TestRedisBloomHashPositionsDeterministic(t *testing.T) {
	subMeta, err := redisBloomBuildSubFilterMeta(100, 0.01)
	require.NoError(t, err)

	positions1 := redisBloomHashPositions([]byte("alpha"), subMeta.BitCount, subMeta.HashCount)
	positions2 := redisBloomHashPositions([]byte("alpha"), subMeta.BitCount, subMeta.HashCount)

	require.Equal(t, positions1, positions2)
	require.NotEmpty(t, positions1)
	for _, position := range positions1 {
		require.Less(t, position, subMeta.BitCount)
	}
}
