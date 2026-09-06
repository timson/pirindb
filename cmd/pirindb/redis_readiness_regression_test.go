package main

import (
	"bufio"
	"fmt"
	"math"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/timson/pirindb/storage"
)

func TestRedisExpirationArithmeticRejectsOverflowWithoutMutation(t *testing.T) {
	server, conn := setupTestRedisServer(t)
	currentTime := time.Unix(1_700_000_000, 0)
	server.nowFn = func() time.Time { return currentTime }

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	maxInt := strconv.FormatInt(math.MaxInt64, 10)

	writeRedisCommand(t, writer, "SET", "expire-overflow", "original")
	require.Equal(t, "OK", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "EXPIRE", "expire-overflow", maxInt)
	require.Equal(t, redisTestError("ERR expire time is out of range"), readRedisReply(t, reader))
	writeRedisCommand(t, writer, "GET", "expire-overflow")
	require.Equal(t, "original", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "TTL", "expire-overflow")
	require.Equal(t, int64(-1), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SET", "pexpire-overflow", "original")
	require.Equal(t, "OK", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "PEXPIRE", "pexpire-overflow", maxInt)
	require.Equal(t, redisTestError("ERR expire time is out of range"), readRedisReply(t, reader))
	writeRedisCommand(t, writer, "GET", "pexpire-overflow")
	require.Equal(t, "original", readRedisReply(t, reader))

	maxSetEX := strconv.FormatInt(math.MaxInt64/1000, 10)
	writeRedisCommand(t, writer, "SET", "set-ex-overflow", "original")
	require.Equal(t, "OK", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "SET", "set-ex-overflow", "replacement", "EX", maxSetEX)
	require.Equal(t, redisTestError("ERR expire time is out of range"), readRedisReply(t, reader))
	writeRedisCommand(t, writer, "GET", "set-ex-overflow")
	require.Equal(t, "original", readRedisReply(t, reader))
}

func TestRedisExpirationArithmeticAcceptsLargestRepresentableDeadline(t *testing.T) {
	server, conn := setupTestRedisServer(t)
	currentTime := time.Unix(1_700_000_000, 0)
	server.nowFn = func() time.Time { return currentTime }
	nowMs := currentTime.UnixMilli()

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	maxSeconds := (math.MaxInt64 - nowMs) / 1000
	writeRedisCommand(t, writer, "SET", "max-valid-ex", "value", "EX", strconv.FormatInt(maxSeconds, 10))
	require.Equal(t, "OK", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "EXISTS", "max-valid-ex")
	require.Equal(t, int64(1), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SET", "max-valid-pexpire", "value")
	require.Equal(t, "OK", readRedisReply(t, reader))
	maxMilliseconds := math.MaxInt64 - nowMs
	writeRedisCommand(t, writer, "PEXPIRE", "max-valid-pexpire", strconv.FormatInt(maxMilliseconds, 10))
	require.Equal(t, int64(1), readRedisReply(t, reader))
	writeRedisCommand(t, writer, "EXISTS", "max-valid-pexpire")
	require.Equal(t, int64(1), readRedisReply(t, reader))
}

func TestRedisSameKeyListMovePreservesTTL(t *testing.T) {
	server, conn := setupTestRedisServer(t)
	currentTime := time.Unix(1_700_000_000, 0)
	server.nowFn = func() time.Time { return currentTime }

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "RPUSH", "rotate", "only")
	require.Equal(t, int64(1), readRedisReply(t, reader))
	writeRedisCommand(t, writer, "EXPIRE", "rotate", "100")
	require.Equal(t, int64(1), readRedisReply(t, reader))
	writeRedisCommand(t, writer, "RPOPLPUSH", "rotate", "rotate")
	require.Equal(t, "only", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "TTL", "rotate")
	require.Equal(t, int64(100), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "BRPOPLPUSH", "rotate", "rotate", "1")
	require.Equal(t, "only", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "TTL", "rotate")
	require.Equal(t, int64(100), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "RPUSH", "rotate", "second", "third")
	require.Equal(t, int64(3), readRedisReply(t, reader))
	writeRedisCommand(t, writer, "RPOPLPUSH", "rotate", "rotate")
	require.Equal(t, "third", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "LRANGE", "rotate", "0", "-1")
	require.Equal(t, []any{"third", "only", "second"}, readRedisReply(t, reader))
	writeRedisCommand(t, writer, "TTL", "rotate")
	require.Equal(t, int64(100), readRedisReply(t, reader))
}

func TestRedisExpirySweepIsBoundedAndInfoIsReadOnly(t *testing.T) {
	server, conn := setupTestRedisServer(t)
	currentTime := time.Unix(1_700_000_000, 0)
	server.nowFn = func() time.Time { return currentTime }

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	for _, key := range []string{"expired-a", "expired-b", "expired-c"} {
		writeRedisCommand(t, writer, "SET", key, "value", "EX", "1")
		require.Equal(t, "OK", readRedisReply(t, reader))
	}
	currentTime = currentTime.Add(2 * time.Second)

	writeRedisCommand(t, writer, "DBSIZE")
	require.Equal(t, int64(0), readRedisReply(t, reader))
	writeRedisCommand(t, writer, "INFO")
	info := readRedisReply(t, reader).(string)
	require.NotContains(t, info, "db0:keys=")

	physicalCount := func() int64 {
		var count int64
		require.NoError(t, server.DB.View(func(tx *storage.Tx) error {
			var err error
			count, err = countRedisDBKeysFastTx(tx, server.redisNamespace(0))
			return err
		}))
		return count
	}
	require.Equal(t, int64(3), physicalCount(), "INFO must not physically purge expired keys")

	settings := redisExpiryWorkerSettings{batchSize: 1, maxBatchesPerCycle: 1}
	require.NoError(t, server.runExpirySweepCycle(currentTime.UnixMilli(), settings))
	require.Equal(t, int64(2), physicalCount())
	require.NoError(t, server.runExpirySweepCycle(currentTime.UnixMilli(), settings))
	require.Equal(t, int64(1), physicalCount())
	require.NoError(t, server.runExpirySweepCycle(currentTime.UnixMilli(), settings))
	require.Equal(t, int64(0), physicalCount())

	stats := server.expiryWorkerStats()
	require.Equal(t, uint64(3), stats.KeysRemoved)
	require.Equal(t, uint64(3), stats.Batches)
}

func TestRedisActiveExpiryWorkerReclaimsWithoutKeyspaceCommands(t *testing.T) {
	server, conn := setupTestRedisServer(t)
	var currentMillis atomic.Int64
	currentMillis.Store(time.Unix(1_700_000_000, 0).UnixMilli())
	server.nowFn = func() time.Time { return time.UnixMilli(currentMillis.Load()) }
	server.Config.Redis.ExpirySweepIntervalMs = 5
	server.Config.Redis.ExpirySweepBatchSize = 1
	server.Config.Redis.ExpirySweepMaxBatchesPerCycle = 2
	server.wg.Add(1)
	go server.expiryLoop()

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	for _, key := range []string{"worker-a", "worker-b", "worker-c"} {
		writeRedisCommand(t, writer, "SET", key, "value", "EX", "1")
		require.Equal(t, "OK", readRedisReply(t, reader))
	}
	currentMillis.Add((2 * time.Second).Milliseconds())
	server.wakeExpiryWorker()

	require.Eventually(t, func() bool {
		var count int64
		err := server.DB.View(func(tx *storage.Tx) error {
			var innerErr error
			count, innerErr = countRedisDBKeysFastTx(tx, server.redisNamespace(0))
			return innerErr
		})
		return err == nil && count == 0 && server.expiryWorkerStats().KeysRemoved >= 3
	}, time.Second, 5*time.Millisecond)

	stats := server.expiryWorkerStats()
	require.GreaterOrEqual(t, stats.KeysRemoved, uint64(3))
}

func TestRedisListSegmentLocalOperationsAcrossBoundaries(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	args := make([]string, 0, 202)
	args = append(args, "RPUSH", "segmented")
	for i := 0; i < 200; i++ {
		args = append(args, fmt.Sprintf("v%03d", i))
	}
	writeRedisCommand(t, writer, args...)
	require.Equal(t, int64(200), readRedisReply(t, reader))

	for _, testCase := range []struct {
		index string
		want  string
	}{{"0", "v000"}, {"63", "v063"}, {"64", "v064"}, {"-1", "v199"}} {
		writeRedisCommand(t, writer, "LINDEX", "segmented", testCase.index)
		require.Equal(t, testCase.want, readRedisReply(t, reader))
	}

	large := strings.Repeat("x", redisListTargetSegmentPayloadSize+100)
	writeRedisCommand(t, writer, "LSET", "segmented", "64", large)
	require.Equal(t, "OK", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "LRANGE", "segmented", "63", "65")
	require.Equal(t, []any{"v063", large, "v065"}, readRedisReply(t, reader))
	writeRedisCommand(t, writer, "LLEN", "segmented")
	require.Equal(t, int64(200), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "LTRIM", "segmented", "50", "150")
	require.Equal(t, "OK", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "LLEN", "segmented")
	require.Equal(t, int64(101), readRedisReply(t, reader))
	writeRedisCommand(t, writer, "LRANGE", "segmented", "0", "2")
	require.Equal(t, []any{"v050", "v051", "v052"}, readRedisReply(t, reader))
	writeRedisCommand(t, writer, "LRANGE", "segmented", "-3", "-1")
	require.Equal(t, []any{"v148", "v149", "v150"}, readRedisReply(t, reader))
}

func TestRedisCommandSizePreflightRejectsBeforeMutation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "preflight.db")
	db, err := storage.Open(path, storage.DefaultOptions().WithMaxTransactionBytes(32*1024))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	srv := NewRedisServer(&Config{Redis: &RedisConfig{}}, db, createLogger("ERROR"))
	_, err = srv.executeCommand(0, [][]byte{[]byte("SET"), []byte("too-large"), []byte(strings.Repeat("x", 24*1024))}, nil, 1000, false)
	require.ErrorIs(t, err, errRedisCommandExceedsTransactionLimit)
	require.NoError(t, db.View(func(tx *storage.Tx) error {
		keyType, err := redisRawKeyTypeTx(tx, redisNamespaceForDB(0), []byte("too-large"))
		require.NoError(t, err)
		require.Equal(t, redisKeyTypeNone, keyType)
		return nil
	}))
}
