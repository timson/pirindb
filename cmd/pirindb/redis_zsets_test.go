package main

import (
	"bufio"
	"bytes"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/timson/pirindb/storage"
)

func TestRedisZSetCommands(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "ZADD", "leaders", "2", "alice", "1", "bob", "2", "carol")
	require.Equal(t, int64(3), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "ZADD", "leaders", "4", "alice", "2", "carol")
	require.Equal(t, int64(0), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "ZCARD", "leaders")
	require.Equal(t, int64(3), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "ZSCORE", "leaders", "alice")
	require.Equal(t, "4", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "ZRANGEBYSCORE", "leaders", "2", "4")
	require.Equal(t, []any{"carol", "alice"}, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "ZREVRANGEBYSCORE", "leaders", "4", "2")
	require.Equal(t, []any{"alice", "carol"}, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "TYPE", "leaders")
	require.Equal(t, "zset", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "GET", "leaders")
	require.Equal(t, redisTestError("WRONGTYPE Operation against a key holding the wrong kind of value"), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "ZREM", "leaders", "bob", "missing")
	require.Equal(t, int64(1), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "ZRANGEBYSCORE", "leaders", "-inf", "+inf")
	require.Equal(t, []any{"carol", "alice"}, readRedisReply(t, reader))
}

func TestRedisZSetLexAndRangeRemovals(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "ZADD", "lex", "0", "ant", "0", "bee", "0", "cat", "0", "dog", "0", "eel")
	require.Equal(t, int64(5), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "ZRANGEBYLEX", "lex", "[bee", "[dog", "LIMIT", "1", "2")
	require.Equal(t, []any{"cat", "dog"}, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "ZREVRANGEBYLEX", "lex", "[dog", "[bee", "LIMIT", "0", "2")
	require.Equal(t, []any{"dog", "cat"}, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "ZREMRANGEBYLEX", "lex", "[bee", "[cat")
	require.Equal(t, int64(2), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "ZRANGEBYLEX", "lex", "-", "+")
	require.Equal(t, []any{"ant", "dog", "eel"}, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "ZADD", "scores", "1", "a", "2", "b", "2", "c", "3", "d", "4", "e")
	require.Equal(t, int64(5), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "ZREMRANGEBYSCORE", "scores", "(1", "3")
	require.Equal(t, int64(3), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "ZRANGEBYSCORE", "scores", "-inf", "+inf")
	require.Equal(t, []any{"a", "e"}, readRedisReply(t, reader))
}

func TestRedisZSetTTLAndKeyspaceIntegration(t *testing.T) {
	server, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	current := time.Unix(1_700_000_000, 0)
	server.nowFn = func() time.Time {
		return current
	}

	writeRedisCommand(t, writer, "SET", "plain", "value")
	require.Equal(t, "OK", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "LPUSH", "queue", "job")
	require.Equal(t, int64(1), readRedisReply(t, reader))
	writeRedisCommand(t, writer, "HSET", "hash", "field", "value")
	require.Equal(t, int64(1), readRedisReply(t, reader))
	writeRedisCommand(t, writer, "ZADD", "scores", "1", "alice")
	require.Equal(t, int64(1), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "DBSIZE")
	require.Equal(t, int64(4), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "KEYS", "*")
	require.Equal(t, []any{"hash", "plain", "queue", "scores"}, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SCAN", "0", "COUNT", "10")
	require.Equal(t, []any{"0", []any{"hash", "plain", "queue", "scores"}}, readRedisReply(t, reader))

	writeRedisCommand(t, writer, "EXPIRE", "scores", "10")
	require.Equal(t, int64(1), readRedisReply(t, reader))
	writeRedisCommand(t, writer, "TTL", "scores")
	require.Equal(t, int64(10), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "ZADD", "scores", "2", "bob")
	require.Equal(t, int64(1), readRedisReply(t, reader))
	writeRedisCommand(t, writer, "TTL", "scores")
	require.Equal(t, int64(10), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "SET", "scores", "plain")
	require.Equal(t, "OK", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "TTL", "scores")
	require.Equal(t, int64(-1), readRedisReply(t, reader))
	writeRedisCommand(t, writer, "TYPE", "scores")
	require.Equal(t, "string", readRedisReply(t, reader))
}

func TestRedisZSetPipelineRollbackOnWrongType(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "PIPELINE")
	require.Equal(t, "OK", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "ZADD", "scores", "1", "alice")
	require.Equal(t, "QUEUED", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "HSET", "scores", "field", "value")
	require.Equal(t, "QUEUED", readRedisReply(t, reader))

	writeRedisCommand(t, writer, "EXEC")
	require.Equal(t, redisTestError("ERR pipeline aborted at command 2 (hset): WRONGTYPE Operation against a key holding the wrong kind of value"), readRedisReply(t, reader))

	writeRedisCommand(t, writer, "TYPE", "scores")
	require.Equal(t, "none", readRedisReply(t, reader))
}

func TestRedisZSetSortableScoreOrder(t *testing.T) {
	scores := []float64{
		math.Inf(-1),
		-123.5,
		-1,
		-0,
		0,
		0.25,
		42,
		math.Inf(1),
	}

	encoded := make([][]byte, 0, len(scores))
	for _, score := range scores {
		encoded = append(encoded, encodeRedisZSetSortableScore(score))
	}

	for i := 1; i < len(encoded); i++ {
		require.LessOrEqual(t, bytes.Compare(encoded[i-1], encoded[i]), 0)
	}

	decodedZero, err := decodeRedisZSetSortableScore(encodeRedisZSetSortableScore(-0))
	require.NoError(t, err)
	require.Equal(t, float64(0), decodedZero)
}

func TestRedisZSetUpdateRemovesStaleScoreIndex(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "redis-zset.db")

	db, err := storage.Open(dbPath, storage.DefaultOptions())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
		_ = os.Remove(dbPath)
		_ = os.Remove(db.GetOptions().TxLogPath)
	})

	ns := redisNamespaceForDB(0)
	key := []byte("scores")
	member := []byte("alice")
	nowMs := time.Now().UnixMilli()

	err = db.Update(func(tx *storage.Tx) error {
		_, err := redisZSetAddTx(tx, ns, key, []redisZSetScoreMemberPair{{score: 1, member: member}}, nowMs)
		return err
	})
	require.NoError(t, err)

	err = db.Update(func(tx *storage.Tx) error {
		_, err := redisZSetAddTx(tx, ns, key, []redisZSetScoreMemberPair{{score: 5, member: member}}, nowMs)
		return err
	})
	require.NoError(t, err)

	err = db.View(func(tx *storage.Tx) error {
		memberBucket, scoreBucket, meta, found, err := getRedisZSetBucketsTx(tx, ns, key, nowMs)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, uint64(1), meta.Cardinality)

		encodedScore, exists := memberBucket.Get(member)
		require.True(t, exists)
		require.Equal(t, encodeRedisZSetSortableScore(5), encodedScore)

		_, oldExists := scoreBucket.Get(redisZSetScoreIndexKey(1, member))
		require.False(t, oldExists)

		_, newExists := scoreBucket.Get(redisZSetScoreIndexKey(5, member))
		require.True(t, newExists)
		return nil
	})
	require.NoError(t, err)
}
