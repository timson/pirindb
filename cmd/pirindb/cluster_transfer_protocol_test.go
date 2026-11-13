package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/timson/pirindb/storage"
)

func TestClusterTransferProtocolRejectsCorruptionOversizeAndOutOfOrder(t *testing.T) {
	tmpDir := t.TempDir()
	source := openTestClusterServer(t, newTestClusterConfig(t, "a", filepath.Join(tmpDir, "source.db")))
	destination := openTestClusterServer(t, newTestClusterConfig(t, "b", filepath.Join(tmpDir, "destination.db")))
	state := buildClusterStateForTests(source.Cluster.Snapshot().SlotCount, "http://source", "http://destination")
	require.NoError(t, source.Cluster.ReplaceState(state))
	require.NoError(t, destination.Cluster.ReplaceState(state))

	key := findKeyForOwner(state, "a")
	slot := ClusterKeySlot(key, state.SlotCount)
	nowMs := redisNowUnixMilli(time.Now())
	require.NoError(t, PutRedisBytes(source.DB, redisNamespaceForDB(0, state.SlotCount), key, []byte("protocol-value"), nowMs))

	var valid bytes.Buffer
	require.NoError(t, streamClusterSlotRange(source.DB, &valid, state.SlotCount, slot, slot, nowMs))
	require.Greater(t, valid.Len(), 38)

	corrupted := cloneBytes(valid.Bytes())
	corrupted[38] ^= 0xff
	_, err := importClusterSlotStream(destination.DB, bytes.NewReader(corrupted), nowMs)
	require.ErrorContains(t, err, "checksum")
	value, found, err := GetRedisBytes(destination.DB, redisNamespaceForDB(0, state.SlotCount), key, nowMs)
	require.NoError(t, err)
	require.False(t, found)
	require.Nil(t, value)

	outOfOrder := cloneBytes(valid.Bytes())
	binary.LittleEndian.PutUint64(outOfOrder[22:30], 2)
	_, err = importClusterSlotStream(destination.DB, bytes.NewReader(outOfOrder), nowMs)
	require.ErrorContains(t, err, "expected 1")

	var oversized bytes.Buffer
	require.NoError(t, writeClusterSlotStreamHeader(&oversized, clusterSlotStreamHeader{
		Version: clusterTransferProtocolVersion, SlotCount: state.SlotCount, StartSlot: slot, EndSlot: slot,
	}))
	require.NoError(t, binary.Write(&oversized, binary.LittleEndian, uint64(1)))
	require.NoError(t, binary.Write(&oversized, binary.LittleEndian, uint32(clusterTransferMaxFramePayload+1)))
	require.NoError(t, binary.Write(&oversized, binary.LittleEndian, uint32(0)))
	_, err = importClusterSlotStream(destination.DB, bytes.NewReader(oversized.Bytes()), nowMs)
	require.ErrorContains(t, err, "length is invalid")
}

func TestClusterTransferReceiptsMakeRetryIdempotent(t *testing.T) {
	tmpDir := t.TempDir()
	source := openTestClusterServer(t, newTestClusterConfig(t, "a", filepath.Join(tmpDir, "source.db")))
	destinationConfig := newTestClusterConfig(t, "b", filepath.Join(tmpDir, "destination.db"))
	destination := openTestClusterServer(t, destinationConfig)
	state := buildClusterStateForTests(source.Cluster.Snapshot().SlotCount, "http://source", "http://destination")
	require.NoError(t, source.Cluster.ReplaceState(state))
	require.NoError(t, destination.Cluster.ReplaceState(state))
	key := findKeyForOwner(state, "a")
	slot := ClusterKeySlot(key, state.SlotCount)
	nowMs := redisNowUnixMilli(time.Now())
	require.NoError(t, PutRedisBytes(source.DB, redisNamespaceForDB(0, state.SlotCount), key, []byte("once"), nowMs))

	identity := clusterTransferIdentity{ClusterID: state.ClusterID, MoveID: "move-1", SourceNodeID: "a", DestinationNodeID: "b", Epoch: state.Epoch}
	var stream bytes.Buffer
	_, err := streamClusterSlotRangeChunkWithIdentity(source.DB, &stream, state.SlotCount, slot, slot, nowMs, redisDatabaseMin, nil, 1, identity)
	require.NoError(t, err)
	receipt := clusterImportReceiptContext{JobID: "move-1", ChunkIndex: 1}
	first, err := importClusterSlotStreamWithReceiptAndIdentity(destination.DB, bytes.NewReader(stream.Bytes()), nowMs, receipt, identity)
	require.NoError(t, err)
	require.Equal(t, uint64(1), first.EntriesTransferred)
	require.NoError(t, destination.DB.Close())
	restartedDestination := openTestClusterServer(t, destinationConfig)
	require.NoError(t, restartedDestination.Cluster.ReplaceState(state))
	second, err := importClusterSlotStreamWithReceiptAndIdentity(restartedDestination.DB, bytes.NewReader(stream.Bytes()), nowMs, receipt, identity)
	require.NoError(t, err)
	require.Equal(t, uint64(0), second.EntriesTransferred)

	conflictingIdentity := identity
	conflictingIdentity.ClusterID = "different-cluster"
	var conflicting bytes.Buffer
	_, err = streamClusterSlotRangeChunkWithIdentity(source.DB, &conflicting, state.SlotCount, slot, slot, nowMs, redisDatabaseMin, nil, 1, conflictingIdentity)
	require.NoError(t, err)
	_, err = importClusterSlotStreamWithReceipt(restartedDestination.DB, bytes.NewReader(conflicting.Bytes()), nowMs, receipt)
	require.ErrorContains(t, err, "checksum does not match")
}

func TestClusterTransferRecoversDroppedResponseAfterDestinationCommit(t *testing.T) {
	tmpDir := t.TempDir()
	source := openTestClusterServer(t, newTestClusterConfig(t, "a", filepath.Join(tmpDir, "source.db")))
	destination := openTestClusterServer(t, newTestClusterConfig(t, "b", filepath.Join(tmpDir, "destination.db")))
	destinationRouter := destination.buildRouter()
	destinationHTTP := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/cluster/internal/slot-import" {
			recorder := httptest.NewRecorder()
			destinationRouter.ServeHTTP(recorder, r)
			panic(http.ErrAbortHandler)
		}
		destinationRouter.ServeHTTP(w, r)
	}))
	t.Cleanup(destinationHTTP.Close)
	state := buildClusterStateForTests(source.Cluster.Snapshot().SlotCount, "http://source", destinationHTTP.URL)
	require.NoError(t, source.Cluster.ReplaceState(state))
	require.NoError(t, destination.Cluster.ReplaceState(state))
	key := findKeyForOwner(state, "a")
	slot := ClusterKeySlot(key, state.SlotCount)
	nowMs := redisNowUnixMilli(time.Now())
	require.NoError(t, PutRedisBytes(source.DB, redisNamespaceForDB(0, state.SlotCount), key, []byte("committed-before-drop"), nowMs))

	stats, err := source.postClusterSlotImportChunk(
		context.Background(), state.Nodes[1], "dropped-response", 1,
		state.SlotCount, slot, slot, nowMs, redisDatabaseMin, nil, 1,
	)
	require.NoError(t, err)
	require.Greater(t, stats.RecordsTransferred, uint64(0))
	require.Equal(t, uint64(1), stats.Retries)
	require.Equal(t, uint64(1), stats.EntriesTransferred)
	value, found, err := GetRedisBytes(destination.DB, redisNamespaceForDB(0, state.SlotCount), key, nowMs)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "committed-before-drop", string(value))
}

func TestClusterTransferDoesNotPublishPartialLargeString(t *testing.T) {
	tmpDir := t.TempDir()
	source := openTestClusterServer(t, newTestClusterConfig(t, "a", filepath.Join(tmpDir, "source.db")))
	destination := openTestClusterServer(t, newTestClusterConfig(t, "b", filepath.Join(tmpDir, "destination.db")))
	state := buildClusterStateForTests(source.Cluster.Snapshot().SlotCount, "http://source", "http://destination")
	require.NoError(t, source.Cluster.ReplaceState(state))
	require.NoError(t, destination.Cluster.ReplaceState(state))
	key := findKeyForOwner(state, "a")
	slot := ClusterKeySlot(key, state.SlotCount)
	nowMs := redisNowUnixMilli(time.Now())
	require.NoError(t, PutRedisBytes(source.DB, redisNamespaceForDB(0, state.SlotCount), key, bytes.Repeat([]byte("x"), 9*1024*1024), nowMs))

	identity := clusterTransferIdentity{ClusterID: state.ClusterID, MoveID: "partial-large", SourceNodeID: "a", DestinationNodeID: "b", Epoch: state.Epoch}
	var stream bytes.Buffer
	_, err := streamClusterSlotRangeChunkWithIdentity(source.DB, &stream, state.SlotCount, slot, slot, nowMs, redisDatabaseMin, nil, 0, identity)
	require.NoError(t, err)
	partial := clusterTransferStreamBeforeRecord(t, stream.Bytes(), clusterTransferRecordKeyEnd)
	receipt := clusterImportReceiptContext{JobID: identity.MoveID, ChunkIndex: 1}
	_, err = importClusterSlotStreamWithReceiptAndIdentity(destination.DB, bytes.NewReader(partial), nowMs, receipt, identity)
	require.ErrorIs(t, err, io.EOF)
	value, found, err := GetRedisBytes(destination.DB, redisNamespaceForDB(0, state.SlotCount), key, nowMs)
	require.NoError(t, err)
	require.False(t, found)
	require.Nil(t, value)
	stagingKeys, err := countClusterImportStagingKeys(destination.DB)
	require.NoError(t, err)
	require.Equal(t, uint64(1), stagingKeys)

	require.NoError(t, deleteClusterImportJobState(destination.DB, identity.MoveID, 1, state.SlotCount))
	stagingKeys, err = countClusterImportStagingKeys(destination.DB)
	require.NoError(t, err)
	require.Zero(t, stagingKeys)
	var stagedStateFound bool
	require.NoError(t, destination.DB.View(func(tx *storage.Tx) error {
		_, stagedStateFound, err = loadClusterImportKeyStateTx(tx, 0, key)
		return err
	}))
	require.False(t, stagedStateFound)

	_, err = importClusterSlotStreamWithReceiptAndIdentity(destination.DB, bytes.NewReader(stream.Bytes()), nowMs, receipt, identity)
	require.NoError(t, err)
	value, found, err = GetRedisBytes(destination.DB, redisNamespaceForDB(0, state.SlotCount), key, nowMs)
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, value, 9*1024*1024)
}

func TestClusterTransferChunksByBytesWithExplicitOversizedKeyHandling(t *testing.T) {
	tmpDir := t.TempDir()
	source := openTestClusterServer(t, newTestClusterConfig(t, "a", filepath.Join(tmpDir, "source.db")))
	state := buildClusterStateForTests(source.Cluster.Snapshot().SlotCount, "http://source", "http://destination")
	require.NoError(t, source.Cluster.ReplaceState(state))
	tag := findHashTagForOwner(state, "a")
	require.NotEmpty(t, tag)
	ns := redisNamespaceForDB(0, state.SlotCount)
	nowMs := redisNowUnixMilli(time.Now())
	keys := [][]byte{
		[]byte("byte-budget:{" + tag + "}:1"),
		[]byte("byte-budget:{" + tag + "}:2"),
		[]byte("byte-budget:{" + tag + "}:3"),
	}
	for _, key := range keys {
		require.NoError(t, PutRedisBytes(source.DB, ns, key, bytes.Repeat([]byte("v"), 12*1024), nowMs))
	}
	slot := ClusterKeySlot(keys[0], state.SlotCount)
	identity := clusterTransferIdentity{ClusterID: state.ClusterID, MoveID: "byte-budget", SourceNodeID: "a", DestinationNodeID: "b", Epoch: state.Epoch}
	var stream bytes.Buffer
	stats, err := streamClusterSlotRangeChunkWithIdentityAndPolicy(
		source.DB,
		&stream,
		state.SlotCount,
		slot,
		slot,
		nowMs,
		redisDatabaseMin,
		nil,
		clusterTransferChunkPolicy{TargetBytes: 20 * 1024, MaxBytes: 40 * 1024, MaxRecords: 1000},
		identity,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(1), stats.EntriesTransferred)
	require.False(t, stats.EOF)
	require.LessOrEqual(t, stats.BytesTransferred, uint64(40*1024))
	require.False(t, stats.OversizedLogicalKey)

	var oversizedKey []byte
	oversizedSlot := -1
	for index := 0; index < 100000; index++ {
		candidate := []byte(fmt.Sprintf("oversized:%d", index))
		candidateSlot := ClusterKeySlot(candidate, state.SlotCount)
		if candidateSlot != slot && state.SlotOwners[candidateSlot] == "a" {
			oversizedKey = candidate
			oversizedSlot = candidateSlot
			break
		}
	}
	require.NotEmpty(t, oversizedKey)
	require.NoError(t, PutRedisBytes(source.DB, ns, oversizedKey, bytes.Repeat([]byte("x"), 96*1024), nowMs))
	var oversized bytes.Buffer
	oversizedStats, err := streamClusterSlotRangeChunkWithIdentityAndPolicy(
		source.DB,
		&oversized,
		state.SlotCount,
		oversizedSlot,
		oversizedSlot,
		nowMs,
		redisDatabaseMin,
		nil,
		clusterTransferChunkPolicy{TargetBytes: 16 * 1024, MaxBytes: 32 * 1024, MaxRecords: 1000},
		identity,
	)
	require.NoError(t, err)
	require.True(t, oversizedStats.OversizedLogicalKey)
	require.Equal(t, uint64(1), oversizedStats.EntriesTransferred)
}

func TestClusterTransferRoundTripAllRedisTypesAndAbsoluteTTL(t *testing.T) {
	tmpDir := t.TempDir()
	source := openTestClusterServer(t, newTestClusterConfig(t, "a", filepath.Join(tmpDir, "source.db")))
	destination := openTestClusterServer(t, newTestClusterConfig(t, "b", filepath.Join(tmpDir, "destination.db")))
	state := buildClusterStateForTests(source.Cluster.Snapshot().SlotCount, "http://source", "http://destination")
	require.NoError(t, source.Cluster.ReplaceState(state))
	require.NoError(t, destination.Cluster.ReplaceState(state))
	tag := findHashTagForOwner(state, "a")
	require.NotEmpty(t, tag)
	prefix := "types:{" + tag + "}:"
	stringKey := []byte(prefix + "string")
	listKey := []byte(prefix + "list")
	hashKey := []byte(prefix + "hash")
	zsetKey := []byte(prefix + "zset")
	bloomKey := []byte(prefix + "bloom")
	topKKey := []byte(prefix + "topk")
	slot := ClusterKeySlot(stringKey, state.SlotCount)
	sourceRedis := NewRedisServer(source.Config, source.DB, source.Logger)
	sourceRedis.Cluster = source.Cluster

	clusterTestExecute(t, sourceRedis, "SET", string(stringKey), "value")
	clusterTestExecute(t, sourceRedis, "RPUSH", string(listKey), "one", "two", "three")
	clusterTestExecute(t, sourceRedis, "HSET", string(hashKey), "field-a", "a", "field-b", "b")
	clusterTestExecute(t, sourceRedis, "ZADD", string(zsetKey), "1", "alpha", "2", "beta")
	clusterTestExecute(t, sourceRedis, "BF.RESERVE", string(bloomKey), "0.01", "100")
	clusterTestExecute(t, sourceRedis, "BF.ADD", string(bloomKey), "present")
	clusterTestExecute(t, sourceRedis, "TOPK.RESERVE", string(topKKey), "2", "100", "5", "0.9")
	clusterTestExecute(t, sourceRedis, "TOPK.ADD", string(topKKey), "hot", "cold", "hot")
	clusterTestExecute(t, sourceRedis, "PEXPIRE", string(stringKey), "120000")

	nowMs := redisNowUnixMilli(time.Now())
	var stream bytes.Buffer
	require.NoError(t, streamClusterSlotRange(source.DB, &stream, state.SlotCount, slot, slot, nowMs))
	_, err := importClusterSlotStream(destination.DB, bytes.NewReader(stream.Bytes()), nowMs)
	require.NoError(t, err)

	destinationState := state.Clone()
	destinationState.SlotOwners[slot] = "b"
	require.NoError(t, destination.Cluster.ReplaceState(destinationState))
	destinationRedis := NewRedisServer(destination.Config, destination.DB, destination.Logger)
	destinationRedis.Cluster = destination.Cluster
	require.Equal(t, "value", clusterTestReplyValue(clusterTestExecute(t, destinationRedis, "GET", string(stringKey))))
	require.Equal(t, []any{"one", "two", "three"}, clusterTestReplyValue(clusterTestExecute(t, destinationRedis, "LRANGE", string(listKey), "0", "-1")))
	require.Equal(t, "b", clusterTestReplyValue(clusterTestExecute(t, destinationRedis, "HGET", string(hashKey), "field-b")))
	require.Equal(t, "2", clusterTestReplyValue(clusterTestExecute(t, destinationRedis, "ZSCORE", string(zsetKey), "beta")))
	require.Equal(t, int64(1), clusterTestReplyValue(clusterTestExecute(t, destinationRedis, "BF.EXISTS", string(bloomKey), "present")))
	require.Equal(t, []any{int64(1)}, clusterTestReplyValue(clusterTestExecute(t, destinationRedis, "TOPK.QUERY", string(topKKey), "hot")))

	sourceExpiry := clusterTestExpiration(t, source.DB, redisNamespaceForDB(0, state.SlotCount), stringKey)
	destinationExpiry := clusterTestExpiration(t, destination.DB, redisNamespaceForDB(0, state.SlotCount), stringKey)
	require.Equal(t, sourceExpiry, destinationExpiry)
}

func clusterTransferStreamBeforeRecord(t *testing.T, stream []byte, targetType uint8) []byte {
	t.Helper()
	const headerSize = 8 + 2 + 4 + 4 + 4
	offset := headerSize
	for offset < len(stream) {
		start := offset
		require.GreaterOrEqual(t, len(stream)-offset, 16)
		payloadLen := int(binary.LittleEndian.Uint32(stream[offset+8 : offset+12]))
		expectedCRC := binary.LittleEndian.Uint32(stream[offset+12 : offset+16])
		offset += 16
		require.LessOrEqual(t, payloadLen, len(stream)-offset)
		payload := stream[offset : offset+payloadLen]
		require.Equal(t, expectedCRC, crc32.Checksum(payload, clusterTransferCRC32Table))
		var record clusterTransferRecord
		require.NoError(t, gobDecodeClusterTransferRecord(payload, &record))
		if record.Type == targetType {
			return cloneBytes(stream[:start])
		}
		offset += payloadLen
	}
	t.Fatalf("record type %d was not found", targetType)
	return nil
}

func gobDecodeClusterTransferRecord(payload []byte, record *clusterTransferRecord) error {
	_, _, decoded, err := readClusterTransferRecord(bytes.NewReader(appendClusterTransferFrameForTest(payload)))
	if err == nil {
		*record = decoded
	}
	return err
}

func appendClusterTransferFrameForTest(payload []byte) []byte {
	frame := make([]byte, 16+len(payload))
	binary.LittleEndian.PutUint64(frame[0:8], 1)
	binary.LittleEndian.PutUint32(frame[8:12], uint32(len(payload)))
	binary.LittleEndian.PutUint32(frame[12:16], crc32.Checksum(payload, clusterTransferCRC32Table))
	copy(frame[16:], payload)
	return frame
}

func clusterTestExecute(t *testing.T, server *RedisServer, args ...string) redisReply {
	t.Helper()
	encoded := make([][]byte, len(args))
	for index, arg := range args {
		encoded[index] = []byte(arg)
	}
	reply, err := server.executeCommand(0, encoded, nil, redisNowUnixMilli(time.Now()), false)
	require.NoError(t, err)
	return reply
}

func clusterTestReplyValue(reply redisReply) any {
	switch value := reply.(type) {
	case redisSimpleStringReply:
		return value.value
	case redisBulkReply:
		if value.null {
			return nil
		}
		return string(value.value)
	case redisIntegerReply:
		return value.value
	case redisArrayReply:
		items := make([]any, 0, len(value.values))
		for _, item := range value.values {
			items = append(items, clusterTestReplyValue(item))
		}
		return items
	default:
		return nil
	}
}

func clusterTestExpiration(t *testing.T, db *storage.DB, ns redisNamespace, key []byte) int64 {
	t.Helper()
	var expiration int64
	require.NoError(t, db.View(func(tx *storage.Tx) error {
		var found bool
		var err error
		expiration, found, err = loadRedisExpireAtMsTx(tx, ns, key)
		if err == nil && !found {
			t.Fatalf("expiration for %q was not found", key)
		}
		return err
	}))
	return expiration
}

func FuzzClusterTransferFrameDecoder(f *testing.F) {
	f.Add([]byte{})
	f.Add(make([]byte, 16))
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _, _, _ = readClusterTransferRecord(bytes.NewReader(data))
	})
}
