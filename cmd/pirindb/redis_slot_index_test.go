package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/timson/pirindb/storage"
)

func openRedisSlotIndexTestDB(t *testing.T) *storage.DB {
	t.Helper()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "slot-index.db")
	logger := createLogger("ERROR")
	storage.SetLogger(logger)
	db, err := storage.Open(dbPath, storage.DefaultOptions())
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = db.Close()
		_ = os.Remove(dbPath)
		_ = os.Remove(db.GetOptions().TxLogPath)
	})
	return db
}

func TestRedisSlotIndexTracksStringLifecycle(t *testing.T) {
	db := openRedisSlotIndexTestDB(t)
	slotCount := 128
	ns := redisNamespaceForDB(0, slotCount)
	nowMs := redisNowUnixMilli(time.Now())

	sourceKey := []byte("slot:index:string:source")
	destinationKey := []byte("slot:index:string:destination")
	sourceSlot := ClusterKeySlot(sourceKey, slotCount)
	destinationSlot := ClusterKeySlot(destinationKey, slotCount)

	require.NoError(t, PutRedisBytes(db, ns, sourceKey, []byte("value"), nowMs))

	err := db.View(func(tx *storage.Tx) error {
		keys, err := listRedisKeysBySlotRangeTx(tx, ns, sourceSlot, sourceSlot, nowMs)
		require.NoError(t, err)
		require.Contains(t, keys, sourceKey)
		return nil
	})
	require.NoError(t, err)

	renamed, _, err := RenameRedisKey(db, ns, sourceKey, destinationKey, false, nowMs)
	require.NoError(t, err)
	require.True(t, renamed)

	err = db.View(func(tx *storage.Tx) error {
		keys, err := listRedisKeysBySlotRangeTx(tx, ns, sourceSlot, sourceSlot, nowMs)
		require.NoError(t, err)
		require.NotContains(t, keys, sourceKey)

		keys, err = listRedisKeysBySlotRangeTx(tx, ns, destinationSlot, destinationSlot, nowMs)
		require.NoError(t, err)
		require.Contains(t, keys, destinationKey)
		return nil
	})
	require.NoError(t, err)

	deleted, err := DeleteManyRedisBytes(db, ns, [][]byte{destinationKey}, nowMs)
	require.NoError(t, err)
	require.Equal(t, int64(1), deleted)

	err = db.View(func(tx *storage.Tx) error {
		keys, err := listRedisKeysBySlotRangeTx(tx, ns, destinationSlot, destinationSlot, nowMs)
		require.NoError(t, err)
		require.NotContains(t, keys, destinationKey)
		return nil
	})
	require.NoError(t, err)
}

func TestRedisSlotIndexDrivesSlotRangeDelete(t *testing.T) {
	db := openRedisSlotIndexTestDB(t)
	slotCount := 128
	ns := redisNamespaceForDB(0, slotCount)
	nowMs := redisNowUnixMilli(time.Now())

	hashKey := []byte("slot:index:hash")
	stringKey := []byte("slot:index:string")
	hashSlot := ClusterKeySlot(hashKey, slotCount)
	stringSlot := ClusterKeySlot(stringKey, slotCount)
	if hashSlot == stringSlot {
		stringKey = []byte("slot:index:string:other")
		stringSlot = ClusterKeySlot(stringKey, slotCount)
	}
	require.NotEqual(t, hashSlot, stringSlot)

	err := db.Update(func(tx *storage.Tx) error {
		_, err := redisHashSetTx(tx, ns, hashKey, []hashFieldValuePair{
			{field: []byte("field"), value: []byte("value")},
		}, nowMs)
		if err != nil {
			return err
		}
		return putRedisBytesTx(tx, ns, stringKey, []byte("keep"), nowMs)
	})
	require.NoError(t, err)

	err = db.View(func(tx *storage.Tx) error {
		keys, err := listRedisKeysBySlotRangeTx(tx, ns, hashSlot, hashSlot, nowMs)
		require.NoError(t, err)
		require.Contains(t, keys, hashKey)
		return nil
	})
	require.NoError(t, err)

	require.NoError(t, deleteClusterSlotRange(db, slotCount, hashSlot, hashSlot, nowMs))

	_, found, err := GetRedisBytes(db, ns, hashKey, nowMs)
	require.NoError(t, err)
	require.False(t, found)

	value, found, err := GetRedisBytes(db, ns, stringKey, nowMs)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "keep", string(value))

	err = db.View(func(tx *storage.Tx) error {
		keys, err := listRedisKeysBySlotRangeTx(tx, ns, hashSlot, hashSlot, nowMs)
		require.NoError(t, err)
		require.NotContains(t, keys, hashKey)
		return nil
	})
	require.NoError(t, err)
}
