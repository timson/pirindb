package main

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/timson/pirindb/storage"
)

func TestRedisGenerationGCScansEveryLegacyObject(t *testing.T) {
	db := openRedisKeyMetaTestDB(t, filepath.Join(t.TempDir(), "generation-gc.db"))
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	ns := redisNamespaceForDB(0)

	const objectCount = 7
	for idx := 0; idx < objectCount; idx++ {
		createLegacyHashForMigration(t, db, ns, []byte(fmt.Sprintf("legacy:%02d", idx)), uint64(100+idx), 1)
	}
	require.NoError(t, db.Update(func(tx *storage.Tx) error {
		_, err := flushRedisDBGenerationTx(tx, ns, 1000)
		return err
	}))

	// A batch smaller than the directory proves that generation discovery is
	// resumable instead of dropping the unvisited metadata with the directory.
	srv := &RedisServer{DB: db, stopCh: make(chan struct{})}
	require.NoError(t, srv.runRedisGCCycle(1100, redisGCSettings{
		batchSize:          2,
		batchBytes:         1024 * 1024,
		maxBatchesPerCycle: 1000,
	}))

	require.NoError(t, db.View(func(tx *storage.Tx) error {
		for idx := 0; idx < objectCount; idx++ {
			_, err := tx.GetBucket(redisHashBucketName(ns, uint64(100+idx)))
			require.ErrorIs(t, err, storage.ErrBucketNotFound)
		}
		queue, err := optionalRedisBucket(tx, ns.gcQueueBucket)
		require.NoError(t, err)
		if queue != nil {
			require.Zero(t, queue.ItemCount())
		}
		return nil
	}))
}

func TestRedisExactKeyGCHandlesKeyRecreationByType(t *testing.T) {
	db := openRedisKeyMetaTestDB(t, filepath.Join(t.TempDir(), "exact-key-gc.db"))
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	ns := redisNamespaceForDB(0)
	require.NoError(t, db.Update(func(tx *storage.Tx) error {
		return saveRedisKeyDirectoryStateTx(tx, ns, redisKeyDirectoryState{
			Phase:       redisKeyDirectoryPhaseActive,
			BucketIndex: byte(len(redisLegacyKeySources)),
		})
	}))

	for _, key := range [][]byte{[]byte("becomes-hash"), []byte("stays-string")} {
		require.NoError(t, PutRedisBytes(db, ns, key, []byte("old"), 1000))
		require.NoError(t, db.Update(func(tx *storage.Tx) error {
			_, err := unlinkRedisKeyByRawTypeTx(tx, ns, key, redisKeyTypeString, 1010)
			return err
		}))
	}
	require.NoError(t, db.Update(func(tx *storage.Tx) error {
		_, err := redisHashSetTx(tx, ns, []byte("becomes-hash"), []hashFieldValuePair{{
			field: []byte("field"), value: []byte("new-hash"),
		}}, 1020)
		return err
	}))
	require.NoError(t, PutRedisBytes(db, ns, []byte("stays-string"), []byte("new-string"), 1020))

	srv := &RedisServer{DB: db, stopCh: make(chan struct{})}
	require.NoError(t, srv.runRedisGCCycle(1030, redisGCSettings{
		batchSize:          8,
		batchBytes:         1024 * 1024,
		maxBatchesPerCycle: 32,
	}))

	require.NoError(t, db.View(func(tx *storage.Tx) error {
		stringsBucket, err := tx.GetBucket(ns.stringBucket)
		if err != nil {
			return err
		}
		_, found := stringsBucket.Get([]byte("becomes-hash"))
		require.False(t, found, "the stale string record must be reclaimed")
		value, found := stringsBucket.Get([]byte("stays-string"))
		require.True(t, found)
		require.Equal(t, []byte("new-string"), value)

		hashValue, found, err := redisHashGetTx(tx, ns, []byte("becomes-hash"), []byte("field"), 1030)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, []byte("new-hash"), hashValue)
		return nil
	}))
}
