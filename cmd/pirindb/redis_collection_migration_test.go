package main

import (
	"fmt"
	"math"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/timson/pirindb/storage"
)

func createLegacyHashForMigration(t *testing.T, db *storage.DB, ns redisNamespace, key []byte, id uint64, fields int) {
	t.Helper()
	require.NoError(t, db.Update(func(tx *storage.Tx) error {
		data, err := tx.CreateBucket(redisHashBucketName(ns, id))
		if err != nil {
			return err
		}
		for idx := 0; idx < fields; idx++ {
			if err = data.Put([]byte(fmt.Sprintf("field:%04d", idx)), []byte(fmt.Sprintf("value:%04d", idx))); err != nil {
				return err
			}
		}
		metaBucket, err := tx.CreateBucketIfNotExists(ns.hashMetaBucket)
		if err != nil {
			return err
		}
		if err = metaBucket.Put(key, (&redisHashMeta{ID: id, FieldCount: uint64(fields)}).serialize()); err != nil {
			return err
		}
		typeCode, err := redisKeyMetaTypeCode(redisKeyTypeHash)
		if err != nil {
			return err
		}
		if err = putRedisKeyMetaTx(tx, ns, key, redisKeyMeta{Type: typeCode, StorageFormat: redisKeyStorageLegacy, ObjectID: id, ExpireAtMs: -1}); err != nil {
			return err
		}
		if err = saveRedisKeyDirectoryStateTx(tx, ns, redisKeyDirectoryState{Phase: redisKeyDirectoryPhaseActive, BucketIndex: byte(len(redisLegacyKeySources))}); err != nil {
			return err
		}
		return ensureRedisSlotIndexEntryTx(tx, ns, key)
	}))
}

func finishObjectMigrationForTest(t *testing.T, db *storage.DB, ns redisNamespace, records int, bytes int64) redisObjectMigrationState {
	t.Helper()
	for attempts := 0; attempts < 1000; attempts++ {
		var state redisObjectMigrationState
		var found bool
		require.NoError(t, db.Update(func(tx *storage.Tx) error {
			var err error
			state, found, err = beginRedisObjectMigrationTx(tx, ns)
			return err
		}))
		if !found {
			if state.Phase == redisObjectMigrationPhaseIdle {
				return state
			}
			continue
		}
		var result redisObjectMigrationBatchResult
		require.NoError(t, db.Update(func(tx *storage.Tx) error {
			var err error
			result, err = copyRedisObjectMigrationBatchTx(tx, ns, 1000, records, bytes)
			return err
		}))
		_ = result
	}
	t.Fatal("object migration did not finish")
	return redisObjectMigrationState{}
}

func TestRedisLegacyHashMigratesToSharedBucketAndQueuesCleanup(t *testing.T) {
	db := openRedisKeyMetaTestDB(t, filepath.Join(t.TempDir(), "migration.db"))
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	ns := redisNamespaceForDB(0)
	key := []byte("legacy-hash")
	createLegacyHashForMigration(t, db, ns, key, 41, 301)

	finishObjectMigrationForTest(t, db, ns, 17, 2048)

	require.NoError(t, db.View(func(tx *storage.Tx) error {
		keyMeta, found, err := loadRedisKeyMetaTx(tx, ns, key)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, byte(redisKeyStorageShared), keyMeta.StorageFormat)
		require.NotEqual(t, uint64(41), keyMeta.ObjectID)

		hashMeta, found, err := loadRedisHashMetaTx(tx, ns, key)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, keyMeta.ObjectID, hashMeta.ID)
		require.Equal(t, byte(redisKeyStorageShared), hashMeta.StorageFormat)
		data, err := openRedisObjectBucketTx(tx, ns.hashDataBucket, nil, hashMeta.ID, redisKeyStorageShared, false)
		if err != nil {
			return err
		}
		require.Equal(t, 301, countRedisObjectEntries(data))
		value, exists := data.Get([]byte("field:0300"))
		require.True(t, exists)
		require.Equal(t, []byte("value:0300"), value)
		queue, err := tx.GetBucket(ns.gcQueueBucket)
		if err != nil {
			return err
		}
		require.Equal(t, uint64(1), queue.ItemCount())
		return nil
	}))

	settings := redisGCSettings{batchSize: 19, batchBytes: 4096, maxBatchesPerCycle: 1000}
	srv := &RedisServer{DB: db, stopCh: make(chan struct{})}
	require.NoError(t, srv.runRedisGCCycle(2000, settings))
	require.NoError(t, db.View(func(tx *storage.Tx) error {
		_, err := tx.GetBucket(redisHashBucketName(ns, 41))
		require.ErrorIs(t, err, storage.ErrBucketNotFound)
		queue, err := tx.GetBucket(ns.gcQueueBucket)
		if err == nil {
			require.Equal(t, uint64(0), queue.ItemCount())
		}
		return nil
	}))
}

func TestRedisMigrationRestartAbandonsHiddenDestination(t *testing.T) {
	db := openRedisKeyMetaTestDB(t, filepath.Join(t.TempDir(), "migration.db"))
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	ns := redisNamespaceForDB(0)
	key := []byte("restart-hash")
	createLegacyHashForMigration(t, db, ns, key, 51, 80)

	var first redisObjectMigrationState
	require.NoError(t, db.Update(func(tx *storage.Tx) error {
		var found bool
		var err error
		first, found, err = beginRedisObjectMigrationTx(tx, ns)
		require.True(t, found)
		return err
	}))
	require.NoError(t, db.Update(func(tx *storage.Tx) error {
		_, err := copyRedisObjectMigrationBatchTx(tx, ns, 1000, 7, 1024)
		return err
	}))
	require.NoError(t, db.Update(func(tx *storage.Tx) error {
		state, err := loadRedisObjectMigrationStateTx(tx, ns)
		if err != nil {
			return err
		}
		return resetRedisObjectMigrationTx(tx, ns, state, 1100)
	}))

	finishObjectMigrationForTest(t, db, ns, 11, 2048)
	require.NoError(t, db.View(func(tx *storage.Tx) error {
		meta, found, err := loadRedisKeyMetaTx(tx, ns, key)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, byte(redisKeyStorageShared), meta.StorageFormat)
		require.NotEqual(t, first.DestID, meta.ObjectID)
		queue, err := tx.GetBucket(ns.gcQueueBucket)
		if err != nil {
			return err
		}
		require.Equal(t, uint64(2), queue.ItemCount())
		return nil
	}))
}

func TestRedisEveryCompositeLegacyTypeMigratesOnline(t *testing.T) {
	db := openRedisKeyMetaTestDB(t, filepath.Join(t.TempDir(), "all-types.db"))
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	ns := redisNamespaceForDB(0)
	require.NoError(t, db.Update(func(tx *storage.Tx) error {
		if err := saveRedisKeyDirectoryStateTx(tx, ns, redisKeyDirectoryState{Phase: redisKeyDirectoryPhaseActive, BucketIndex: byte(len(redisLegacyKeySources))}); err != nil {
			return err
		}
		listMeta := &redisListMeta{ID: 101, Length: 1, HeadSegID: 1, TailSegID: 1}
		listData, err := tx.CreateBucket(redisListBucketName(ns, listMeta.ID))
		if err != nil {
			return err
		}
		segment := &redisListSegment{}
		segment.appendValue([]byte("item"))
		if err = listData.Put(redisListSegmentKey(1), segment.serialize()); err != nil {
			return err
		}
		listMetas, err := tx.CreateBucketIfNotExists(ns.listMetaBucket)
		if err != nil {
			return err
		}
		if err = listMetas.Put([]byte("a-list"), listMeta.serialize()); err != nil {
			return err
		}

		zsetMeta := &redisZSetMeta{ID: 102, Cardinality: 1}
		members, err := tx.CreateBucket(redisZSetMemberBucketName(ns, zsetMeta.ID))
		if err != nil {
			return err
		}
		scores, err := tx.CreateBucket(redisZSetScoreBucketName(ns, zsetMeta.ID))
		if err != nil {
			return err
		}
		encodedScore := encodeRedisZSetSortableScore(2.5)
		if err = members.Put([]byte("member"), encodedScore); err != nil {
			return err
		}
		if err = scores.Put(redisZSetScoreIndexKeyFromEncoded(encodedScore, []byte("member")), []byte{}); err != nil {
			return err
		}
		zsetMetas, err := tx.CreateBucketIfNotExists(ns.zsetMetaBucket)
		if err != nil {
			return err
		}
		if err = zsetMetas.Put([]byte("b-zset"), zsetMeta.serialize()); err != nil {
			return err
		}

		bloomMeta := &redisBloomMeta{ID: 103, InitialCapacity: 10, ErrorRateBits: math.Float64bits(0.01), Expansion: 2, SubFilterCount: 1}
		bloomData, err := tx.CreateBucket(redisBloomDataBucketName(ns, bloomMeta.ID))
		if err != nil {
			return err
		}
		subMeta, err := redisBloomBuildSubFilterMeta(10, 0.01)
		if err != nil {
			return err
		}
		if err = bloomData.Put(redisBloomSubMetaKey(0), subMeta.serialize()); err != nil {
			return err
		}
		bloomMetas, err := tx.CreateBucketIfNotExists(ns.bloomMetaBucket)
		if err != nil {
			return err
		}
		if err = bloomMetas.Put([]byte("c-bloom"), bloomMeta.serialize()); err != nil {
			return err
		}

		topKMeta := &redisTopKMeta{ID: 104, K: 5, Width: 8, Depth: 7, DecayBits: math.Float64bits(0.9)}
		if _, err = tx.CreateBucket(redisTopKDataBucketName(ns, topKMeta.ID)); err != nil {
			return err
		}
		topKMetas, err := tx.CreateBucketIfNotExists(ns.topkMetaBucket)
		if err != nil {
			return err
		}
		if err = topKMetas.Put([]byte("d-topk"), topKMeta.serialize()); err != nil {
			return err
		}

		entries := []struct {
			key      []byte
			typeCode byte
			id       uint64
		}{{[]byte("a-list"), redisKeyMetaTypeList, 101}, {[]byte("b-zset"), redisKeyMetaTypeZSet, 102}, {[]byte("c-bloom"), redisKeyMetaTypeBloom, 103}, {[]byte("d-topk"), redisKeyMetaTypeTopK, 104}}
		for _, entry := range entries {
			if err = putRedisKeyMetaTx(tx, ns, entry.key, redisKeyMeta{Type: entry.typeCode, StorageFormat: redisKeyStorageLegacy, ObjectID: entry.id, ExpireAtMs: -1}); err != nil {
				return err
			}
			if err = ensureRedisSlotIndexEntryTx(tx, ns, entry.key); err != nil {
				return err
			}
		}
		return nil
	}))

	finishObjectMigrationForTest(t, db, ns, 1, 1024)
	require.NoError(t, db.View(func(tx *storage.Tx) error {
		for _, key := range [][]byte{[]byte("a-list"), []byte("b-zset"), []byte("c-bloom"), []byte("d-topk")} {
			meta, found, err := loadRedisKeyMetaTx(tx, ns, key)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, byte(redisKeyStorageShared), meta.StorageFormat)
		}
		queue, err := tx.GetBucket(ns.gcQueueBucket)
		if err != nil {
			return err
		}
		require.Equal(t, uint64(5), queue.ItemCount())
		return nil
	}))
}
