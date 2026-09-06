package main

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/timson/pirindb/storage"
)

func openRedisKeyMetaTestDB(t *testing.T, path string) *storage.DB {
	t.Helper()
	db, err := storage.Open(path, storage.DefaultOptions().
		WithSyncPolicy(storage.SyncPolicyJournal).
		WithCheckpointTxThreshold(1024))
	require.NoError(t, err)
	return db
}

func TestRedisKeyDirectoryMigrationResumesAndActivates(t *testing.T) {
	path := storage.TempFileName(".db")
	defer os.Remove(path)
	defer os.Remove(path + ".tlog")
	db := openRedisKeyMetaTestDB(t, path)
	ns := redisNamespaceForDB(0)

	require.NoError(t, db.Update(func(tx *storage.Tx) error {
		stringsBucket, err := tx.CreateBucketIfNotExists(ns.stringBucket)
		if err != nil {
			return err
		}
		for i := 0; i < 300; i++ {
			key := []byte(fmt.Sprintf("legacy:%03d", i))
			if err = stringsBucket.Put(key, []byte("value")); err != nil {
				return err
			}
			if err = ensureRedisSlotIndexEntryTx(tx, ns, key); err != nil {
				return err
			}
		}
		if err = setRedisExpireAtMsTx(tx, ns, []byte("legacy:001"), 123456); err != nil {
			return err
		}

		listMeta := &redisListMeta{ID: 77, Length: 1, HeadSegID: 1, TailSegID: 1}
		dataBucket, err := tx.CreateBucketIfNotExists(redisListBucketName(ns, listMeta.ID))
		if err != nil {
			return err
		}
		segment := &redisListSegment{}
		segment.appendValue([]byte("item"))
		if err = dataBucket.Put(redisListSegmentKey(1), segment.serialize()); err != nil {
			return err
		}
		metaBucket, err := tx.CreateBucketIfNotExists(ns.listMetaBucket)
		if err != nil {
			return err
		}
		if err = metaBucket.Put([]byte("legacy-list"), listMeta.serialize()); err != nil {
			return err
		}
		return ensureRedisSlotIndexEntryTx(tx, ns, []byte("legacy-list"))
	}))

	for i := 0; i < 2; i++ {
		require.NoError(t, db.Update(func(tx *storage.Tx) error {
			active, processed, err := migrateRedisKeyDirectoryBatchTx(tx, ns, 50)
			require.False(t, active)
			require.Equal(t, 50, processed)
			return err
		}))
	}
	require.NoError(t, db.Close())

	db = openRedisKeyMetaTestDB(t, path)
	defer db.Close()
	for attempts := 0; attempts < 20; attempts++ {
		var active bool
		require.NoError(t, db.Update(func(tx *storage.Tx) error {
			var err error
			active, _, err = migrateRedisKeyDirectoryBatchTx(tx, ns, 50)
			return err
		}))
		if active {
			break
		}
	}

	require.NoError(t, db.View(func(tx *storage.Tx) error {
		active, err := redisKeyDirectoryActiveTx(tx, ns)
		require.NoError(t, err)
		require.True(t, active)
		bucket, err := tx.GetBucket(ns.keyMetaBucket)
		if err != nil {
			return err
		}
		require.Equal(t, uint64(301), bucket.ItemCount())

		stringMeta, found, err := loadRedisKeyMetaTx(tx, ns, []byte("legacy:001"))
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, redisKeyMetaTypeString, stringMeta.Type)
		require.Equal(t, int64(123456), stringMeta.ExpireAtMs)

		listMeta, found, err := loadRedisKeyMetaTx(tx, ns, []byte("legacy-list"))
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, redisKeyMetaTypeList, listMeta.Type)
		require.Equal(t, uint64(77), listMeta.ObjectID)
		return nil
	}))
}

func TestRedisKeyDirectoryMigrationRejectsConflictingLegacyTypes(t *testing.T) {
	path := storage.TempFileName(".db")
	defer os.Remove(path)
	db := openRedisKeyMetaTestDB(t, path)
	defer db.Close()
	ns := redisNamespaceForDB(0)
	key := []byte("conflict")

	require.NoError(t, db.Update(func(tx *storage.Tx) error {
		stringsBucket, err := tx.CreateBucketIfNotExists(ns.stringBucket)
		if err != nil {
			return err
		}
		if err = stringsBucket.Put(key, []byte("value")); err != nil {
			return err
		}
		hashMetaBucket, err := tx.CreateBucketIfNotExists(ns.hashMetaBucket)
		if err != nil {
			return err
		}
		return hashMetaBucket.Put(key, (&redisHashMeta{ID: 99, FieldCount: 1}).serialize())
	}))

	err := db.Update(func(tx *storage.Tx) error {
		_, _, migrateErr := migrateRedisKeyDirectoryBatchTx(tx, ns, 1000)
		return migrateErr
	})
	require.ErrorContains(t, err, "conflicting legacy ownership")
}
