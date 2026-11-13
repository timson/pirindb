package main

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/timson/pirindb/storage"
)

func TestRedisCompositeKeyCountDoesNotGrowBucketCatalog(t *testing.T) {
	db := openRedisKeyMetaTestDB(t, filepath.Join(t.TempDir(), "shared.db"))
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	ns := redisNamespaceForDB(0)
	require.NoError(t, db.Update(func(tx *storage.Tx) error {
		return saveRedisKeyDirectoryStateTx(tx, ns, redisKeyDirectoryState{Phase: redisKeyDirectoryPhaseActive, BucketIndex: byte(len(redisLegacyKeySources))})
	}))
	createHashes := func(start, end int) {
		for base := start; base < end; base += 100 {
			limit := base + 100
			if limit > end {
				limit = end
			}
			require.NoError(t, db.Update(func(tx *storage.Tx) error {
				for idx := base; idx < limit; idx++ {
					key := []byte(fmt.Sprintf("hash:%06d", idx))
					if _, err := redisHashSetTx(tx, ns, key, []hashFieldValuePair{{field: []byte("f"), value: []byte("v")}}, 1000); err != nil {
						return err
					}
				}
				return nil
			}))
		}
	}
	createHashes(0, 1000)
	stat, err := db.StatE()
	require.NoError(t, err)
	bucketCount := len(stat.Buckets)
	for name := range stat.Buckets {
		require.False(t, strings.HasPrefix(name, string(ns.hashDataPrefix)), "fresh hashes must not create private buckets")
	}
	require.Contains(t, stat.Buckets, string(ns.hashDataBucket))

	createHashes(1000, 2000)
	stat, err = db.StatE()
	require.NoError(t, err)
	require.Equal(t, bucketCount, len(stat.Buckets))
	require.Equal(t, uint64(2000), stat.Buckets[string(ns.hashMetaBucket)].ItemsN)
}

func TestRedisListTrimQueuesDetachedSegmentsForBoundedGC(t *testing.T) {
	db := openRedisKeyMetaTestDB(t, filepath.Join(t.TempDir(), "list-gc.db"))
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	ns := redisNamespaceForDB(0)
	values := make([][]byte, 0, 300)
	for idx := 0; idx < 300; idx++ {
		values = append(values, []byte(fmt.Sprintf("%03d:%s", idx, strings.Repeat("x", 300))))
	}
	require.NoError(t, db.Update(func(tx *storage.Tx) error {
		if err := saveRedisKeyDirectoryStateTx(tx, ns, redisKeyDirectoryState{Phase: redisKeyDirectoryPhaseActive, BucketIndex: byte(len(redisLegacyKeySources))}); err != nil {
			return err
		}
		_, err := redisPushTx(tx, ns, []byte("list"), values, false, 1000)
		return err
	}))
	var before int
	require.NoError(t, db.View(func(tx *storage.Tx) error {
		meta, _, err := loadRedisListMetaTx(tx, ns, []byte("list"))
		if err != nil {
			return err
		}
		store, err := openRedisObjectBucketTx(tx, ns.listDataBucket, redisListBucketName(ns, meta.ID), meta.ID, meta.StorageFormat, false)
		if err != nil {
			return err
		}
		before = countRedisObjectEntries(store)
		return nil
	}))
	require.NoError(t, db.Update(func(tx *storage.Tx) error {
		return redisListTrimTx(tx, ns, []byte("list"), 100, 199, 1100)
	}))
	require.NoError(t, db.View(func(tx *storage.Tx) error {
		queue, err := tx.GetBucket(ns.gcQueueBucket)
		if err != nil {
			return err
		}
		require.GreaterOrEqual(t, queue.ItemCount(), uint64(1))
		return nil
	}))

	srv := &RedisServer{DB: db, stopCh: make(chan struct{})}
	require.NoError(t, srv.runRedisGCCycle(1200, redisGCSettings{batchSize: 1, batchBytes: 1024 * 1024, maxBatchesPerCycle: 1000}))
	require.NoError(t, db.View(func(tx *storage.Tx) error {
		meta, _, err := loadRedisListMetaTx(tx, ns, []byte("list"))
		if err != nil {
			return err
		}
		store, err := openRedisObjectBucketTx(tx, ns.listDataBucket, redisListBucketName(ns, meta.ID), meta.ID, meta.StorageFormat, false)
		if err != nil {
			return err
		}
		require.Less(t, countRedisObjectEntries(store), before)
		return nil
	}))
}
