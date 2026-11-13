package main

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/timson/pirindb/storage"
)

func TestRedisLargeListRemoveUsesAtomicHiddenRebuild(t *testing.T) {
	db := openRedisKeyMetaTestDB(t, filepath.Join(t.TempDir(), "list-cow.db"))
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	ns := redisNamespaceForDB(0)
	values := make([][]byte, 0, 5000)
	for idx := 0; idx < 5000; idx++ {
		value := []byte(fmt.Sprintf("keep:%04d", idx))
		if idx%5 == 0 {
			value = []byte("remove")
		}
		values = append(values, value)
	}
	require.NoError(t, db.Update(func(tx *storage.Tx) error {
		if err := saveRedisKeyDirectoryStateTx(tx, ns, redisKeyDirectoryState{Phase: redisKeyDirectoryPhaseActive, BucketIndex: byte(len(redisLegacyKeySources))}); err != nil {
			return err
		}
		if _, err := redisPushTx(tx, ns, []byte("large-list"), values, false, 1000); err != nil {
			return err
		}
		return setRedisExpireAtMsTx(tx, ns, []byte("large-list"), 100000)
	}))
	srv := NewRedisServer(&Config{Redis: &RedisConfig{MigrationBatchSize: 128, MigrationBatchBytes: 4096}}, db, createLogger("ERROR"))
	removed, err := srv.redisListRemCOW(ns, []byte("large-list"), 0, []byte("remove"), 2000)
	require.NoError(t, err)
	require.Equal(t, int64(1000), removed)
	require.NoError(t, db.View(func(tx *storage.Tx) error {
		length, err := redisListLenTx(tx, ns, []byte("large-list"), 2000)
		require.NoError(t, err)
		require.Equal(t, int64(4000), length)
		values, err := redisListRangeTx(tx, ns, []byte("large-list"), 0, 2, 2000)
		require.NoError(t, err)
		require.Equal(t, [][]byte{[]byte("keep:0001"), []byte("keep:0002"), []byte("keep:0003")}, values)
		deadline, found, err := loadRedisExpireAtMsTx(tx, ns, []byte("large-list"))
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, int64(100000), deadline)
		builds, err := optionalRedisBucket(tx, ns.buildStateBucket)
		if err != nil {
			return err
		}
		if builds != nil {
			require.Equal(t, uint64(0), builds.ItemCount())
		}
		return nil
	}))
}

func TestRedisLargeZSetRangeDeleteUsesAtomicHiddenRebuild(t *testing.T) {
	db := openRedisKeyMetaTestDB(t, filepath.Join(t.TempDir(), "zset-cow.db"))
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	ns := redisNamespaceForDB(0)
	require.NoError(t, db.Update(func(tx *storage.Tx) error {
		return saveRedisKeyDirectoryStateTx(tx, ns, redisKeyDirectoryState{Phase: redisKeyDirectoryPhaseActive, BucketIndex: byte(len(redisLegacyKeySources))})
	}))
	for base := 0; base < 5000; base += 500 {
		pairs := make([]redisZSetScoreMemberPair, 0, 500)
		for idx := base; idx < base+500; idx++ {
			pairs = append(pairs, redisZSetScoreMemberPair{score: float64(idx), member: []byte(fmt.Sprintf("member:%04d", idx))})
		}
		require.NoError(t, db.Update(func(tx *storage.Tx) error {
			_, err := redisZSetAddTx(tx, ns, []byte("large-zset"), pairs, 1000)
			return err
		}))
	}
	srv := NewRedisServer(&Config{Redis: &RedisConfig{MigrationBatchSize: 127, MigrationBatchBytes: 4096}}, db, createLogger("ERROR"))
	min, err := parseRedisZSetScoreBound([]byte("1000"))
	require.NoError(t, err)
	max, err := parseRedisZSetScoreBound([]byte("3999"))
	require.NoError(t, err)
	removed, err := srv.redisZSetRemRangeCOW(ns, []byte("large-zset"), 2000, redisZSetScoreRangeCOWMatcher(min, max))
	require.NoError(t, err)
	require.Equal(t, int64(3000), removed)
	require.NoError(t, db.View(func(tx *storage.Tx) error {
		cardinality, err := redisZSetCardTx(tx, ns, []byte("large-zset"), 2000)
		require.NoError(t, err)
		require.Equal(t, int64(2000), cardinality)
		_, found, err := redisZSetScoreTx(tx, ns, []byte("large-zset"), []byte("member:2000"), 2000)
		require.NoError(t, err)
		require.False(t, found)
		score, found, err := redisZSetScoreTx(tx, ns, []byte("large-zset"), []byte("member:4999"), 2000)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, float64(4999), score)
		return nil
	}))
}

func TestRedisAbandonedBuildRecoveryUsesBoundedBatches(t *testing.T) {
	db := openRedisKeyMetaTestDB(t, filepath.Join(t.TempDir(), "build-recovery.db"))
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	ns := redisNamespaceForDB(0)
	const abandoned = redisBuildRecoveryBatchSize + 3
	require.NoError(t, db.Update(func(tx *storage.Tx) error {
		for idx := 0; idx < abandoned; idx++ {
			if err := saveRedisBuildStateTx(tx, ns, []byte(fmt.Sprintf("build:%03d", idx)), redisBuildState{
				Type:   redisBuildTypeList,
				DestID: uint64(idx + 1),
			}); err != nil {
				return err
			}
		}
		return nil
	}))

	srv := NewRedisServer(&Config{}, db, createLogger("ERROR"))
	require.NoError(t, srv.recoverRedisAbandonedBuilds())
	require.NoError(t, db.View(func(tx *storage.Tx) error {
		builds, err := tx.GetBucket(ns.buildStateBucket)
		if err != nil {
			return err
		}
		require.Zero(t, builds.ItemCount())
		queue, err := tx.GetBucket(ns.gcQueueBucket)
		if err != nil {
			return err
		}
		require.Equal(t, uint64(abandoned), queue.ItemCount())
		return nil
	}))
}
