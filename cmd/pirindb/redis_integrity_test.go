package main

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/timson/pirindb/storage"
)

func TestRedisLogicalIntegrityDetectsIndexMismatch(t *testing.T) {
	db := openRedisKeyMetaTestDB(t, filepath.Join(t.TempDir(), "integrity.db"))
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	ns := redisNamespaceForDB(0)
	nowMs := int64(1000)
	require.NoError(t, db.Update(func(tx *storage.Tx) error {
		for dbIndex := redisDatabaseMin; dbIndex <= redisDatabaseMax; dbIndex++ {
			if err := saveRedisKeyDirectoryStateTx(tx, redisNamespaceForDB(dbIndex), redisKeyDirectoryState{Phase: redisKeyDirectoryPhaseActive, BucketIndex: byte(len(redisLegacyKeySources))}); err != nil {
				return err
			}
		}
		if err := putRedisBytesTx(tx, ns, []byte("string"), []byte("value"), nowMs); err != nil {
			return err
		}
		if _, err := redisPushTx(tx, ns, []byte("list"), [][]byte{[]byte("a"), []byte("b")}, false, nowMs); err != nil {
			return err
		}
		if _, err := redisHashSetTx(tx, ns, []byte("hash"), []hashFieldValuePair{{field: []byte("field"), value: []byte("value")}}, nowMs); err != nil {
			return err
		}
		if _, err := redisZSetAddTx(tx, ns, []byte("zset"), []redisZSetScoreMemberPair{{score: 1.5, member: []byte("member")}}, nowMs); err != nil {
			return err
		}
		return setRedisExpireAtMsTx(tx, ns, []byte("string"), nowMs+10000)
	}))

	report, err := CheckRedisIntegrity(db, 16384)
	require.NoError(t, err)
	require.Empty(t, report.Problems)
	require.Equal(t, uint64(4), report.KeysChecked)

	require.NoError(t, db.Update(func(tx *storage.Tx) error {
		bucket, err := tx.GetBucket(ns.slotIndexBucket)
		if err != nil {
			return err
		}
		return bucket.Remove(redisSlotIndexKey(ClusterKeySlot([]byte("hash"), ns.slotCount), []byte("hash")))
	}))
	report, err = CheckRedisIntegrity(db, 16384)
	require.NoError(t, err)
	require.NotEmpty(t, report.Problems)
	require.Contains(t, strings.Join(report.Problems, "\n"), "slot index")
}

func TestRedisLogicalIntegrityDetectsOrphanedRecords(t *testing.T) {
	db := openRedisKeyMetaTestDB(t, filepath.Join(t.TempDir(), "integrity-orphans.db"))
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	ns := redisNamespaceForDB(0)
	require.NoError(t, db.Update(func(tx *storage.Tx) error {
		for dbIndex := redisDatabaseMin; dbIndex <= redisDatabaseMax; dbIndex++ {
			if err := saveRedisKeyDirectoryStateTx(tx, redisNamespaceForDB(dbIndex), redisKeyDirectoryState{
				Phase: redisKeyDirectoryPhaseActive, BucketIndex: byte(len(redisLegacyKeySources)),
			}); err != nil {
				return err
			}
		}
		if err := putRedisBytesTx(tx, ns, []byte("expiring"), []byte("value"), 1000); err != nil {
			return err
		}
		if err := setRedisExpireAtMsTx(tx, ns, []byte("expiring"), 5000); err != nil {
			return err
		}
		if _, err := redisPushTx(tx, ns, []byte("list"), [][]byte{[]byte("value")}, false, 1000); err != nil {
			return err
		}

		stringsBucket, err := tx.GetBucket(ns.stringBucket)
		if err != nil {
			return err
		}
		if err = stringsBucket.Put([]byte("orphan-string"), []byte("orphan")); err != nil {
			return err
		}
		expiryIndex, err := tx.GetBucket(ns.expireIndexBucket)
		if err != nil {
			return err
		}
		if err = expiryIndex.Remove(redisExpireIndexKey(5000, []byte("expiring"))); err != nil {
			return err
		}
		meta, found, err := loadRedisListMetaTx(tx, ns, []byte("list"))
		if err != nil {
			return err
		}
		if !found {
			return errors.New("list metadata disappeared")
		}
		store, err := openRedisObjectBucketTx(tx, ns.listDataBucket, redisListBucketName(ns, meta.ID), meta.ID, meta.StorageFormat, false)
		if err != nil {
			return err
		}
		orphan := &redisListSegment{}
		orphan.appendValue([]byte("unreachable"))
		if err = store.Put(redisListSegmentKey(^uint64(0)), orphan.serialize()); err != nil {
			return err
		}
		builds, err := tx.CreateBucketIfNotExists(ns.buildStateBucket)
		if err != nil {
			return err
		}
		return builds.Put([]byte("broken-build"), []byte("invalid"))
	}))

	report, err := CheckRedisIntegrity(db, 16384)
	require.NoError(t, err)
	joined := strings.Join(report.Problems, "\n")
	require.Contains(t, joined, "string value has no matching key-directory owner")
	require.Contains(t, joined, "expiry index entry is missing")
	require.Contains(t, joined, "unreachable segment")
	require.Contains(t, joined, "hidden-build state is invalid")
}
