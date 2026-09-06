package storage

import (
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNodeCacheIsBoundedAndHandlesInvalidation(t *testing.T) {
	cache := newNodeCache(BTreePageSize, int64(2*BTreePageSize*nodeCacheEntryPageMultiplier))
	require.NotNil(t, cache)

	nodes := []*BNode{{PageNum: 10}, {PageNum: 11}, {PageNum: 12}}
	cache.put(10, nodes[0])
	cache.put(11, nodes[1])
	_, found := cache.get(10)
	require.True(t, found)

	cache.put(12, nodes[2])
	_, found = cache.get(10)
	require.False(t, found)
	_, found = cache.get(12)
	require.True(t, found)

	cache.invalidate(11)
	_, found = cache.get(11)
	require.False(t, found)
	cache.put(11, nodes[1])
	got, found := cache.get(11)
	require.True(t, found)
	require.Same(t, nodes[1], got)
}

func TestIntegrityCheckBypassesWarmNodeCache(t *testing.T) {
	path := TempFileName(".db")
	db, err := Open(path, DefaultOptions().WithNodeCacheBytes(4*1024*1024))
	require.NoError(t, err)
	tlogPath := db.dal.opts.TxLogPath
	t.Cleanup(func() {
		_ = db.Close()
		_ = os.Remove(path)
		_ = os.Remove(tlogPath)
	})

	var bucketRoot uint64
	require.NoError(t, db.Update(func(tx *Tx) error {
		bucket, createErr := tx.CreateBucket([]byte("cache-check"))
		if createErr != nil {
			return createErr
		}
		bucketRoot = bucket.root
		return bucket.Put([]byte("key"), []byte("value"))
	}))
	requireBucketValue(t, db, []byte("cache-check"), []byte("key"), []byte("value"))
	_, cached := db.dal.nodeCache.get(bucketRoot)
	require.True(t, cached)

	flipFileByte(t, path, int64(bucketRoot*BTreePageSize+100))
	_, err = db.Check()
	require.True(t, errors.Is(err, ErrChecksumMismatch), "unexpected check error: %v", err)
}

func TestNodeCachePreservesRollbackIsolationAndCommitInvalidation(t *testing.T) {
	path := TempFileName(".db")
	db, err := Open(path, DefaultOptions().WithNodeCacheBytes(4*1024*1024))
	require.NoError(t, err)
	tlogPath := db.dal.opts.TxLogPath
	t.Cleanup(func() {
		_ = db.Close()
		_ = os.Remove(path)
		_ = os.Remove(tlogPath)
	})

	require.NoError(t, db.Update(func(tx *Tx) error {
		bucket, createErr := tx.CreateBucket([]byte("cache"))
		if createErr != nil {
			return createErr
		}
		return bucket.Put([]byte("key"), []byte("old"))
	}))
	requireBucketValue(t, db, []byte("cache"), []byte("key"), []byte("old"))

	tx := db.Begin(true)
	bucket, err := tx.GetBucket([]byte("cache"))
	require.NoError(t, err)
	require.NoError(t, bucket.Put([]byte("key"), []byte("rolled-back")))
	tx.Rollback()
	requireBucketValue(t, db, []byte("cache"), []byte("key"), []byte("old"))

	require.NoError(t, db.Update(func(tx *Tx) error {
		bucket, getErr := tx.GetBucket([]byte("cache"))
		if getErr != nil {
			return getErr
		}
		return bucket.Put([]byte("key"), []byte("committed"))
	}))
	requireBucketValue(t, db, []byte("cache"), []byte("key"), []byte("committed"))
}
