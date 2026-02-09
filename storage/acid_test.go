package storage

import (
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func txLogSize(t *testing.T, db *DB) int64 {
	t.Helper()
	info, err := os.Stat(db.dal.opts.TxLogPath)
	require.NoError(t, err)
	return info.Size()
}

func requireBucketNotFound(t *testing.T, db *DB, bucketName []byte) {
	t.Helper()
	err := db.View(func(tx *Tx) error {
		_, err := tx.GetBucket(bucketName)
		require.ErrorIs(t, err, ErrBucketNotFound)
		return nil
	})
	require.NoError(t, err)
}

func requireBucketValue(t *testing.T, db *DB, bucketName, key, expected []byte) {
	t.Helper()
	err := db.View(func(tx *Tx) error {
		bucket, err := tx.GetBucket(bucketName)
		require.NoError(t, err)
		value, found := bucket.Get(key)
		require.True(t, found)
		require.Equal(t, expected, value)
		return nil
	})
	require.NoError(t, err)
}

func TestACIDAtomicity_UpdateCallbackErrorRollsBack(t *testing.T) {
	db, _ := CreateTestDB(t)
	freeBefore := db.dal.freelist.availablePageN()
	expectedErr := errors.New("force rollback")

	err := db.Update(func(tx *Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("users"))
		require.NoError(t, err)
		require.NoError(t, bucket.Put([]byte("id"), []byte("1")))
		return expectedErr
	})
	require.ErrorIs(t, err, expectedErr)

	requireBucketNotFound(t, db, []byte("users"))
	require.Equal(t, freeBefore, db.dal.freelist.availablePageN())
	require.Equal(t, int64(0), txLogSize(t, db))
}

func TestACIDAtomicity_TxLogPhaseFailureAbortsCheckpoint(t *testing.T) {
	db, filename := CreateTestDB(t)
	freeBefore := db.dal.freelist.availablePageN()

	failOnce := true
	db.dal.beforeSetPageHook = func(_ *Page) error {
		if db.dal.txLog.active && failOnce {
			failOnce = false
			return fmt.Errorf("simulated txlog write failure")
		}
		return nil
	}

	err := db.Update(func(tx *Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("users"))
		require.NoError(t, err)
		return bucket.Put([]byte("id"), []byte("1234"))
	})
	require.ErrorContains(t, err, "simulated txlog write failure")
	db.dal.beforeSetPageHook = nil

	require.Equal(t, freeBefore, db.dal.freelist.availablePageN())
	require.Equal(t, int64(0), txLogSize(t, db))
	requireBucketNotFound(t, db, []byte("users"))

	CloseTestDB(t, db)
	db = OpenTestDB(t, filename, DefaultOptions().WithRecovery(true))
	requireBucketNotFound(t, db, []byte("users"))
}

func TestACIDDurability_SecondPhaseFailureRecoversOnOpen(t *testing.T) {
	db, filename := CreateTestDB(t)

	tx := db.Begin(true)
	bucket, err := tx.CreateBucketIfNotExists([]byte("users"))
	require.NoError(t, err)
	require.NoError(t, bucket.Put([]byte("id"), []byte("1234")))

	failOnce := true
	db.dal.beforeSetPageHook = func(_ *Page) error {
		if !db.dal.txLog.active && failOnce {
			failOnce = false
			return fmt.Errorf("simulated crash during db write phase")
		}
		return nil
	}

	err = tx.Commit()
	require.ErrorContains(t, err, "simulated crash during db write phase")
	db.dal.beforeSetPageHook = nil
	require.Greater(t, txLogSize(t, db), int64(0))

	CloseTestDB(t, db)
	db = OpenTestDB(t, filename, DefaultOptions().WithRecovery(false))
	requireBucketNotFound(t, db, []byte("users"))

	CloseTestDB(t, db)
	db = OpenTestDB(t, filename, DefaultOptions().WithRecovery(true))
	requireBucketValue(t, db, []byte("users"), []byte("id"), []byte("1234"))
	require.Equal(t, int64(0), txLogSize(t, db))
}

func TestACIDDurability_SuccessfulCommitClearsTxLog(t *testing.T) {
	db, filename := CreateTestDB(t)

	err := db.Update(func(tx *Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("users"))
		if err != nil {
			return err
		}
		return bucket.Put([]byte("id"), []byte("committed"))
	})
	require.NoError(t, err)
	require.Equal(t, int64(0), txLogSize(t, db))

	CloseTestDB(t, db)
	db = OpenTestDB(t, filename, DefaultOptions().WithRecovery(true))
	requireBucketValue(t, db, []byte("users"), []byte("id"), []byte("committed"))
	require.Equal(t, int64(0), txLogSize(t, db))
}

func TestACIDConsistency_FailedWriteKeepsPreviousValue(t *testing.T) {
	db, _ := CreateTestDB(t)

	err := db.Update(func(tx *Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("users"))
		if err != nil {
			return err
		}
		return bucket.Put([]byte("id"), []byte("v1"))
	})
	require.NoError(t, err)

	expectedErr := errors.New("abort after mutation")
	err = db.Update(func(tx *Tx) error {
		bucket, err := tx.GetBucket([]byte("users"))
		require.NoError(t, err)
		require.NoError(t, bucket.Put([]byte("id"), []byte("v2")))
		require.NoError(t, bucket.Put([]byte("temp"), []byte("transient")))
		return expectedErr
	})
	require.ErrorIs(t, err, expectedErr)

	requireBucketValue(t, db, []byte("users"), []byte("id"), []byte("v1"))
	err = db.View(func(tx *Tx) error {
		bucket, err := tx.GetBucket([]byte("users"))
		require.NoError(t, err)
		_, found := bucket.Get([]byte("temp"))
		require.False(t, found)
		return nil
	})
	require.NoError(t, err)
}

func TestACIDIsolation_ReadTransactionBlocksWriter(t *testing.T) {
	db, _ := CreateTestDB(t)

	err := db.Update(func(tx *Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("users"))
		if err != nil {
			return err
		}
		return bucket.Put([]byte("id"), []byte("v1"))
	})
	require.NoError(t, err)

	readTx := db.Begin(false)
	bucket, err := readTx.GetBucket([]byte("users"))
	require.NoError(t, err)
	value, found := bucket.Get([]byte("id"))
	require.True(t, found)
	require.Equal(t, []byte("v1"), value)

	writerDone := make(chan error, 1)
	go func() {
		writerDone <- db.Update(func(tx *Tx) error {
			writerBucket, writerErr := tx.GetBucket([]byte("users"))
			if writerErr != nil {
				return writerErr
			}
			return writerBucket.Put([]byte("id"), []byte("v2"))
		})
	}()

	select {
	case writerErr := <-writerDone:
		require.Failf(t, "writer finished while read tx is active", "unexpected writer result: %v", writerErr)
	case <-time.After(100 * time.Millisecond):
	}

	value, found = bucket.Get([]byte("id"))
	require.True(t, found)
	require.Equal(t, []byte("v1"), value)

	readTx.Rollback()
	require.NoError(t, <-writerDone)
	requireBucketValue(t, db, []byte("users"), []byte("id"), []byte("v2"))
}
