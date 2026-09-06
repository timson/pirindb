package storage

import (
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func txLogActive(t *testing.T, db *DB) bool {
	t.Helper()
	active, err := db.dal.txLog.HasActiveJournal()
	require.NoError(t, err)
	return active
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
	require.False(t, txLogActive(t, db))
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
	require.False(t, txLogActive(t, db))
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
	require.True(t, txLogActive(t, db))

	CloseTestDB(t, db)
	db = OpenTestDB(t, filename, DefaultOptions().WithRecovery(false))
	require.ErrorIs(t, db.View(func(*Tx) error { return nil }), ErrRecoveryRequired)
	require.ErrorIs(t, db.Update(func(*Tx) error { return nil }), ErrRecoveryRequired)

	CloseTestDB(t, db)
	db = OpenTestDB(t, filename, DefaultOptions().WithRecovery(true))
	requireBucketValue(t, db, []byte("users"), []byte("id"), []byte("1234"))
	require.False(t, txLogActive(t, db))
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
	require.False(t, txLogActive(t, db))

	CloseTestDB(t, db)
	db = OpenTestDB(t, filename, DefaultOptions().WithRecovery(true))
	requireBucketValue(t, db, []byte("users"), []byte("id"), []byte("committed"))
	require.False(t, txLogActive(t, db))
}

func TestNoOpWriteTransactionSkipsJournalAndSyncWork(t *testing.T) {
	path := TempFileName(".db")
	db, err := Open(path, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = db.Close()
		_ = os.Remove(path)
		_ = os.Remove(db.dal.opts.TxLogPath)
	})
	before, err := db.dal.txLog.Size()
	require.NoError(t, err)
	require.NoError(t, db.Update(func(*Tx) error { return nil }))
	after, err := db.dal.txLog.Size()
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.False(t, txLogActive(t, db))
}

func TestJournalPolicyLeavesJournalActiveUntilCheckpoint(t *testing.T) {
	path := TempFileName(".db")
	opts := DefaultOptions().
		WithSyncPolicy(SyncPolicyJournal).
		WithCheckpointTxThreshold(2)

	db, err := Open(path, opts)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = db.Close()
		_ = os.Remove(path)
		_ = os.Remove(db.dal.opts.TxLogPath)
	})

	err = db.Update(func(tx *Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("users"))
		require.NoError(t, err)
		return bucket.Put([]byte("id1"), []byte("v1"))
	})
	require.NoError(t, err)
	require.True(t, txLogActive(t, db))

	err = db.Update(func(tx *Tx) error {
		bucket, err := tx.GetBucket([]byte("users"))
		require.NoError(t, err)
		return bucket.Put([]byte("id2"), []byte("v2"))
	})
	require.NoError(t, err)
	require.False(t, txLogActive(t, db))

	requireBucketValue(t, db, []byte("users"), []byte("id1"), []byte("v1"))
	requireBucketValue(t, db, []byte("users"), []byte("id2"), []byte("v2"))
}

func TestJournalPolicyRecoveryBeforeCheckpoint(t *testing.T) {
	path := TempFileName(".db")
	opts := DefaultOptions().
		WithSyncPolicy(SyncPolicyJournal).
		WithCheckpointTxThreshold(100)

	db, err := Open(path, opts)
	require.NoError(t, err)

	err = db.Update(func(tx *Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("users"))
		require.NoError(t, err)
		return bucket.Put([]byte("id"), []byte("journal-only"))
	})
	require.NoError(t, err)
	require.True(t, txLogActive(t, db))

	CloseTestDB(t, db)
	db = OpenTestDB(t, path, opts)
	requireBucketValue(t, db, []byte("users"), []byte("id"), []byte("journal-only"))
	require.False(t, txLogActive(t, db))

	t.Cleanup(func() {
		_ = os.Remove(path)
		_ = os.Remove(db.dal.opts.TxLogPath)
	})
}

func TestJournalReplacementFailurePreservesEarlierCommittedJournal(t *testing.T) {
	path := TempFileName(".db")
	opts := DefaultOptions().
		WithSyncPolicy(SyncPolicyJournal).
		WithCheckpointTxThreshold(100)
	db, err := Open(path, opts)
	require.NoError(t, err)
	tlogPath := db.dal.opts.TxLogPath
	t.Cleanup(func() {
		_ = os.Remove(path)
		_ = os.Remove(tlogPath)
	})

	require.NoError(t, db.Update(func(tx *Tx) error {
		bucket, createErr := tx.CreateBucketIfNotExists([]byte("journal"))
		if createErr != nil {
			return createErr
		}
		return bucket.Put([]byte("first"), []byte("durable"))
	}))
	require.True(t, txLogActive(t, db))

	failOnce := true
	db.dal.txLog.beforeWriteHook = func() error {
		if failOnce {
			failOnce = false
			return errors.New("replace journal failed")
		}
		return nil
	}
	err = db.Update(func(tx *Tx) error {
		bucket, getErr := tx.GetBucket([]byte("journal"))
		if getErr != nil {
			return getErr
		}
		return bucket.Put([]byte("second"), []byte("not-committed"))
	})
	require.ErrorContains(t, err, "replace journal failed")
	db.dal.txLog.beforeWriteHook = nil
	require.True(t, txLogActive(t, db), "the first commit's recovery journal must remain active")

	// Close files directly to model a process exit without a graceful
	// checkpoint, then prove recovery retained only the acknowledged update.
	require.NoError(t, db.dal.Close())
	db, err = Open(path, opts)
	require.NoError(t, err)
	defer db.Close()
	requireBucketValue(t, db, []byte("journal"), []byte("first"), []byte("durable"))
	require.NoError(t, db.View(func(tx *Tx) error {
		bucket, getErr := tx.GetBucket([]byte("journal"))
		if getErr != nil {
			return getErr
		}
		_, found, getErr := bucket.GetE([]byte("second"))
		require.NoError(t, getErr)
		require.False(t, found)
		return nil
	}))
}

func TestGroupPolicyBatchesTransactionsAndBlocksReaders(t *testing.T) {
	path := TempFileName(".db")
	opts := DefaultOptions().
		WithSyncPolicy(SyncPolicyGroup).
		WithGroupCommitTxThreshold(2).
		WithGroupCommitWindow(50 * time.Millisecond)

	db, err := Open(path, opts)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = db.Close()
		_ = os.Remove(path)
		_ = os.Remove(db.dal.opts.TxLogPath)
	})

	firstDone := make(chan error, 1)
	go func() {
		firstDone <- db.Update(func(tx *Tx) error {
			bucket, err := tx.CreateBucketIfNotExists([]byte("users"))
			require.NoError(t, err)
			return bucket.Put([]byte("id1"), []byte("v1"))
		})
	}()

	select {
	case err = <-firstDone:
		require.Failf(t, "first group commit finished too early", "unexpected result: %v", err)
	case <-time.After(10 * time.Millisecond):
	}

	readDone := make(chan error, 1)
	go func() {
		readDone <- db.View(func(tx *Tx) error {
			bucket, err := tx.GetBucket([]byte("users"))
			if err != nil {
				return err
			}
			value, found := bucket.Get([]byte("id1"))
			require.True(t, found)
			require.Equal(t, []byte("v1"), value)
			return nil
		})
	}()

	select {
	case err = <-readDone:
		require.Failf(t, "reader finished while group batch is pending", "unexpected result: %v", err)
	case <-time.After(10 * time.Millisecond):
	}

	err = db.Update(func(tx *Tx) error {
		bucket, err := tx.GetBucket([]byte("users"))
		require.NoError(t, err)
		return bucket.Put([]byte("id2"), []byte("v2"))
	})
	require.NoError(t, err)
	require.NoError(t, <-firstDone)
	require.NoError(t, <-readDone)
	require.False(t, txLogActive(t, db))
	requireBucketValue(t, db, []byte("users"), []byte("id1"), []byte("v1"))
	requireBucketValue(t, db, []byte("users"), []byte("id2"), []byte("v2"))
}

func TestGroupPolicyReaderArrivingBeforeBatchFormationDoesNotDeadlock(t *testing.T) {
	path := TempFileName(".db")
	opts := DefaultOptions().
		WithSyncPolicy(SyncPolicyGroup).
		WithGroupCommitTxThreshold(1).
		WithGroupCommitWindow(time.Second)
	db, err := Open(path, opts)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = db.Close()
		_ = os.Remove(path)
		_ = os.Remove(db.dal.opts.TxLogPath)
	})

	writerReady := make(chan struct{})
	releaseWriter := make(chan struct{})
	writerDone := make(chan error, 1)
	go func() {
		writerDone <- db.Update(func(tx *Tx) error {
			bucket, createErr := tx.CreateBucketIfNotExists([]byte("handshake"))
			if createErr != nil {
				return createErr
			}
			close(writerReady)
			<-releaseWriter
			return bucket.Put([]byte("key"), []byte("value"))
		})
	}()
	<-writerReady

	readerDone := make(chan error, 1)
	go func() {
		readerDone <- db.View(func(*Tx) error { return nil })
	}()
	time.Sleep(10 * time.Millisecond)
	close(releaseWriter)

	select {
	case err = <-writerDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("group writer deadlocked while forming its batch")
	}
	select {
	case err = <-readerDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("reader remained blocked after group batch flush")
	}
}

func TestGroupPolicyFlushFailureDoesNotIndependentlyRollbackSharedFreelist(t *testing.T) {
	path := TempFileName(".db")
	opts := DefaultOptions().
		WithSyncPolicy(SyncPolicyGroup).
		WithGroupCommitTxThreshold(2).
		WithGroupCommitWindow(time.Second)
	db, err := Open(path, opts)
	require.NoError(t, err)
	tlogPath := db.dal.opts.TxLogPath
	t.Cleanup(func() {
		_ = os.Remove(path)
		_ = os.Remove(tlogPath)
	})
	db.dal.txLog.beforeWriteHook = func() error { return errors.New("group journal failed") }

	firstDone := make(chan error, 1)
	go func() {
		firstDone <- db.Update(func(tx *Tx) error {
			bucket, createErr := tx.CreateBucketIfNotExists([]byte("group-a"))
			if createErr != nil {
				return createErr
			}
			return bucket.Put([]byte("key"), []byte("value"))
		})
	}()
	time.Sleep(10 * time.Millisecond)
	secondErr := db.Update(func(tx *Tx) error {
		bucket, createErr := tx.CreateBucketIfNotExists([]byte("group-b"))
		if createErr != nil {
			return createErr
		}
		return bucket.Put([]byte("key"), []byte("value"))
	})
	firstErr := <-firstDone
	require.ErrorContains(t, firstErr, "group journal failed")
	require.ErrorContains(t, secondErr, "group journal failed")
	require.ErrorIs(t, db.Update(func(*Tx) error { return nil }), ErrDatabaseFatal)

	db.dal.txLog.beforeWriteHook = nil
	require.NoError(t, db.Close())
	db, err = Open(path, opts)
	require.NoError(t, err)
	defer db.Close()
	requireBucketNotFound(t, db, []byte("group-a"))
	requireBucketNotFound(t, db, []byte("group-b"))
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
