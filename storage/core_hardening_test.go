package storage

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBucketRemoveDescendingRootChangePersists(t *testing.T) {
	db, filename := CreateTestDB(t)
	const total = 2000
	const firstRemoved = 159

	err := db.Update(func(tx *Tx) error {
		bucket, createErr := tx.CreateBucket([]byte("descending"))
		if createErr != nil {
			return createErr
		}
		for i := 0; i < total; i++ {
			key := []byte(fmt.Sprintf("%05d", i))
			if putErr := bucket.Put(key, key); putErr != nil {
				return putErr
			}
		}
		return nil
	})
	require.NoError(t, err)

	err = db.Update(func(tx *Tx) error {
		bucket, getErr := tx.GetBucket([]byte("descending"))
		if getErr != nil {
			return getErr
		}
		for i := total - 1; i >= firstRemoved; i-- {
			if removeErr := bucket.Remove([]byte(fmt.Sprintf("%05d", i))); removeErr != nil {
				return fmt.Errorf("remove %d: %w", i, removeErr)
			}
		}
		for _, pageNum := range tx.pagesToDelete {
			if pageNum == bucket.root {
				return fmt.Errorf("live bucket root %d is scheduled for deletion", bucket.root)
			}
		}
		return nil
	})
	require.NoError(t, err)

	CloseTestDB(t, db)
	db = OpenTestDB(t, filename, nil)
	err = db.View(func(tx *Tx) error {
		bucket, getErr := tx.GetBucket([]byte("descending"))
		if getErr != nil {
			return getErr
		}
		for i := 0; i < firstRemoved; i++ {
			key := []byte(fmt.Sprintf("%05d", i))
			value, found := bucket.Get(key)
			if !found || !bytes.Equal(key, value) {
				return fmt.Errorf("surviving key %d missing or changed", i)
			}
		}
		return nil
	})
	require.NoError(t, err)
}

func TestCatalogRootCollapseAfterMassBucketDeletionPersists(t *testing.T) {
	db, filename := CreateTestDB(t)
	const total = 400
	const survivors = 11
	require.NoError(t, db.Update(func(tx *Tx) error {
		for idx := 0; idx < total; idx++ {
			if _, err := tx.CreateBucket([]byte(fmt.Sprintf("bucket-%04d", idx))); err != nil {
				return err
			}
		}
		return nil
	}))
	require.NoError(t, db.Update(func(tx *Tx) error {
		for idx := total - 1; idx >= survivors; idx-- {
			if err := tx.DeleteBucket([]byte(fmt.Sprintf("bucket-%04d", idx))); err != nil {
				return err
			}
		}
		return nil
	}))

	CloseTestDB(t, db)
	db = OpenTestDB(t, filename, nil)
	require.NoError(t, db.View(func(tx *Tx) error {
		buckets, err := tx.BucketsE()
		if err != nil {
			return err
		}
		require.Len(t, buckets, survivors)
		for idx, name := range buckets {
			require.Equal(t, fmt.Sprintf("bucket-%04d", idx), string(name))
		}
		return nil
	}))
	_, err := db.Check()
	require.NoError(t, err)
}

func TestBucketMutatorsRejectReadTransactionBeforeAllocation(t *testing.T) {
	db, _ := CreateTestDB(t)
	require.NoError(t, db.Update(func(tx *Tx) error {
		bucket, err := tx.CreateBucket([]byte("readonly"))
		if err != nil {
			return err
		}
		return bucket.Put([]byte("existing"), []byte("value"))
	}))

	tx := db.Begin(false)
	defer tx.Rollback()
	bucket, err := tx.GetBucket([]byte("readonly"))
	require.NoError(t, err)
	currentPage := db.dal.freelist.currentPage
	releasedExtents := append([]pageExtent(nil), db.dal.freelist.releasedExtents...)
	releasedCount := db.dal.freelist.releasedPageN()

	require.ErrorIs(t, bucket.Put([]byte("small"), []byte("value")), ErrWriteInRxTransaction)
	require.ErrorIs(t, bucket.Put([]byte("blob"), bytes.Repeat([]byte("x"), MaxValueSize+1)), ErrWriteInRxTransaction)
	require.ErrorIs(t, bucket.PutReader([]byte("reader"), bytes.NewReader(bytes.Repeat([]byte("y"), MaxValueSize+1)), MaxValueSize+1), ErrWriteInRxTransaction)
	require.ErrorIs(t, bucket.Remove([]byte("existing")), ErrWriteInRxTransaction)
	require.Empty(t, tx.allocatedPageNums)
	require.Empty(t, tx.dirtyNodes)
	require.Empty(t, tx.dirtyPages)
	require.Equal(t, currentPage, db.dal.freelist.currentPage)
	require.ElementsMatch(t, releasedExtents, db.dal.freelist.releasedExtents)
	require.Equal(t, releasedCount, db.dal.freelist.releasedPageN())
}

func TestBucketCopiesCallerOwnedKeysNamesAndValues(t *testing.T) {
	db, _ := CreateTestDB(t)
	bucketName := []byte("alpha")
	key := []byte("key-1")
	require.NoError(t, db.Update(func(tx *Tx) error {
		bucket, err := tx.CreateBucket(bucketName)
		if err != nil {
			return err
		}
		if err = bucket.Put(key, []byte("original")); err != nil {
			return err
		}
		copy(bucketName, []byte("omega"))
		copy(key, []byte("key-2"))
		return nil
	}))

	require.NoError(t, db.View(func(tx *Tx) error {
		bucket, err := tx.GetBucket([]byte("alpha"))
		if err != nil {
			return err
		}
		value, found := bucket.Get([]byte("key-1"))
		require.True(t, found)
		require.Equal(t, []byte("original"), value)
		value[0] = 'X'
		again, found := bucket.Get([]byte("key-1"))
		require.True(t, found)
		require.Equal(t, []byte("original"), again)
		_, err = tx.GetBucket([]byte("omega"))
		require.ErrorIs(t, err, ErrBucketNotFound)
		return nil
	}))

	lookupName := []byte("alpha")
	require.NoError(t, db.Update(func(tx *Tx) error {
		_, err := tx.GetBucket(lookupName)
		copy(lookupName, []byte("omega"))
		return err
	}))
	require.NoError(t, db.View(func(tx *Tx) error {
		_, err := tx.GetBucket([]byte("omega"))
		require.ErrorIs(t, err, ErrBucketNotFound)
		_, err = tx.GetBucket([]byte("alpha"))
		return err
	}))
}

func TestBucketRejectsUseAfterTransactionClose(t *testing.T) {
	db, _ := CreateTestDB(t)
	tx := db.Begin(true)
	bucket, err := tx.CreateBucket([]byte("closed"))
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	require.ErrorIs(t, bucket.Put([]byte("key"), []byte("value")), ErrTxClosed)
	require.ErrorIs(t, bucket.Remove([]byte("key")), ErrTxClosed)
	_, _, err = bucket.ValueLen([]byte("key"))
	require.True(t, errors.Is(err, ErrTxClosed))
}

func TestInvalidBucketNameDoesNotAllocateOrPoisonTransaction(t *testing.T) {
	db, _ := CreateTestDB(t)
	tx := db.Begin(true)
	before := db.dal.freelist.currentPage
	_, err := tx.CreateBucket(nil)
	require.ErrorIs(t, err, ErrInvalidBucketName)
	_, err = tx.CreateBucket(bytes.Repeat([]byte("x"), MaxKeySize))
	require.ErrorIs(t, err, ErrInvalidBucketName)
	require.Equal(t, before, db.dal.freelist.currentPage)
	require.Empty(t, tx.allocatedPageNums)
	_, err = tx.CreateBucket([]byte("valid"))
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
}

func TestIntegrityCheckValidatesTreesBlobsStatsAndFreelist(t *testing.T) {
	db, _ := CreateTestDB(t)
	require.NoError(t, db.Update(func(tx *Tx) error {
		first, err := tx.CreateBucket([]byte("first"))
		if err != nil {
			return err
		}
		for i := 0; i < 1000; i++ {
			key := []byte(fmt.Sprintf("%05d", i))
			if err = first.Put(key, []byte("value")); err != nil {
				return err
			}
		}
		second, err := tx.CreateBucket([]byte("second"))
		if err != nil {
			return err
		}
		return second.Put([]byte("blob"), bytes.Repeat([]byte("z"), MaxValueSize+5000))
	}))

	report, err := db.Check()
	require.NoError(t, err)
	require.True(t, report.PageChecksums)
	require.EqualValues(t, 2, report.BucketCount)
	require.EqualValues(t, 1001, report.ItemCount)
	require.Greater(t, report.BTreePages, uint64(2))
	require.Greater(t, report.BlobPages, uint64(0))
	require.Zero(t, report.OrphanPageCount)
}

func TestIntegrityCheckRejectsLivePageInFreelist(t *testing.T) {
	db, _ := CreateTestDB(t)
	var bucketRoot uint64
	require.NoError(t, db.Update(func(tx *Tx) error {
		bucket, err := tx.CreateBucket([]byte("live"))
		if err != nil {
			return err
		}
		bucketRoot = bucket.root
		return bucket.Put([]byte("key"), []byte("value"))
	}))
	require.True(t, db.dal.freelist.addReleasedPage(bucketRoot))
	_, err := db.Check()
	require.ErrorIs(t, err, ErrIntegrityCheckFailed)
}

func TestIntegrityCheckReportsOrphanedAllocatedPage(t *testing.T) {
	db, _ := CreateTestDB(t)
	page, err := db.dal.AllocatePage()
	require.NoError(t, err)
	require.NotZero(t, page.PageNumber)
	report, err := db.Check()
	require.ErrorIs(t, err, ErrIntegrityCheckFailed)
	require.EqualValues(t, 1, report.OrphanPageCount)
	require.Contains(t, report.OrphanPages, page.PageNumber)
}

func TestFailedBlobWritePoisonsTransactionAndRollsBackPages(t *testing.T) {
	path := TempFileName(".db")
	opts := DefaultOptions().WithMaxTransactionBytes(4 * BTreePageSize)
	db, err := Open(path, opts)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = db.Close()
		_ = os.Remove(path)
		_ = os.Remove(db.dal.opts.TxLogPath)
	})
	require.NoError(t, db.Update(func(tx *Tx) error {
		_, createErr := tx.CreateBucket([]byte("bounded"))
		return createErr
	}))
	tx := db.Begin(true)
	bucket, err := tx.GetBucket([]byte("bounded"))
	require.NoError(t, err)
	currentPage := db.dal.freelist.currentPage
	fileSize := db.dal.size
	err = bucket.PutReader([]byte("too-large"), bytes.NewReader(bytes.Repeat([]byte("x"), 8*BTreePageSize)), 8*BTreePageSize)
	require.ErrorIs(t, err, ErrTransactionTooLarge)
	require.Empty(t, tx.allocatedPageNums)
	require.Equal(t, currentPage, db.dal.freelist.currentPage)
	require.Equal(t, fileSize, db.dal.size)
	require.ErrorIs(t, tx.Commit(), ErrTransactionTooLarge)
	_, err = db.Check()
	require.NoError(t, err)
	require.NoError(t, db.View(func(tx *Tx) error {
		bucket, getErr := tx.GetBucket([]byte("bounded"))
		if getErr != nil {
			return getErr
		}
		_, found := bucket.Get([]byte("too-large"))
		require.False(t, found)
		return nil
	}))
}

func TestInvalidKeyDoesNotPoisonTransactionOrAllocateBlob(t *testing.T) {
	db, _ := CreateTestDB(t)
	require.NoError(t, db.Update(func(tx *Tx) error {
		bucket, err := tx.CreateBucket([]byte("validation"))
		if err != nil {
			return err
		}
		beforeAllocated := len(tx.allocatedPageNums)
		err = bucket.Put(bytes.Repeat([]byte("k"), MaxKeySize), bytes.Repeat([]byte("v"), MaxValueSize+5000))
		require.ErrorIs(t, err, ErrKeyTooLarge)
		require.Len(t, tx.allocatedPageNums, beforeAllocated)
		return bucket.Put([]byte("valid"), []byte("value"))
	}))
}

func TestJournalFailureDoesNotRetainAbortedPendingPages(t *testing.T) {
	path := TempFileName(".db")
	opts := DefaultOptions().WithSyncPolicy(SyncPolicyJournal).WithCheckpointTxThreshold(1)
	db, err := Open(path, opts)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = db.Close()
		_ = os.Remove(path)
		_ = os.Remove(db.dal.opts.TxLogPath)
	})
	require.NoError(t, db.Update(func(tx *Tx) error {
		bucket, createErr := tx.CreateBucket([]byte("journal"))
		if createErr != nil {
			return createErr
		}
		return bucket.Put([]byte("stable"), []byte("v1"))
	}))
	require.Empty(t, db.dal.pendingJournal)
	db.dal.opts.CheckpointTxThreshold = 100

	failOnce := true
	db.dal.txLog.beforeWriteHook = func() error {
		if failOnce {
			failOnce = false
			return fmt.Errorf("simulated journal replacement failure")
		}
		return nil
	}
	err = db.Update(func(tx *Tx) error {
		bucket, getErr := tx.GetBucket([]byte("journal"))
		if getErr != nil {
			return getErr
		}
		return bucket.Put([]byte("aborted"), []byte("must-not-survive"))
	})
	require.ErrorContains(t, err, "simulated journal replacement failure")
	require.Empty(t, db.dal.pendingJournal)
	require.Zero(t, db.dal.pendingJournalTxN)
	db.dal.txLog.beforeWriteHook = nil

	require.NoError(t, db.Update(func(tx *Tx) error {
		bucket, getErr := tx.GetBucket([]byte("journal"))
		if getErr != nil {
			return getErr
		}
		return bucket.Put([]byte("committed"), []byte("v2"))
	}))
	require.NoError(t, db.View(func(tx *Tx) error {
		bucket, getErr := tx.GetBucket([]byte("journal"))
		if getErr != nil {
			return getErr
		}
		_, found := bucket.Get([]byte("aborted"))
		require.False(t, found)
		value, found := bucket.Get([]byte("committed"))
		require.True(t, found)
		require.Equal(t, []byte("v2"), value)
		return nil
	}))
}

func TestPostWALFailurePoisonsDatabaseUntilRecovery(t *testing.T) {
	db, _ := CreateTestDB(t)
	tx := db.Begin(true)
	bucket, err := tx.CreateBucket([]byte("fatal"))
	require.NoError(t, err)
	require.NoError(t, bucket.Put([]byte("key"), []byte("value")))
	failOnce := true
	db.dal.beforeSetPageHook = func(_ *Page) error {
		if !db.dal.txLog.active && failOnce {
			failOnce = false
			return fmt.Errorf("post-WAL checkpoint failure")
		}
		return nil
	}
	err = tx.Commit()
	require.ErrorContains(t, err, "post-WAL checkpoint failure")
	db.dal.beforeSetPageHook = nil
	_, err = db.BeginE(false)
	require.ErrorIs(t, err, ErrDatabaseFatal)
	require.ErrorIs(t, db.Update(func(*Tx) error { return nil }), ErrDatabaseFatal)
}

func TestCloseWaitsForActiveTransactionAndRejectsNewWork(t *testing.T) {
	db, _ := CreateTestDB(t)
	tx := db.Begin(false)
	closed := make(chan error, 1)
	go func() { closed <- db.Close() }()
	select {
	case err := <-closed:
		require.Failf(t, "close returned while transaction was active", "error: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	tx.Rollback()
	require.NoError(t, <-closed)
	_, err := db.BeginE(false)
	require.ErrorIs(t, err, ErrDatabaseClosed)
}
