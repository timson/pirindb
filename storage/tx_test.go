package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTxRollbackCreateBucket(t *testing.T) {
	db, _ := CreateTestDB(t)
	tx := db.Begin(true)
	bucket, err := tx.CreateBucket([]byte("test"))
	require.NoError(t, err)

	for i := 0; i < 5000; i++ {
		data := make([]byte, 8)
		binary.BigEndian.PutUint64(data, uint64(i))
		err = bucket.Put(data, []byte(fmt.Sprintf("test_%d", i)))
		require.NoError(t, err)
	}

	tx.Rollback()

	tx = db.Begin(false)
	defer tx.Rollback()
	bucket, err = tx.GetBucket([]byte("test"))
	require.Error(t, err)
}

func TestTxRollbackMultiInserts(t *testing.T) {
	db, _ := CreateTestDB(t)
	tx := db.Begin(true)
	bucket, err := tx.CreateBucket([]byte("test"))
	require.NoError(t, err)
	err = bucket.Put([]byte("foo"), []byte("bar"))
	require.NoError(t, err)
	err = tx.Commit()
	require.NoError(t, err)

	tx = db.Begin(true)
	bucket, err = tx.GetBucket([]byte("test"))
	require.NoError(t, err)
	for i := 0; i < 5000; i++ {
		data := make([]byte, 8)
		binary.BigEndian.PutUint64(data, uint64(i))
		err = bucket.Put([]byte(fmt.Sprintf("test_%d", i)), data)
		require.NoError(t, err)
	}
	tx.Rollback()

	err = db.View(func(tx *Tx) error {
		bucket, err = tx.GetBucket([]byte("test"))
		if err != nil {
			return err
		}
		for i := 0; i < 5000; i++ {
			_, found := bucket.Get([]byte(fmt.Sprintf("test_%d", i)))
			if found {
				return fmt.Errorf("expected to not find %d", i)
			}
		}
		return nil
	})
	require.NoError(t, err)
}

func TestTxIsolation(t *testing.T) {
	db, _ := CreateTestDB(t)

	// Insert initial key-value pair
	err := db.Update(func(tx *Tx) error {
		bucket, _ := tx.CreateBucket([]byte("foo"))
		return bucket.Put([]byte("key"), []byte("initial"))
	})
	require.NoError(t, err)

	startRead := make(chan struct{})
	releaseRead := make(chan struct{})
	readerDone := make(chan error, 1)
	go func() {
		readerDone <- db.View(func(tx *Tx) error {
			bucket, _ := tx.GetBucket([]byte("foo"))
			val, _ := bucket.Get([]byte("key"))

			require.Equal(t, []byte("initial"), val, "reader should see initial value")
			startRead <- struct{}{}
			<-releaseRead
			val2, _ := bucket.Get([]byte("key"))
			require.Equal(t, []byte("initial"), val2, "reader must still see consistent snapshot")

			return nil
		})
	}()

	<-startRead

	writeStarted := make(chan struct{})
	writeDone := make(chan error, 1)
	go func() {
		writeStarted <- struct{}{}
		writeDone <- db.Update(func(tx *Tx) error {
			bucket, _ := tx.GetBucket([]byte("foo"))
			return bucket.Put([]byte("key"), []byte("modified"))
		})
	}()

	<-writeStarted
	select {
	case errWriter := <-writeDone:
		require.Failf(t, "writer finished too early", "writer result: %v", errWriter)
	case <-time.After(100 * time.Millisecond):
	}
	releaseRead <- struct{}{}
	require.NoError(t, <-readerDone)
	require.NoError(t, <-writeDone)

	err = db.View(func(tx *Tx) error {
		bucket, _ := tx.GetBucket([]byte("foo"))
		val, _ := bucket.Get([]byte("key"))
		require.Equal(t, []byte("modified"), val, "final read should see modified value")
		return nil
	})
	require.NoError(t, err)
}

func TestTxConcurrencyIsolation(t *testing.T) {
	db, _ := CreateTestDB(t)

	// Set up initial data
	err := db.Update(func(tx *Tx) error {
		bucket, _ := tx.CreateBucket([]byte("foo"))
		return bucket.Put([]byte("key"), []byte("initial"))
	})
	require.NoError(t, err)

	const numReaders = 5
	var wg sync.WaitGroup

	wg.Add(numReaders)
	started := make(chan struct{}, numReaders)
	releaseReaders := make(chan struct{})

	for i := 0; i < numReaders; i++ {
		go func() {
			defer wg.Done()
			err := db.View(func(tx *Tx) error {
				started <- struct{}{}
				bucket, _ := tx.GetBucket([]byte("foo"))
				val, _ := bucket.Get([]byte("key"))
				require.Equal(t, []byte("initial"), val)
				<-releaseReaders
				return nil
			})
			require.NoError(t, err)
		}()
	}

	for i := 0; i < numReaders; i++ {
		<-started
	}

	writeDone := make(chan error, 1)
	go func() {
		writeDone <- db.Update(func(tx *Tx) error {
			bucket, _ := tx.GetBucket([]byte("foo"))
			return bucket.Put([]byte("key"), []byte("updated_by_writer"))
		})
	}()

	select {
	case errWriter := <-writeDone:
		require.Failf(t, "writer finished while readers are active", "writer result: %v", errWriter)
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseReaders)
	wg.Wait()
	require.NoError(t, <-writeDone)

	writer1Started := make(chan struct{})
	releaseWriter1 := make(chan struct{})
	writer1Done := make(chan error, 1)
	writer2Done := make(chan error, 1)

	go func() {
		writer1Done <- db.Update(func(tx *Tx) error {
			writer1Started <- struct{}{}
			bucket, _ := tx.GetBucket([]byte("foo"))
			<-releaseWriter1
			return bucket.Put([]byte("key"), []byte("writer1"))
		})
	}()

	<-writer1Started
	go func() {
		writer2Done <- db.Update(func(tx *Tx) error {
			bucket, _ := tx.GetBucket([]byte("foo"))
			return bucket.Put([]byte("key"), []byte("writer2"))
		})
	}()

	select {
	case errWriter2 := <-writer2Done:
		require.Failf(t, "writer2 finished before writer1 released lock", "writer2 result: %v", errWriter2)
	case <-time.After(100 * time.Millisecond):
	}

	close(releaseWriter1)
	require.NoError(t, <-writer1Done)
	require.NoError(t, <-writer2Done)

	err = db.View(func(tx *Tx) error {
		bucket, _ := tx.GetBucket([]byte("foo"))
		val, _ := bucket.Get([]byte("key"))
		require.NotNil(t, val)
		return nil
	})
	require.NoError(t, err)
}

func TestTxRollbackReleasesCreateBucketAllocatedPages(t *testing.T) {
	db, _ := CreateTestDB(t)
	before := db.dal.freelist.availablePageN()

	tx := db.Begin(true)
	_, err := tx.CreateBucket([]byte("rollback_bucket"))
	require.NoError(t, err)
	tx.Rollback()

	after := db.dal.freelist.availablePageN()
	require.Equal(t, before, after)
}

func TestTxRollbackReleasesBlobAllocatedPages(t *testing.T) {
	db, _ := CreateTestDB(t)
	before := db.dal.freelist.availablePageN()

	tx := db.Begin(true)
	bucket, err := tx.CreateBucket([]byte("blob_rollback"))
	require.NoError(t, err)
	err = bucket.Put([]byte("k"), bytes.Repeat([]byte("x"), MaxValueSize+512))
	require.NoError(t, err)
	tx.Rollback()

	after := db.dal.freelist.availablePageN()
	require.Equal(t, before, after)
}

func TestCreateBucketExistingDoesNotDirtyTxState(t *testing.T) {
	db, _ := CreateTestDB(t)
	err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateBucket([]byte("foo"))
		return err
	})
	require.NoError(t, err)

	tx := db.Begin(true)
	_, err = tx.CreateBucket([]byte("foo"))
	require.ErrorIs(t, err, ErrBucketExists)
	require.Equal(t, 0, len(tx.dirtyBuckets))
	require.Equal(t, 0, len(tx.allocatedPageNums))
	tx.Rollback()
}

func TestDeleteBucketReclaimsBlobAndNodePages(t *testing.T) {
	db, _ := CreateTestDB(t)
	before := db.dal.freelist.availablePageN()

	err := db.Update(func(tx *Tx) error {
		bucket, err := tx.CreateBucket([]byte("foo"))
		if err != nil {
			return err
		}
		if err = bucket.Put([]byte("k1"), bytes.Repeat([]byte("a"), MaxValueSize+600)); err != nil {
			return err
		}
		if err = bucket.Put([]byte("k2"), []byte("small")); err != nil {
			return err
		}
		return nil
	})
	require.NoError(t, err)
	require.Less(t, db.dal.freelist.availablePageN(), before)

	err = db.Update(func(tx *Tx) error {
		return tx.DeleteBucket([]byte("foo"))
	})
	require.NoError(t, err)
	require.Equal(t, before, db.dal.freelist.availablePageN())

	err = db.View(func(tx *Tx) error {
		_, err := tx.GetBucket([]byte("foo"))
		require.ErrorIs(t, err, ErrBucketNotFound)
		return nil
	})
	require.NoError(t, err)
}

func TestDeleteBucketAfterReadOrWriteInSameTx(t *testing.T) {
	db, _ := CreateTestDB(t)
	err := db.Update(func(tx *Tx) error {
		bucket, err := tx.CreateBucket([]byte("foo"))
		if err != nil {
			return err
		}
		if err = bucket.Put([]byte("k"), []byte("v")); err != nil {
			return err
		}
		return nil
	})
	require.NoError(t, err)

	err = db.Update(func(tx *Tx) error {
		bucket, err := tx.GetBucket([]byte("foo"))
		if err != nil {
			return err
		}
		if err = bucket.Put([]byte("k2"), bytes.Repeat([]byte("x"), MaxValueSize+333)); err != nil {
			return err
		}
		return tx.DeleteBucket([]byte("foo"))
	})
	require.NoError(t, err)

	err = db.View(func(tx *Tx) error {
		_, err := tx.GetBucket([]byte("foo"))
		require.ErrorIs(t, err, ErrBucketNotFound)
		return nil
	})
	require.NoError(t, err)
}

func TestCommitAsyncPublishesOrderedGroupTransactionsBeforeWaiting(t *testing.T) {
	path := TempFileName(".db")
	opts := DefaultOptions().
		WithSyncPolicy(SyncPolicyGroup).
		WithGroupCommitTxThreshold(8).
		WithGroupCommitWindow(time.Second)
	db, err := Open(path, opts)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = db.Close()
		_ = os.Remove(path)
		_ = os.Remove(opts.TxLogPath)
	})

	futures := make([]*CommitFuture, 0, 8)
	started := time.Now()
	for idx := 0; idx < 8; idx++ {
		tx := db.Begin(true)
		bucket, createErr := tx.CreateBucketIfNotExists([]byte("async"))
		require.NoError(t, createErr)
		require.NoError(t, bucket.Put([]byte(fmt.Sprintf("%02d", idx)), []byte("value")))
		futures = append(futures, tx.CommitAsync())
	}
	require.Less(t, time.Since(started), 500*time.Millisecond, "submissions should not wait for the group window")
	for _, future := range futures {
		require.NoError(t, future.Wait())
		require.NoError(t, future.Wait(), "commit futures must support repeated waits")
	}
	require.Equal(t, uint64(1), db.GroupCommitBatchCount())
	require.NoError(t, db.View(func(tx *Tx) error {
		bucket, getErr := tx.GetBucket([]byte("async"))
		if getErr != nil {
			return getErr
		}
		require.Equal(t, uint64(8), bucket.ItemCount())
		return nil
	}))
}

func TestCommitAsyncCompletesForEarlyCommitReturns(t *testing.T) {
	db, _ := CreateTestDB(t)

	readFuture := db.Begin(false).CommitAsync()
	require.NoError(t, waitCommitFuture(t, readFuture))

	closedTx := db.Begin(true)
	closedTx.Rollback()
	closedFuture := closedTx.CommitAsync()
	require.ErrorIs(t, waitCommitFuture(t, closedFuture), ErrTxClosed)
}

func waitCommitFuture(t *testing.T, future *CommitFuture) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- future.Wait() }()
	select {
	case err := <-done:
		return err
	case <-time.After(time.Second):
		t.Fatal("commit future did not complete")
		return nil
	}
}
