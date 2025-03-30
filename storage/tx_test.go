package storage

import (
	"encoding/binary"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTxRollbackCreateBucket(t *testing.T) {
	db, _ := createTestDB(t)
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
	bucket, err = tx.GetBucket([]byte("test"))
	require.Error(t, err)
}

func TestTxRollbackMultiInserts(t *testing.T) {
	db, _ := createTestDB(t)
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
	db, _ := createTestDB(t)

	// Insert initial key-value pair
	err := db.Update(func(tx *Tx) error {
		bucket, _ := tx.CreateBucket([]byte("foo"))
		return bucket.Put([]byte("key"), []byte("initial"))
	})
	require.NoError(t, err)

	startRead := make(chan struct{})
	continueWrite := make(chan struct{})
	done := make(chan struct{})

	// Start a reader that will block until signaled
	go func() {
		errReader := db.View(func(tx *Tx) error {
			bucket, _ := tx.GetBucket([]byte("foo"))
			val, _ := bucket.Get([]byte("key"))

			require.Equal(t, []byte("initial"), val, "reader should see initial value")
			startRead <- struct{}{}            // signal reader started
			<-continueWrite                    // wait for write attempt
			time.Sleep(500 * time.Millisecond) // simulate long read
			val2, _ := bucket.Get([]byte("key"))
			require.Equal(t, []byte("initial"), val2, "reader must still see consistent snapshot")

			return nil
		})
		require.NoError(t, errReader)
		done <- struct{}{}
	}()

	<-startRead // wait for reader to start

	// Start a writer while read tx is active
	writeStarted := make(chan struct{})
	go func() {
		writeStarted <- struct{}{}
		errWriter := db.Update(func(tx *Tx) error {
			bucket, _ := tx.GetBucket([]byte("foo"))
			return bucket.Put([]byte("key"), []byte("modified"))
		})
		require.NoError(t, errWriter)
		done <- struct{}{}
	}()

	<-writeStarted
	continueWrite <- struct{}{} // let the writer try to proceed

	// Wait for both goroutines to complete
	<-done
	<-done

	// Final check: reader saw original, writer modified it
	err = db.View(func(tx *Tx) error {
		bucket, _ := tx.GetBucket([]byte("foo"))
		val, _ := bucket.Get([]byte("key"))
		require.Equal(t, []byte("modified"), val, "final read should see modified value")
		return nil
	})
	require.NoError(t, err)
}

func TestTxConcurrencyIsolation(t *testing.T) {
	db, _ := createTestDB(t)

	// Set up initial data
	err := db.Update(func(tx *Tx) error {
		bucket, _ := tx.CreateBucket([]byte("foo"))
		return bucket.Put([]byte("key"), []byte("initial"))
	})
	require.NoError(t, err)

	const numReaders = 5
	var wg sync.WaitGroup

	t.Logf("Starting %d concurrent readers", numReaders)
	wg.Add(numReaders)
	started := make(chan struct{}, numReaders)

	// Launch multiple readers
	for i := 0; i < numReaders; i++ {
		go func(readerID int) {
			defer wg.Done()
			err := db.View(func(tx *Tx) error {
				started <- struct{}{}
				t.Logf("Reader %d started", readerID)
				bucket, _ := tx.GetBucket([]byte("foo"))
				val, _ := bucket.Get([]byte("key"))
				require.Equal(t, []byte("initial"), val)
				time.Sleep(500 * time.Millisecond) // simulate long read
				return nil
			})
			require.NoError(t, err)
			t.Logf("Reader %d finished", readerID)
		}(i)
	}

	// Wait until all readers have started
	for i := 0; i < numReaders; i++ {
		<-started
	}

	t.Log("Attempting writer while readers are active (should block)")
	writeDone := make(chan struct{})
	startWrite := time.Now()

	go func() {
		err := db.Update(func(tx *Tx) error {
			bucket, _ := tx.GetBucket([]byte("foo"))
			return bucket.Put([]byte("key"), []byte("updated_by_writer"))
		})
		require.NoError(t, err)
		t.Log("Writer finished")
		writeDone <- struct{}{}
	}()

	// Wait for readers to finish
	wg.Wait()

	<-writeDone
	writeDuration := time.Since(startWrite)
	t.Logf("Writer waited for %.2f seconds", writeDuration.Seconds())
	require.Greater(t, writeDuration, 400*time.Millisecond, "Writer should be blocked until readers finish")

	// Try running two writers concurrently to ensure only one proceeds at a time
	t.Log("Starting two writers simultaneously")
	writer1Started := make(chan struct{})
	writer2Done := make(chan struct{})

	go func() {
		err := db.Update(func(tx *Tx) error {
			writer1Started <- struct{}{}
			bucket, _ := tx.GetBucket([]byte("foo"))
			time.Sleep(500 * time.Millisecond) // hold the lock
			return bucket.Put([]byte("key"), []byte("writer1"))
		})
		require.NoError(t, err)
		t.Log("Writer 1 done")
	}()

	go func() {
		<-writer1Started // ensure writer1 starts first
		start := time.Now()
		err := db.Update(func(tx *Tx) error {
			bucket, _ := tx.GetBucket([]byte("foo"))
			return bucket.Put([]byte("key"), []byte("writer2"))
		})
		require.NoError(t, err)
		t.Logf("Writer 2 waited %.2f seconds", time.Since(start).Seconds())
		writer2Done <- struct{}{}
	}()

	<-writer2Done

	// Final check
	err = db.View(func(tx *Tx) error {
		bucket, _ := tx.GetBucket([]byte("foo"))
		val, _ := bucket.Get([]byte("key"))
		t.Logf("Final value in DB: %s", val)
		return nil
	})
	require.NoError(t, err)
}
