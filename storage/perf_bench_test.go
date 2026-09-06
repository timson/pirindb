package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

var benchmarkFreelistSnapshot *Freelist

func benchKey(i int) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, uint64(i))
	return key
}

func openBenchmarkDB(b *testing.B) *DB {
	path := TempFileName(".db")
	db, err := Open(path, nil)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		_ = db.Close()
		_ = os.Remove(path)
		_ = os.Remove(db.dal.opts.TxLogPath)
	})
	return db
}

func createBatchBucketIfNeeded(tx *Tx, name []byte) (*Bucket, error) {
	bucket, err := tx.CreateBucketIfNotExists(name)
	if err != nil {
		return nil, err
	}
	return bucket, nil
}

func writeBatch(tx *Tx, bucketName []byte, start, count int, value []byte) error {
	bucket, err := createBatchBucketIfNeeded(tx, bucketName)
	if err != nil {
		return err
	}
	for i := 0; i < count; i++ {
		if err = bucket.Put(benchKey(start+i), value); err != nil {
			return err
		}
	}
	return nil
}

func TestBatchWrite1000InSingleTx(t *testing.T) {
	const batchSize = 1000

	db, _ := CreateTestDB(t)
	value := bytes.Repeat([]byte("v"), 64)
	start := time.Now()

	err := db.Update(func(tx *Tx) error {
		return writeBatch(tx, []byte("bench"), 0, batchSize, value)
	})
	if err != nil {
		t.Fatal(err)
	}

	elapsed := time.Since(start)
	writesPerSecond := float64(batchSize) / elapsed.Seconds()
	t.Logf("batch write: %d records in one tx, value_size=%dB, elapsed=%s, writes/sec=%.0f",
		batchSize, len(value), elapsed, writesPerSecond)

	err = db.View(func(tx *Tx) error {
		bucket, err := tx.GetBucket([]byte("bench"))
		if err != nil {
			return err
		}
		for i := 0; i < batchSize; i++ {
			got, found := bucket.Get(benchKey(i))
			if !found {
				t.Fatalf("missing key %d", i)
			}
			if !bytes.Equal(got, value) {
				t.Fatalf("unexpected value for key %d", i)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func BenchmarkFreelistAllocateRelease(b *testing.B) {
	f := NewFreelist(BTreePageSize, uint64(rootPageNumber+b.N+8))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pageNum, err := f.GetNextPageNumber()
		if err != nil {
			b.Fatal(err)
		}
		f.ReleasePage(pageNum)
	}
}

func BenchmarkFreelistSnapshotFragmented(b *testing.B) {
	f := newFreelist(BTreePageSize, 1_000_000, true)
	f.releasedExtents = make([]pageExtent, 100_000)
	for idx := range f.releasedExtents {
		pageNum := uint64(rootPageNumber + idx*2)
		f.releasedExtents[idx] = pageExtent{start: pageNum, end: pageNum}
	}
	f.releasedCount = uint64(len(f.releasedExtents))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchmarkFreelistSnapshot = cloneFreelist(f)
	}
}

func BenchmarkFreelistFragmentedAllocateRelease(b *testing.B) {
	f := newFreelist(BTreePageSize, 1_000_000, true)
	f.currentPage = 900_000
	f.releasedExtents = make([]pageExtent, 100_000)
	for idx := range f.releasedExtents {
		pageNum := uint64(rootPageNumber + idx*2)
		f.releasedExtents[idx] = pageExtent{start: pageNum, end: pageNum}
	}
	f.releasedCount = uint64(len(f.releasedExtents))
	f.dirty = true
	markFreelistPersisted(f)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pageNum, err := f.GetNextPageNumber()
		if err != nil {
			b.Fatal(err)
		}
		f.ReleasePage(pageNum)
		markFreelistPersisted(f)
	}
}

func BenchmarkBaseSetInWriteTx(b *testing.B) {
	db := openBenchmarkDB(b)
	const keyspace = 4096
	value := bytes.Repeat([]byte("v"), 64)
	err := db.Update(func(tx *Tx) error {
		bucket, createErr := tx.CreateBucket([]byte("bench"))
		if createErr != nil {
			return createErr
		}
		for i := 0; i < keyspace; i++ {
			if putErr := bucket.Put(benchKey(i), value); putErr != nil {
				return putErr
			}
		}
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}
	tx := db.Begin(true)
	defer tx.Rollback()
	bucket, err := tx.GetBucket([]byte("bench"))
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err = bucket.Put(benchKey(i%keyspace), value); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBaseInsertInWriteTx(b *testing.B) {
	db := openBenchmarkDB(b)
	value := bytes.Repeat([]byte("v"), 64)
	const batchSize = 100_000

	b.ResetTimer()
	b.StopTimer()
	completed := 0
	for completed < b.N {
		tx := db.Begin(true)
		bucket, err := tx.CreateBucket([]byte("insert"))
		if err != nil {
			tx.Rollback()
			b.Fatal(err)
		}
		count := min(batchSize, b.N-completed)
		b.StartTimer()
		for idx := 0; idx < count; idx++ {
			if err = bucket.Put(benchKey(idx), value); err != nil {
				b.StopTimer()
				tx.Rollback()
				b.Fatal(err)
			}
		}
		b.StopTimer()
		tx.Rollback()
		completed += count
	}
}

func BenchmarkBatchWrite1000InSingleTx(b *testing.B) {
	const batchSize = 1000

	db := openBenchmarkDB(b)
	value := bytes.Repeat([]byte("v"), 64)
	b.SetBytes(int64(batchSize * len(value)))
	b.ReportMetric(float64(batchSize), "writes/tx")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := i * batchSize
		err := db.Update(func(tx *Tx) error {
			return writeBatch(tx, []byte("bench"), start, batchSize, value)
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSingleWritePerTx(b *testing.B) {
	db := openBenchmarkDB(b)
	value := bytes.Repeat([]byte("v"), 64)
	b.SetBytes(int64(len(value)))
	b.ReportMetric(1, "writes/tx")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := db.Update(func(tx *Tx) error {
			bucket, bucketErr := tx.CreateBucketIfNotExists([]byte("bench"))
			if bucketErr != nil {
				return bucketErr
			}
			return bucket.Put(benchKey(i), value)
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSingleWritePerTxJournal(b *testing.B) {
	path := TempFileName(".db")
	opts := DefaultOptions().
		WithSyncPolicy(SyncPolicyJournal).
		WithCheckpointTxThreshold(64)
	db, err := Open(path, opts)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		_ = db.Close()
		_ = os.Remove(path)
		_ = os.Remove(db.dal.opts.TxLogPath)
	})

	value := bytes.Repeat([]byte("v"), 64)
	b.SetBytes(int64(len(value)))
	b.ReportMetric(1, "writes/tx")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err = db.Update(func(tx *Tx) error {
			bucket, bucketErr := tx.CreateBucketIfNotExists([]byte("bench"))
			if bucketErr != nil {
				return bucketErr
			}
			return bucket.Put(benchKey(i), value)
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkParallelSingleWritePerTxGroup(b *testing.B) {
	path := TempFileName(".db")
	opts := DefaultOptions().
		WithSyncPolicy(SyncPolicyGroup).
		WithGroupCommitTxThreshold(16).
		WithGroupCommitWindow(time.Millisecond)
	db, err := Open(path, opts)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		_ = db.Close()
		_ = os.Remove(path)
		_ = os.Remove(db.dal.opts.TxLogPath)
	})

	value := bytes.Repeat([]byte("v"), 64)
	b.SetBytes(int64(len(value)))
	b.ReportMetric(1, "writes/tx")

	var keyCounter atomic.Uint64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			keyID := int(keyCounter.Add(1))
			updateErr := db.Update(func(tx *Tx) error {
				bucket, bucketErr := tx.CreateBucketIfNotExists([]byte("bench"))
				if bucketErr != nil {
					return bucketErr
				}
				return bucket.Put(benchKey(keyID), value)
			})
			if updateErr != nil {
				b.Fatal(updateErr)
			}
		}
	})
}

func BenchmarkBaseGetInReadTx(b *testing.B) {
	db := openBenchmarkDB(b)
	const preload = 4096

	err := db.Update(func(tx *Tx) error {
		bucket, bucketErr := tx.CreateBucket([]byte("bench"))
		if bucketErr != nil {
			return bucketErr
		}
		for i := 0; i < preload; i++ {
			if putErr := bucket.Put(benchKey(i), []byte("value")); putErr != nil {
				return putErr
			}
		}
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}

	readTx := db.Begin(false)
	defer readTx.Rollback()
	bucket, err := readTx.GetBucket([]byte("bench"))
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := benchKey(i % preload)
		value, found := bucket.Get(key)
		if !found || len(value) == 0 {
			b.Fatalf("expected key %d", i%preload)
		}
	}
}

func BenchmarkSingleGetPerReadTx(b *testing.B) {
	db := openBenchmarkDB(b)
	const preload = 4096
	value := []byte("value")

	err := db.Update(func(tx *Tx) error {
		bucket, bucketErr := tx.CreateBucket([]byte("bench"))
		if bucketErr != nil {
			return bucketErr
		}
		for i := 0; i < preload; i++ {
			if putErr := bucket.Put(benchKey(i), value); putErr != nil {
				return putErr
			}
		}
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := benchKey(i % preload)
		err = db.View(func(tx *Tx) error {
			bucket, getErr := tx.GetBucket([]byte("bench"))
			if getErr != nil {
				return getErr
			}
			got, found := bucket.Get(key)
			if !found || len(got) == 0 {
				return fmt.Errorf("expected key")
			}
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkParallelGetPerReadTx(b *testing.B) {
	db := openBenchmarkDB(b)
	const preload = 4096
	requireValue := []byte("value")
	err := db.Update(func(tx *Tx) error {
		bucket, bucketErr := tx.CreateBucket([]byte("bench"))
		if bucketErr != nil {
			return bucketErr
		}
		for i := 0; i < preload; i++ {
			if putErr := bucket.Put(benchKey(i), requireValue); putErr != nil {
				return putErr
			}
		}
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}

	var keyCounter atomic.Uint64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			key := benchKey(int(keyCounter.Add(1)) % preload)
			viewErr := db.View(func(tx *Tx) error {
				bucket, getErr := tx.GetBucket([]byte("bench"))
				if getErr != nil {
					return getErr
				}
				value, found := bucket.Get(key)
				if !found || len(value) == 0 {
					return fmt.Errorf("expected key")
				}
				return nil
			})
			if viewErr != nil {
				b.Fatal(viewErr)
			}
		}
	})
}

func BenchmarkBaseDeleteReinsertInWriteTx(b *testing.B) {
	db := openBenchmarkDB(b)
	tx := db.Begin(true)
	defer tx.Rollback()
	bucket, err := tx.CreateBucket([]byte("bench"))
	if err != nil {
		b.Fatal(err)
	}
	const preload = 4096
	value := []byte("value")
	for i := 0; i < preload; i++ {
		if err = bucket.Put(benchKey(i), value); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := benchKey(i % preload)
		if err = bucket.Remove(key); err != nil {
			b.Fatal(err)
		}
		if err = bucket.Put(key, value); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBaseSeekInReadTx(b *testing.B) {
	db := openBenchmarkDB(b)
	const preload = 4096

	err := db.Update(func(tx *Tx) error {
		bucket, bucketErr := tx.CreateBucket([]byte("bench"))
		if bucketErr != nil {
			return bucketErr
		}
		for i := 0; i < preload; i++ {
			if putErr := bucket.Put(benchKey(i), []byte("value")); putErr != nil {
				return putErr
			}
		}
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}

	readTx := db.Begin(false)
	defer readTx.Rollback()
	bucket, err := readTx.GetBucket([]byte("bench"))
	if err != nil {
		b.Fatal(err)
	}
	cursor := bucket.Cursor()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key, value := cursor.Seek(benchKey(i % preload))
		if key == nil || len(value) == 0 {
			b.Fatalf("expected seek key %d", i%preload)
		}
	}
}

func BenchmarkCursorScanInReadTx(b *testing.B) {
	db := openBenchmarkDB(b)
	const preload = 16_384

	err := db.Update(func(tx *Tx) error {
		bucket, bucketErr := tx.CreateBucket([]byte("bench"))
		if bucketErr != nil {
			return bucketErr
		}
		for i := 0; i < preload; i++ {
			if putErr := bucket.Put(benchKey(i), []byte("value")); putErr != nil {
				return putErr
			}
		}
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}

	readTx := db.Begin(false)
	defer readTx.Rollback()
	bucket, err := readTx.GetBucket([]byte("bench"))
	if err != nil {
		b.Fatal(err)
	}
	cursor := bucket.Cursor()
	key, value := cursor.First()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if key == nil {
			key, value = cursor.First()
		}
		if key == nil || len(value) == 0 {
			b.Fatal("unexpected end of cursor")
		}
		key, value = cursor.Next()
	}
}

func BenchmarkBlobSet64KInWriteTx(b *testing.B) {
	db := openBenchmarkDB(b)
	err := db.Update(func(tx *Tx) error {
		_, createErr := tx.CreateBucket([]byte("blob"))
		return createErr
	})
	if err != nil {
		b.Fatal(err)
	}
	payload := bytes.Repeat([]byte("x"), 64*1024)
	const batchSize = 256
	var tx *Tx
	var bucket *Bucket
	startBatch := func() {
		tx = db.Begin(true)
		bucket, err = tx.GetBucket([]byte("blob"))
		if err != nil {
			tx.Rollback()
			tx = nil
			b.Fatal(err)
		}
	}
	startBatch()
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i > 0 && i%batchSize == 0 {
			b.StopTimer()
			tx.Rollback()
			startBatch()
			b.StartTimer()
		}
		if err = bucket.Put(benchKey(i%batchSize), payload); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBlobGet64KInReadTx(b *testing.B) {
	db := openBenchmarkDB(b)
	payload := bytes.Repeat([]byte("x"), 64*1024)
	key := []byte("blob")

	err := db.Update(func(tx *Tx) error {
		bucket, bucketErr := tx.CreateBucket([]byte("blob"))
		if bucketErr != nil {
			return bucketErr
		}
		return bucket.Put(key, payload)
	})
	if err != nil {
		b.Fatal(err)
	}

	readTx := db.Begin(false)
	defer readTx.Rollback()
	bucket, err := readTx.GetBucket([]byte("blob"))
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		value, found := bucket.Get(key)
		if !found || len(value) != len(payload) {
			b.Fatalf("unexpected blob value length: %d", len(value))
		}
	}
}
