package storage

import (
	"bytes"
	"encoding/binary"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

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

func BenchmarkBaseSetInWriteTx(b *testing.B) {
	db := openBenchmarkDB(b)
	tx := db.Begin(true)
	bucket, err := tx.CreateBucket([]byte("bench"))
	if err != nil {
		b.Fatal(err)
	}
	value := bytes.Repeat([]byte("v"), 64)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err = bucket.Put(benchKey(i), value); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	tx.Rollback()
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
	b.StopTimer()
	readTx.Rollback()
}

func BenchmarkBaseDeleteReinsertInWriteTx(b *testing.B) {
	db := openBenchmarkDB(b)
	tx := db.Begin(true)
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
	b.StopTimer()
	tx.Rollback()
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
	b.StopTimer()
	readTx.Rollback()
}

func BenchmarkBlobSet64KInWriteTx(b *testing.B) {
	db := openBenchmarkDB(b)
	tx := db.Begin(true)
	bucket, err := tx.CreateBucket([]byte("blob"))
	if err != nil {
		b.Fatal(err)
	}
	payload := bytes.Repeat([]byte("x"), 64*1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err = bucket.Put(benchKey(i), payload); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	tx.Rollback()
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
	b.StopTimer()
	readTx.Rollback()
}
