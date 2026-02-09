package storage

import (
	"bytes"
	"encoding/binary"
	"os"
	"testing"
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
