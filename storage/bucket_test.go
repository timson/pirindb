package storage

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/require"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"
)

func TestBucketInsertFindRemoveRandom(t *testing.T) {
	iterations := 1_000_000

	db, filename := createTestDB(t)
	t.Logf("opened db at %s", filename)
	t.Logf("going to insert %d items", iterations)

	err := db.Update(func(tx *Tx) error {
		bucket, _ := tx.CreateBucket([]byte("foo"))
		for idx := range iterations {
			k := fmt.Sprintf("test_%d", idx)
			v := fmt.Sprintf("value_%d", idx)
			err := bucket.Put([]byte(k), []byte(v))
			if err != nil {
				return err
			}
		}
		return nil
	})
	closeTestDB(t, db)

	// Reopen DB
	db = openTestDB(t, filename)
	var keysToRemove [][]byte

	t.Log("test of all inserted items, and randomly choose items to remove")
	err = db.View(func(tx *Tx) error {
		bucket, _ := tx.GetBucket([]byte("foo"))
		for idx := range iterations {
			k := fmt.Sprintf("test_%d", idx)
			v := fmt.Sprintf("value_%d", idx)
			val, found := bucket.Get([]byte(k))
			if !found {
				return fmt.Errorf("key not found: %s", k)
			}
			if !bytes.Equal(val, []byte(v)) {
				return fmt.Errorf("value mismatch: %s", val)
			}
			if rand.Float64() < 0.5 {
				keysToRemove = append(keysToRemove, []byte(k))
			}
		}
		return nil
	})
	require.NoError(t, err)

	chunkSize := 10000
	for idx := 0; idx < len(keysToRemove); idx += chunkSize {
		end := idx + chunkSize
		if end > len(keysToRemove) {
			end = len(keysToRemove)
		}
		chunk := keysToRemove[idx:end]
		err = db.Update(func(tx *Tx) error {
			bucket, _ := tx.GetBucket([]byte("foo"))
			for _, key := range chunk {
				err = bucket.Remove(key)
				if err != nil {
					return err
				}
			}
			return nil
		})
		require.NoError(t, err)
	}

	closeTestDB(t, db)

	// Reopen once again
	db = openTestDB(t, filename)
	err = db.View(func(tx *Tx) error {
		bucket, _ := tx.GetBucket([]byte("foo"))
		for _, k := range keysToRemove {
			_, found := bucket.Get(k)
			if found {
				return fmt.Errorf("key found: %s", k)
			}
		}
		return nil
	})
	require.NoError(t, err)
}

func TestBucketInsertRemove(t *testing.T) {
	db, _ := createTestDB(t)
	err := db.Update(func(tx *Tx) error {
		bucket, _ := tx.CreateBucket([]byte("foo"))
		err := bucket.Put([]byte("foo"), []byte("bar"))
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	err = db.Update(func(tx *Tx) error {
		bucket, _ := tx.GetBucket([]byte("foo"))
		err = bucket.Remove([]byte("foo"))
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	err = db.View(func(tx *Tx) error {
		bucket, _ := tx.GetBucket([]byte("foo"))
		_, found := bucket.Get([]byte("foo"))
		require.False(t, found)
		return nil
	})
	require.NoError(t, err)
}

func BenchmarkBucketOperations(b *testing.B) {
	path := "bench.db"
	// Prepare test data - 16 byte keys and values
	const numEntries = 100_000
	keys := make([][]byte, numEntries)
	values := make([][]byte, numEntries)
	b.Cleanup(func() {
		_ = os.Remove(path)
	})

	db, err := Open(path, 0644)
	require.NoError(b, err)
	defer func() {
		_ = db.Close()
	}()

	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("%016d", i)   // 16 byte string
		value := fmt.Sprintf("%016d", i) // 16 byte string
		keys[i] = []byte(key)
		values[i] = []byte(value)
	}

	// Measure Put Speed
	b.Run("Put", func(b *testing.B) {
		err = db.Update(func(tx *Tx) error {
			bucket, _ := tx.CreateBucket([]byte("foo"))
			start := time.Now()
			for i := 0; i < numEntries; i++ {
				err = bucket.Put(keys[i], values[i])
				if err != nil {
					b.Fatal(err)
				}
			}
			duration := time.Since(start)
			opsPerSecond := float64(numEntries) / duration.Seconds()
			b.ReportMetric(opsPerSecond, "inserts/sec")
			fmt.Printf("Put Performance: %.2f ops/sec (total: %d, time: %v)\n",
				opsPerSecond, numEntries, duration)
			return nil
		})
		if err != nil {
			return
		}
	})

	// Measure Get Speed
	b.Run("Get", func(b *testing.B) {
		err = db.View(func(tx *Tx) error {
			bucket, _ := tx.GetBucket([]byte("foo"))
			start := time.Now()
			for i := 0; i < numEntries; i++ {
				val, found := bucket.Get(keys[i])
				if !found {
					b.Fatalf("Key not found: %s", keys[i])
				}
				if string(val) != string(values[i]) {
					b.Fatalf("Incorrect value for Key %s: expected %s, got %s",
						keys[i], values[i], val)
				}
			}
			duration := time.Since(start)
			opsPerSecond := float64(numEntries) / duration.Seconds()
			b.ReportMetric(opsPerSecond, "finds/sec")
			fmt.Printf("Get Performance: %.2f ops/sec (total: %d, time: %v)\n",
				opsPerSecond, numEntries, duration)
			return nil
		})
	})
}

func TestBucketInsertRandom(t *testing.T) {
	db, filename := createTestDB(t)
	iterations := 1_000_000
	keys := make([][]byte, 0)
	err := db.Update(func(tx *Tx) error {
		bucket, _ := tx.CreateBucket([]byte("foo"))
		for _ = range iterations {
			key := randSeq(8)
			err := bucket.Put([]byte(key), []byte(strings.Repeat("a", 32)))
			require.NoError(t, err)
			keys = append(keys, []byte(key))
		}
		return nil
	})
	require.NoError(t, err)
	closeTestDB(t, db)

	db = openTestDB(t, filename)

	err = db.View(func(tx *Tx) error {
		bucket, _ := tx.GetBucket([]byte("foo"))
		for _, key := range keys {
			_, found := bucket.Get(key)
			if !found {
				return fmt.Errorf("key not found: %s", key)
			}
		}
		return nil
	})
	require.NoError(t, err)
}

func TestBucketInsertBlob(t *testing.T) {
	db, _ := createTestDB(t)
	testValue := make([]byte, 15012)
	for idx := 0; idx < len(testValue); idx++ {
		testValue[idx] = byte(rand.Intn(255))
	}

	err := db.Update(func(tx *Tx) error {
		bucket, _ := tx.CreateBucket([]byte("foo"))
		err := bucket.Put([]byte("foo"), testValue)
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	err = db.View(func(tx *Tx) error {
		bucket, _ := tx.GetBucket([]byte("foo"))
		v, found := bucket.Get([]byte("foo"))
		require.True(t, found)
		require.Equal(t, v, testValue)
		return nil
	})
	require.NoError(t, err)
}

func TestCreateBuckets(t *testing.T) {
	db, _ := createTestDB(t)
	nBuckets := 1000
	originalBuckets := make([][]byte, nBuckets)
	err := db.Update(func(tx *Tx) error {
		for i := 0; i < nBuckets; i++ {
			bucketName := fmt.Sprintf("bucket_%03d", i)
			originalBuckets[i] = []byte(bucketName)
			_, err := tx.CreateBucket([]byte(bucketName))
			require.NoError(t, err)
		}
		return nil
	})
	require.NoError(t, err)

	err = db.View(func(tx *Tx) error {
		buckets := tx.Buckets()
		require.Equal(t, buckets, originalBuckets)
		return nil

	})
	require.NoError(t, err)
}

func TestBucketsForEach(t *testing.T) {
	db, _ := createTestDB(t)
	iterations := 10_000
	originalKeys := make([][]byte, iterations)

	err := db.Update(func(tx *Tx) error {
		bucket, _ := tx.CreateBucket([]byte("foo"))
		for idx := range iterations {
			k := fmt.Sprintf("test_%04d", idx)
			originalKeys[idx] = []byte(k)
			v := fmt.Sprintf("value_%d", idx)
			err := bucket.Put([]byte(k), []byte(v))
			if err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	err = db.View(func(tx *Tx) error {
		bucket, _ := tx.GetBucket([]byte("foo"))
		keys := make([][]byte, 0)
		err = bucket.ForEach(func(k, v []byte) error {
			keys = append(keys, k)
			return nil
		})
		require.Equal(t, originalKeys, keys)
		if err != nil {
			return err
		}
		return nil
	})
	require.NoError(t, err)
}

func TestBucketNextSequence(t *testing.T) {
	db, _ := createTestDB(t)
	iterations := 100
	err := db.Update(func(tx *Tx) error {
		bucket, _ := tx.CreateBucket([]byte("foo"))
		for i := 0; i < iterations; i++ {
			pk, err := bucket.NextSequence()
			if err != nil {
				return err
			}
			err = bucket.Put(itob(pk), []byte(fmt.Sprintf("value_%d", pk)))
			if err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)
	err = db.View(func(tx *Tx) error {
		bucket, _ := tx.GetBucket([]byte("foo"))
		require.Equal(t, int(bucket.Sequence()), iterations)
		return nil
	})
	require.NoError(t, err)
}
