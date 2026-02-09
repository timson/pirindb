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

func testIterations(base int, short int) int {
	if testing.Short() {
		return short
	}
	return base
}

func randSeqWith(rng *rand.Rand, n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rng.Intn(len(letters))]
	}
	return string(b)
}

func TestBucketInsertFindRemoveRandom(t *testing.T) {
	iterations := testIterations(50_000, 5_000)
	rng := rand.New(rand.NewSource(42))

	db, filename := CreateTestDB(t)
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
	CloseTestDB(t, db)

	// Reopen DB
	db = OpenTestDB(t, filename, nil)
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
			if rng.Float64() < 0.5 {
				keysToRemove = append(keysToRemove, []byte(k))
			}
		}
		return nil
	})
	require.NoError(t, err)

	chunkSize := 10000
	for idx := 0; idx < len(keysToRemove); idx += chunkSize {
		end := min(idx+chunkSize, len(keysToRemove))
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

	CloseTestDB(t, db)

	// Reopen once again
	db = OpenTestDB(t, filename, nil)
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
	db, _ := CreateTestDB(t)
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

	db, err := Open(path, nil)
	b.Cleanup(func() {
		_ = os.Remove(path)
		_ = os.Remove(db.dal.opts.TxLogPath)
	})

	require.NoError(b, err)
	defer func() {
		_ = db.Close()
	}()

	for i := range numEntries {
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
			for i := range numEntries {
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
			for i := range numEntries {
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
	db, filename := CreateTestDB(t)
	iterations := testIterations(50_000, 5_000)
	keys := make([][]byte, 0, iterations)
	rng := rand.New(rand.NewSource(1337))
	err := db.Update(func(tx *Tx) error {
		bucket, _ := tx.CreateBucket([]byte("foo"))
		for range iterations {
			key := randSeqWith(rng, 8)
			err := bucket.Put([]byte(key), []byte(strings.Repeat("a", 32)))
			require.NoError(t, err)
			keys = append(keys, []byte(key))
		}
		return nil
	})
	require.NoError(t, err)
	CloseTestDB(t, db)

	db = OpenTestDB(t, filename, nil)
	require.NoError(t, err)

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
	db, _ := CreateTestDB(t)
	testValue := make([]byte, 15012)
	for idx := range testValue {
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
	db, _ := CreateTestDB(t)
	nBuckets := 1000
	originalBuckets := make([][]byte, nBuckets)
	err := db.Update(func(tx *Tx) error {
		for i := range nBuckets {
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
	db, _ := CreateTestDB(t)
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
	db, _ := CreateTestDB(t)
	iterations := 100
	err := db.Update(func(tx *Tx) error {
		bucket, _ := tx.CreateBucket([]byte("foo"))
		for range iterations {
			pk, err := bucket.NextSequence()
			if err != nil {
				return err
			}
			err = bucket.Put(itob(pk), fmt.Appendf(nil, "value_%d", pk))
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

func TestBucketPutOverwriteReleasesOldBlobPages(t *testing.T) {
	db, _ := CreateTestDB(t)

	big1 := bytes.Repeat([]byte("a"), MaxValueSize+500)
	big2 := bytes.Repeat([]byte("b"), MaxValueSize+700)

	err := db.Update(func(tx *Tx) error {
		bucket, _ := tx.CreateBucket([]byte("foo"))
		return bucket.Put([]byte("k"), big1)
	})
	require.NoError(t, err)

	before := len(db.dal.freelist.releasedPages)
	err = db.Update(func(tx *Tx) error {
		bucket, _ := tx.GetBucket([]byte("foo"))
		return bucket.Put([]byte("k"), big2)
	})
	require.NoError(t, err)
	after := len(db.dal.freelist.releasedPages)
	require.Greater(t, after, before)

	err = db.View(func(tx *Tx) error {
		bucket, _ := tx.GetBucket([]byte("foo"))
		v, found := bucket.Get([]byte("k"))
		require.True(t, found)
		require.Equal(t, big2, v)
		return nil
	})
	require.NoError(t, err)
}

func TestBucketPutOverwriteStats(t *testing.T) {
	db, _ := CreateTestDB(t)

	err := db.Update(func(tx *Tx) error {
		bucket, _ := tx.CreateBucket([]byte("foo"))
		require.NoError(t, bucket.Put([]byte("k"), []byte("1111")))
		require.NoError(t, bucket.Put([]byte("k"), []byte("22")))
		require.Equal(t, uint64(1), bucket.itemsN)
		require.Equal(t, uint64(len("k")+len("22")), bucket.bytesInUse)
		return nil
	})
	require.NoError(t, err)
}

func TestBucketPutOverwriteBlobStats(t *testing.T) {
	db, _ := CreateTestDB(t)

	big := bytes.Repeat([]byte("x"), MaxValueSize+333)
	err := db.Update(func(tx *Tx) error {
		bucket, _ := tx.CreateBucket([]byte("foo"))
		require.NoError(t, bucket.Put([]byte("k"), big))
		require.Equal(t, uint64(1), bucket.blobsN)
		require.NoError(t, bucket.Put([]byte("k"), []byte("small")))
		require.Equal(t, uint64(0), bucket.blobsN)
		return nil
	})
	require.NoError(t, err)
}

func TestBucketRemoveRootChangePersists(t *testing.T) {
	db, filename := CreateTestDB(t)
	const total = 2000

	err := db.Update(func(tx *Tx) error {
		bucket, _ := tx.CreateBucket([]byte("foo"))
		for i := 0; i < total; i++ {
			k := fmt.Sprintf("%05d", i)
			if err := bucket.Put([]byte(k), []byte(k)); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	var rootAfter uint64
	err = db.Update(func(tx *Tx) error {
		bucket, err := tx.GetBucket([]byte("foo"))
		if err != nil {
			return err
		}
		for i := 0; i < total-1; i++ {
			k := fmt.Sprintf("%05d", i)
			if err := bucket.Remove([]byte(k)); err != nil {
				return err
			}
		}
		rootAfter = bucket.root
		return nil
	})
	require.NoError(t, err)
	require.NotZero(t, rootAfter)

	CloseTestDB(t, db)
	db = OpenTestDB(t, filename, nil)

	err = db.View(func(tx *Tx) error {
		bucket, err := tx.GetBucket([]byte("foo"))
		if err != nil {
			return err
		}
		require.Equal(t, rootAfter, bucket.root)
		v, found := bucket.Get([]byte(fmt.Sprintf("%05d", total-1)))
		require.True(t, found)
		require.Equal(t, []byte(fmt.Sprintf("%05d", total-1)), v)
		_, found = bucket.Get([]byte("00000"))
		require.False(t, found)
		return nil
	})
	require.NoError(t, err)
}

func TestBucketRemoveBlobReclaimsPages(t *testing.T) {
	db, _ := CreateTestDB(t)
	big := bytes.Repeat([]byte("z"), MaxValueSize+700)

	err := db.Update(func(tx *Tx) error {
		bucket, _ := tx.CreateBucket([]byte("foo"))
		return bucket.Put([]byte("blob"), big)
	})
	require.NoError(t, err)

	before := len(db.dal.freelist.releasedPages)
	err = db.Update(func(tx *Tx) error {
		bucket, err := tx.GetBucket([]byte("foo"))
		require.NoError(t, err)
		return bucket.Remove([]byte("blob"))
	})
	require.NoError(t, err)

	after := len(db.dal.freelist.releasedPages)
	require.Greater(t, after, before)

	err = db.View(func(tx *Tx) error {
		bucket, err := tx.GetBucket([]byte("foo"))
		if err != nil {
			return err
		}
		_, found := bucket.Get([]byte("blob"))
		require.False(t, found)
		return nil
	})
	require.NoError(t, err)
}
