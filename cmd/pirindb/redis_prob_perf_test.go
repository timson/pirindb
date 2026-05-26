package main

import (
	"bufio"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/timson/pirindb/storage"
)

const (
	redisProbPerfUniqueCount      = 1_000_000
	redisProbPerfBatchSize        = 10_000
	redisProbPerfBloomErrorRate   = 0.01
	redisProbPerfTopKK            = uint64(10)
	redisProbPerfTopKWidth        = uint64(10_000)
	redisProbPerfTopKHotWidth     = uint64(50_000)
	redisProbPerfTopKDepth        = uint64(7)
	redisProbPerfTopKDecay        = 0.9
	redisProbPerfBenchmarkBatch   = 1_000
	redisProbPerfRunEnv           = "RUN_REDIS_PROB_PERF"
	redisProbPerfCheckpointWindow = 256
)

func openRedisProbPerfDB(tb testing.TB) (*storage.DB, string) {
	tb.Helper()

	path := storage.TempFileName(".db")
	opts := storage.DefaultOptions().
		WithSyncPolicy(storage.SyncPolicyJournal).
		WithCheckpointTxThreshold(redisProbPerfCheckpointWindow)
	db, err := storage.Open(path, opts)
	require.NoError(tb, err)
	tb.Cleanup(func() {
		_ = db.Close()
		_ = os.Remove(path)
		_ = os.Remove(db.GetOptions().TxLogPath)
	})
	return db, path
}

func requireRedisProbPerfEnabled(t *testing.T) {
	t.Helper()
	if os.Getenv(redisProbPerfRunEnv) == "" {
		t.Skip("set RUN_REDIS_PROB_PERF=1 to run 1M probabilistic structure perf tests")
	}
}

func redisProbPerfBatchItems(prefix string, start, count int) [][]byte {
	items := make([][]byte, count)
	for i := 0; i < count; i++ {
		n := start + i
		item := make([]byte, 0, len(prefix)+1+20)
		item = append(item, prefix...)
		item = append(item, ':')
		item = strconv.AppendInt(item, int64(n), 10)
		items[i] = item
	}
	return items
}

func redisProbPerfIncrements(count int) []uint64 {
	increments := make([]uint64, count)
	for i := range increments {
		increments[i] = 1
	}
	return increments
}

func redisProbPerfUsage(db *storage.DB, dbIndex int) (redisDBUsage, uint64) {
	stat := db.Stat()
	return summarizeRedisDBUsage(stat.Buckets, dbIndex), stat.UsedDBSize
}

func redisProbPerfRandFn(seed uint64) func() uint64 {
	rng := seed
	return func() uint64 {
		rng ^= rng << 7
		rng ^= rng >> 9
		rng ^= rng << 8
		return rng
	}
}

func redisProbPerfTopKHotItems() ([][]byte, []uint64) {
	items := make([][]byte, 0, redisProbPerfTopKK)
	increments := make([]uint64, 0, redisProbPerfTopKK)
	for i := 0; i < int(redisProbPerfTopKK); i++ {
		item := []byte("hot:" + strconv.Itoa(i))
		items = append(items, item)
		increments = append(increments, uint64(10_000-(i*500)))
	}
	return items, increments
}

func TestRedisBloomPerfFill1MUniqueBatched(t *testing.T) {
	requireRedisProbPerfEnabled(t)

	db, path := openRedisProbPerfDB(t)
	ns := redisNamespaceForDB(0)
	key := []byte("perf:bloom")

	err := db.Update(func(tx *storage.Tx) error {
		return redisBloomReserveTx(tx, ns, key, redisBloomReserveOptions{
			errorRate: redisProbPerfBloomErrorRate,
			capacity:  redisProbPerfUniqueCount,
			expansion: redisBloomAutoCreateExpansion,
		}, 0)
	})
	require.NoError(t, err)

	start := time.Now()
	for batchStart := 0; batchStart < redisProbPerfUniqueCount; batchStart += redisProbPerfBatchSize {
		batchCount := redisProbPerfBatchSize
		if remaining := redisProbPerfUniqueCount - batchStart; remaining < batchCount {
			batchCount = remaining
		}
		items := redisProbPerfBatchItems("bf", batchStart, batchCount)
		err = db.Update(func(tx *storage.Tx) error {
			_, innerErr := redisBloomMAddTx(tx, ns, key, items, 0)
			return innerErr
		})
		require.NoError(t, err)
	}
	elapsed := time.Since(start)

	var (
		meta    *redisBloomMeta
		subMeta *redisBloomSubFilterMeta
	)
	err = db.View(func(tx *storage.Tx) error {
		var found bool
		meta, found, err = loadRedisBloomMetaTx(tx, ns, key)
		if err != nil {
			return err
		}
		require.True(t, found)
		bucket, bucketErr := getRedisBloomDataBucketTx(tx, ns, meta)
		if bucketErr != nil {
			return bucketErr
		}
		subMeta, found, err = loadRedisBloomSubFilterMeta(bucket, 0)
		if err != nil {
			return err
		}
		require.True(t, found)
		return nil
	})
	require.NoError(t, err)

	fileInfo, statErr := os.Stat(path)
	require.NoError(t, statErr)
	usage, usedDBSize := redisProbPerfUsage(db, 0)
	itemsPerSecond := float64(redisProbPerfUniqueCount) / elapsed.Seconds()

	existsStart := time.Now()
	for batchStart := 0; batchStart < redisProbPerfUniqueCount; batchStart += redisProbPerfBatchSize {
		batchCount := redisProbPerfBatchSize
		if remaining := redisProbPerfUniqueCount - batchStart; remaining < batchCount {
			batchCount = remaining
		}
		items := redisProbPerfBatchItems("bf", batchStart, batchCount)
		err = db.View(func(tx *storage.Tx) error {
			results, innerErr := redisBloomMExistsTx(tx, ns, key, items, 0)
			if innerErr != nil {
				return innerErr
			}
			for _, result := range results {
				require.Equal(t, int64(1), result)
			}
			return nil
		})
		require.NoError(t, err)
	}
	existsElapsed := time.Since(existsStart)
	existsPerSecond := float64(redisProbPerfUniqueCount) / existsElapsed.Seconds()

	t.Logf("redis bloom perf: items=%d batch=%d sync_policy=journal fill_elapsed=%s fill_items/sec=%.0f exists_elapsed=%s exists_items/sec=%.0f logical_bytes=%d used_db_size=%d file_bytes=%d db_keys=%d subfilters=%d bit_count=%d hash_count=%d",
		redisProbPerfUniqueCount, redisProbPerfBatchSize, elapsed, itemsPerSecond, existsElapsed, existsPerSecond, usage.bytesInUse, usedDBSize, fileInfo.Size(), usage.keys, meta.SubFilterCount, subMeta.BitCount, subMeta.HashCount)
}

func TestRedisTopKPerfFill1MUniqueBatched(t *testing.T) {
	requireRedisProbPerfEnabled(t)

	db, path := openRedisProbPerfDB(t)
	ns := redisNamespaceForDB(0)
	key := []byte("perf:topk")
	increments := redisProbPerfIncrements(redisProbPerfBatchSize)

	err := db.Update(func(tx *storage.Tx) error {
		return redisTopKReserveTx(tx, ns, key, redisProbPerfTopKK, redisProbPerfTopKWidth, redisProbPerfTopKDepth, redisProbPerfTopKDecay, 0)
	})
	require.NoError(t, err)

	randFn := redisProbPerfRandFn(0x9e3779b97f4a7c15)

	start := time.Now()
	for batchStart := 0; batchStart < redisProbPerfUniqueCount; batchStart += redisProbPerfBatchSize {
		batchCount := redisProbPerfBatchSize
		if remaining := redisProbPerfUniqueCount - batchStart; remaining < batchCount {
			batchCount = remaining
		}
		items := redisProbPerfBatchItems("tk", batchStart, batchCount)
		err = db.Update(func(tx *storage.Tx) error {
			var batchIncrements []uint64
			if batchCount == redisProbPerfBatchSize {
				batchIncrements = increments
			} else {
				batchIncrements = redisProbPerfIncrements(batchCount)
			}
			_, innerErr := redisTopKIncrByTx(tx, ns, key, items, batchIncrements, 0, randFn)
			return innerErr
		})
		require.NoError(t, err)
	}
	elapsed := time.Since(start)

	var (
		meta    *redisTopKMeta
		entries []redisTopKHeapEntry
	)
	err = db.View(func(tx *storage.Tx) error {
		var innerErr error
		meta, innerErr = redisTopKInfoTx(tx, ns, key, 0)
		if innerErr != nil {
			return innerErr
		}
		entries, innerErr = redisTopKListTx(tx, ns, key, 0)
		return innerErr
	})
	require.NoError(t, err)
	require.Len(t, entries, int(redisProbPerfTopKK))

	fileInfo, statErr := os.Stat(path)
	require.NoError(t, statErr)
	usage, usedDBSize := redisProbPerfUsage(db, 0)
	itemsPerSecond := float64(redisProbPerfUniqueCount) / elapsed.Seconds()

	t.Logf("redis topk perf: items=%d batch=%d sync_policy=journal elapsed=%s items/sec=%.0f logical_bytes=%d used_db_size=%d file_bytes=%d db_keys=%d k=%d width=%d depth=%d decay=%.3f heap_size=%d",
		redisProbPerfUniqueCount, redisProbPerfBatchSize, elapsed, itemsPerSecond, usage.bytesInUse, usedDBSize, fileInfo.Size(), usage.keys, meta.K, meta.Width, meta.Depth, meta.Decay(), meta.HeapSize)
}

func TestRedisTopKTop10WorksOn1MBackground(t *testing.T) {
	requireRedisProbPerfEnabled(t)

	db, path := openRedisProbPerfDB(t)
	ns := redisNamespaceForDB(0)
	key := []byte("perf:topk:hot")
	increments := redisProbPerfIncrements(redisProbPerfBatchSize)
	randFn := redisProbPerfRandFn(0x243f6a8885a308d3)

	err := db.Update(func(tx *storage.Tx) error {
		return redisTopKReserveTx(tx, ns, key, redisProbPerfTopKK, redisProbPerfTopKHotWidth, redisProbPerfTopKDepth, redisProbPerfTopKDecay, 0)
	})
	require.NoError(t, err)

	start := time.Now()
	for batchStart := 0; batchStart < redisProbPerfUniqueCount; batchStart += redisProbPerfBatchSize {
		batchCount := redisProbPerfBatchSize
		if remaining := redisProbPerfUniqueCount - batchStart; remaining < batchCount {
			batchCount = remaining
		}
		items := redisProbPerfBatchItems("cold", batchStart, batchCount)
		err = db.Update(func(tx *storage.Tx) error {
			var batchIncrements []uint64
			if batchCount == redisProbPerfBatchSize {
				batchIncrements = increments
			} else {
				batchIncrements = redisProbPerfIncrements(batchCount)
			}
			_, innerErr := redisTopKIncrByTx(tx, ns, key, items, batchIncrements, 0, randFn)
			return innerErr
		})
		require.NoError(t, err)
	}

	hotItems, hotIncrements := redisProbPerfTopKHotItems()
	err = db.Update(func(tx *storage.Tx) error {
		_, innerErr := redisTopKIncrByTx(tx, ns, key, hotItems, hotIncrements, 0, randFn)
		return innerErr
	})
	require.NoError(t, err)
	elapsed := time.Since(start)

	var entries []redisTopKHeapEntry
	err = db.View(func(tx *storage.Tx) error {
		var innerErr error
		entries, innerErr = redisTopKListTx(tx, ns, key, 0)
		return innerErr
	})
	require.NoError(t, err)
	require.Len(t, entries, int(redisProbPerfTopKK))
	for i, entry := range entries {
		require.Equal(t, string(hotItems[i]), string(entry.Item))
		if i > 0 {
			require.GreaterOrEqual(t, entries[i-1].Count, entry.Count)
		}
	}

	fileInfo, statErr := os.Stat(path)
	require.NoError(t, statErr)
	usage, usedDBSize := redisProbPerfUsage(db, 0)
	itemsProcessed := redisProbPerfUniqueCount
	for _, count := range hotIncrements {
		itemsProcessed += int(count)
	}
	itemsPerSecond := float64(itemsProcessed) / elapsed.Seconds()

	t.Logf("redis topk hotkeys correctness: cold_uniques=%d hot_total=%d batch=%d elapsed=%s items/sec=%.0f logical_bytes=%d used_db_size=%d file_bytes=%d top10_ok=true",
		redisProbPerfUniqueCount, itemsProcessed-redisProbPerfUniqueCount, redisProbPerfBatchSize, elapsed, itemsPerSecond, usage.bytesInUse, usedDBSize, fileInfo.Size())
}

func BenchmarkRedisBloomMAddBatch1000(b *testing.B) {
	db, _ := openRedisProbPerfDB(b)
	ns := redisNamespaceForDB(0)
	key := []byte("bench:bloom")
	totalItems := b.N * redisProbPerfBenchmarkBatch
	if totalItems <= 0 {
		totalItems = redisProbPerfBenchmarkBatch
	}

	err := db.Update(func(tx *storage.Tx) error {
		return redisBloomReserveTx(tx, ns, key, redisBloomReserveOptions{
			errorRate: redisProbPerfBloomErrorRate,
			capacity:  uint64(totalItems),
			expansion: redisBloomAutoCreateExpansion,
		}, 0)
	})
	if err != nil {
		b.Fatal(err)
	}

	b.ReportMetric(float64(redisProbPerfBenchmarkBatch), "items/tx")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		items := redisProbPerfBatchItems("bfb", i*redisProbPerfBenchmarkBatch, redisProbPerfBenchmarkBatch)
		err = db.Update(func(tx *storage.Tx) error {
			_, innerErr := redisBloomMAddTx(tx, ns, key, items, 0)
			return innerErr
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRedisBloomMExistsBatch1000(b *testing.B) {
	db, _ := openRedisProbPerfDB(b)
	ns := redisNamespaceForDB(0)
	key := []byte("bench:bloom:exists")
	totalItems := b.N * redisProbPerfBenchmarkBatch
	if totalItems <= 0 {
		totalItems = redisProbPerfBenchmarkBatch
	}

	err := db.Update(func(tx *storage.Tx) error {
		if reserveErr := redisBloomReserveTx(tx, ns, key, redisBloomReserveOptions{
			errorRate: redisProbPerfBloomErrorRate,
			capacity:  uint64(totalItems),
			expansion: redisBloomAutoCreateExpansion,
		}, 0); reserveErr != nil {
			return reserveErr
		}
		for i := 0; i < b.N; i++ {
			items := redisProbPerfBatchItems("bfe", i*redisProbPerfBenchmarkBatch, redisProbPerfBenchmarkBatch)
			if _, addErr := redisBloomMAddTx(tx, ns, key, items, 0); addErr != nil {
				return addErr
			}
		}
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}

	b.ReportMetric(float64(redisProbPerfBenchmarkBatch), "items/tx")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		items := redisProbPerfBatchItems("bfe", i*redisProbPerfBenchmarkBatch, redisProbPerfBenchmarkBatch)
		err = db.View(func(tx *storage.Tx) error {
			results, innerErr := redisBloomMExistsTx(tx, ns, key, items, 0)
			if innerErr != nil {
				return innerErr
			}
			for _, result := range results {
				if result != 1 {
					b.Fatalf("expected bloom exists hit, got %d", result)
				}
			}
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRedisTopKAddBatch1000(b *testing.B) {
	db, _ := openRedisProbPerfDB(b)
	ns := redisNamespaceForDB(0)
	key := []byte("bench:topk")
	increments := redisProbPerfIncrements(redisProbPerfBenchmarkBatch)

	err := db.Update(func(tx *storage.Tx) error {
		return redisTopKReserveTx(tx, ns, key, redisProbPerfTopKK, redisProbPerfTopKWidth, redisProbPerfTopKDepth, redisProbPerfTopKDecay, 0)
	})
	if err != nil {
		b.Fatal(err)
	}

	randFn := redisProbPerfRandFn(0x243f6a8885a308d3)

	b.ReportMetric(float64(redisProbPerfBenchmarkBatch), "items/tx")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		items := redisProbPerfBatchItems("tkb", i*redisProbPerfBenchmarkBatch, redisProbPerfBenchmarkBatch)
		err = db.Update(func(tx *storage.Tx) error {
			_, innerErr := redisTopKIncrByTx(tx, ns, key, items, increments, 0, randFn)
			return innerErr
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestRedisProbPerfRoundTripSanity(t *testing.T) {
	_, conn := setupTestRedisServer(t)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writeRedisCommand(t, writer, "TOPK.RESERVE", "tk", "3")
	require.Equal(t, "OK", readRedisReply(t, reader))
	writeRedisCommand(t, writer, "BF.RESERVE", "bf", "0.01", "100")
	require.Equal(t, "OK", readRedisReply(t, reader))
}
