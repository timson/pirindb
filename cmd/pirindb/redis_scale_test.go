package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/timson/pirindb/storage"
)

// TestRedisMillionCompositeKeysScaleGate is intentionally opt-in: it verifies
// the release-scale bucket-catalog and INFO latency gates without adding a
// million-key fixture to every normal test run.
func TestRedisMillionCompositeKeysScaleGate(t *testing.T) {
	if os.Getenv("PIRINDB_REDIS_SCALE_TEST") != "1" {
		t.Skip("set PIRINDB_REDIS_SCALE_TEST=1 to run the million-key Redis scale gate")
	}

	const (
		keyCount  = 1_000_000
		batchSize = 1_000
	)
	db := openRedisKeyMetaTestDB(t, filepath.Join(t.TempDir(), "million-composite-keys.db"))
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	ns := redisNamespaceForDB(0)
	require.NoError(t, db.Update(func(tx *storage.Tx) error {
		return saveRedisKeyDirectoryStateTx(tx, ns, redisKeyDirectoryState{
			Phase: redisKeyDirectoryPhaseActive, BucketIndex: byte(len(redisLegacyKeySources)),
		})
	}))

	started := time.Now()
	fixedBucketCount := 0
	for base := 0; base < keyCount; base += batchSize {
		require.NoError(t, db.Update(func(tx *storage.Tx) error {
			for idx := base; idx < base+batchSize; idx++ {
				key := []byte(fmt.Sprintf("hash:%07d", idx))
				if _, err := redisHashSetTx(tx, ns, key, []hashFieldValuePair{{
					field: []byte("field"), value: []byte("value"),
				}}, 1000); err != nil {
					return err
				}
			}
			return nil
		}))
		if base == 0 {
			stat, err := db.StatE()
			require.NoError(t, err)
			fixedBucketCount = len(stat.Buckets)
		}
	}
	elapsed := time.Since(started)
	t.Logf("created %d shared-bucket hashes in %s (%.0f keys/s)", keyCount, elapsed, float64(keyCount)/elapsed.Seconds())

	stat, err := db.StatE()
	require.NoError(t, err)
	require.Equal(t, fixedBucketCount, len(stat.Buckets), "physical bucket count must remain independent of key count")
	require.Equal(t, uint64(keyCount), stat.Buckets[string(ns.hashMetaBucket)].ItemsN)
	require.Equal(t, uint64(keyCount), stat.Buckets[string(ns.hashDataBucket)].ItemsN)
	for name := range stat.Buckets {
		require.False(t, strings.HasPrefix(name, string(ns.hashDataPrefix)), "new hashes must never create private buckets")
	}

	srv := NewRedisServer(&Config{Redis: &RedisConfig{Enabled: true}}, db, createLogger("ERROR"))
	latencies := make([]time.Duration, 20)
	for idx := range latencies {
		infoStarted := time.Now()
		_, err = srv.buildRedisInfo(2000)
		require.NoError(t, err)
		latencies[idx] = time.Since(infoStarted)
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p95 := latencies[18]
	t.Logf("INFO latency at %d keys: p95=%s", keyCount, p95)
	require.Less(t, p95, 200*time.Millisecond)

	_, err = db.Check()
	require.NoError(t, err)
}
