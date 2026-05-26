package main

import (
	"bytes"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/timson/pirindb/storage"
)

const (
	defaultDBPath                    = "playground/redis_disk_perf/redis-disk-perf.db"
	defaultTotalBytes                = "8GiB"
	defaultValueBytes                = 1024
	defaultBatchSize                 = 5000
	defaultRandomReads               = 100000
	defaultKeyPrefix                 = "disk"
	defaultRedisDB                   = 0
	defaultCheckpointThreshold       = 256
	defaultReadSeed            int64 = 1
	defaultProgressInterval          = time.Second
)

type config struct {
	dbPath                string
	totalBytes            uint64
	valueBytes            int
	batchSize             int
	randomReads           int
	keyPrefix             string
	redisDB               int
	resetDB               bool
	prepareOnly           bool
	readOnly              bool
	syncPolicy            string
	checkpointTxThreshold int
	readSeed              int64
	progressInterval      time.Duration
}

type progressPrinter struct {
	label       string
	unit        string
	total       int
	startedAt   time.Time
	lastPrinted time.Time
	interval    time.Duration
}

func main() {
	cfg := parseFlags()

	if cfg.prepareOnly && cfg.readOnly {
		fatalf("-prepare-only and -read-only are mutually exclusive")
	}

	opts := storage.DefaultOptions().
		WithSyncPolicy(parseSyncPolicy(cfg.syncPolicy)).
		WithCheckpointTxThreshold(cfg.checkpointTxThreshold)

	if cfg.prepareOnly || !cfg.readOnly {
		if cfg.resetDB {
			if err := resetDBFiles(cfg.dbPath); err != nil {
				fatalf("reset db files: %v", err)
			}
		}
		db, err := storage.Open(cfg.dbPath, opts)
		if err != nil {
			fatalf("open db for prepare: %v", err)
		}
		if err = fillRedisStringDataset(db, cfg); err != nil {
			_ = db.Close()
			fatalf("fill dataset: %v", err)
		}
		if err = db.Close(); err != nil {
			fatalf("close db after prepare: %v", err)
		}
	}

	if cfg.prepareOnly {
		return
	}

	db, err := storage.Open(cfg.dbPath, opts)
	if err != nil {
		fatalf("open db for random reads: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err = runRandomReads(db, cfg); err != nil {
		fatalf("random reads: %v", err)
	}
}

func parseFlags() config {
	var (
		dbPath                = flag.String("db", defaultDBPath, "Path to DB file")
		totalBytesRaw         = flag.String("total-bytes", defaultTotalBytes, "Approximate dataset size to fill, e.g. 4GiB, 8GiB, 500MB")
		valueBytes            = flag.Int("value-bytes", defaultValueBytes, "Value size in bytes for each Redis string key")
		batchSize             = flag.Int("batch-size", defaultBatchSize, "How many keys to write per batch transaction")
		randomReads           = flag.Int("random-reads", defaultRandomReads, "How many random GET-style reads to execute")
		keyPrefix             = flag.String("key-prefix", defaultKeyPrefix, "Key prefix used for generated Redis keys")
		redisDB               = flag.Int("redis-db", defaultRedisDB, "Redis logical DB index to emulate in bucket naming")
		resetDB               = flag.Bool("reset-db", true, "Remove existing DB/tlog files before prepare phase")
		prepareOnly           = flag.Bool("prepare-only", false, "Only fill the dataset and exit")
		readOnly              = flag.Bool("read-only", false, "Skip fill and only run random reads against an existing DB")
		syncPolicy            = flag.String("sync-policy", "journal", "Storage sync policy: strict, journal, or group")
		checkpointTxThreshold = flag.Int("checkpoint-tx-threshold", defaultCheckpointThreshold, "Journal checkpoint threshold for sync-policy=journal")
		readSeed              = flag.Int64("read-seed", defaultReadSeed, "Seed for random read key selection")
		progressInterval      = flag.Duration("progress-interval", defaultProgressInterval, "How often to print progress updates; 0 disables periodic progress")
	)
	flag.Parse()

	totalBytes, err := parseByteSize(*totalBytesRaw)
	if err != nil {
		fatalf("parse -total-bytes: %v", err)
	}
	if *valueBytes <= 0 {
		fatalf("-value-bytes must be positive")
	}
	if *batchSize <= 0 {
		fatalf("-batch-size must be positive")
	}
	if *randomReads <= 0 {
		fatalf("-random-reads must be positive")
	}
	if *redisDB < 0 {
		fatalf("-redis-db must be non-negative")
	}

	return config{
		dbPath:                *dbPath,
		totalBytes:            totalBytes,
		valueBytes:            *valueBytes,
		batchSize:             *batchSize,
		randomReads:           *randomReads,
		keyPrefix:             *keyPrefix,
		redisDB:               *redisDB,
		resetDB:               *resetDB,
		prepareOnly:           *prepareOnly,
		readOnly:              *readOnly,
		syncPolicy:            *syncPolicy,
		checkpointTxThreshold: *checkpointTxThreshold,
		readSeed:              *readSeed,
		progressInterval:      *progressInterval,
	}
}

func parseByteSize(raw string) (uint64, error) {
	s := strings.TrimSpace(strings.ToUpper(raw))
	multiplier := uint64(1)
	switch {
	case strings.HasSuffix(s, "GIB"):
		multiplier = 1024 * 1024 * 1024
		s = strings.TrimSuffix(s, "GIB")
	case strings.HasSuffix(s, "GB"):
		multiplier = 1000 * 1000 * 1000
		s = strings.TrimSuffix(s, "GB")
	case strings.HasSuffix(s, "MIB"):
		multiplier = 1024 * 1024
		s = strings.TrimSuffix(s, "MIB")
	case strings.HasSuffix(s, "MB"):
		multiplier = 1000 * 1000
		s = strings.TrimSuffix(s, "MB")
	case strings.HasSuffix(s, "KIB"):
		multiplier = 1024
		s = strings.TrimSuffix(s, "KIB")
	case strings.HasSuffix(s, "KB"):
		multiplier = 1000
		s = strings.TrimSuffix(s, "KB")
	case strings.HasSuffix(s, "B"):
		s = strings.TrimSuffix(s, "B")
	}
	value, err := strconv.ParseUint(strings.TrimSpace(s), 10, 64)
	if err != nil {
		return 0, err
	}
	return value * multiplier, nil
}

func parseSyncPolicy(raw string) storage.SyncPolicy {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "strict":
		return storage.SyncPolicyStrict
	case "journal":
		return storage.SyncPolicyJournal
	case "group":
		return storage.SyncPolicyGroup
	default:
		fatalf("unsupported -sync-policy %q", raw)
	}
	return storage.SyncPolicyJournal
}

func resetDBFiles(dbPath string) error {
	if err := os.Remove(dbPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	txLogPath := strings.TrimSuffix(dbPath, filepath.Ext(dbPath)) + ".tlog"
	if err := os.Remove(txLogPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func redisStringBucketName(dbIndex int) []byte {
	return []byte("__redis_db_" + strconv.Itoa(dbIndex) + "__")
}

func redisPerfKey(prefix string, index int) []byte {
	key := make([]byte, 0, len(prefix)+1+20)
	key = append(key, prefix...)
	key = append(key, ':')
	key = strconv.AppendInt(key, int64(index), 10)
	return key
}

func totalKeys(cfg config) int {
	keys := int(cfg.totalBytes / uint64(cfg.valueBytes))
	if keys <= 0 {
		return 1
	}
	return keys
}

func newProgressPrinter(label string, total int, unit string, interval time.Duration) *progressPrinter {
	now := time.Now()
	return &progressPrinter{
		label:       label,
		unit:        unit,
		total:       total,
		startedAt:   now,
		lastPrinted: now,
		interval:    interval,
	}
}

func (p *progressPrinter) Advance(current int) {
	now := time.Now()
	if current < p.total && (p.interval <= 0 || now.Sub(p.lastPrinted) < p.interval) {
		return
	}

	elapsed := now.Sub(p.startedAt)
	if elapsed <= 0 {
		elapsed = time.Millisecond
	}
	rate := float64(current) / elapsed.Seconds()
	percent := 100.0
	if p.total > 0 {
		percent = (float64(current) / float64(p.total)) * 100
	}

	etaText := "done"
	if current < p.total && rate > 0 {
		remaining := float64(p.total-current) / rate
		etaText = (time.Duration(remaining * float64(time.Second))).Round(time.Second).String()
	}

	fmt.Printf("%s progress: %d/%d (%.1f%%) %s/sec=%.0f elapsed=%s eta=%s\n",
		p.label,
		current,
		p.total,
		percent,
		p.unit,
		rate,
		elapsed.Round(time.Second),
		etaText,
	)

	p.lastPrinted = now
}

func fillRedisStringDataset(db *storage.DB, cfg config) error {
	value := bytes.Repeat([]byte("v"), cfg.valueBytes)
	bucketName := redisStringBucketName(cfg.redisDB)
	keys := totalKeys(cfg)

	fmt.Printf("prepare: db=%s total_keys=%d value_bytes=%d approx_dataset_bytes=%d batch=%d sync_policy=%s checkpoint_tx_threshold=%d\n",
		cfg.dbPath, keys, cfg.valueBytes, uint64(keys)*uint64(cfg.valueBytes), cfg.batchSize, cfg.syncPolicy, cfg.checkpointTxThreshold)

	start := time.Now()
	progress := newProgressPrinter("prepare", keys, "keys", cfg.progressInterval)
	for batchStart := 0; batchStart < keys; batchStart += cfg.batchSize {
		batchCount := cfg.batchSize
		if remaining := keys - batchStart; remaining < batchCount {
			batchCount = remaining
		}
		err := db.Update(func(tx *storage.Tx) error {
			bucket, err := tx.CreateBucketIfNotExists(bucketName)
			if err != nil {
				return err
			}
			for i := 0; i < batchCount; i++ {
				if err = bucket.Put(redisPerfKey(cfg.keyPrefix, batchStart+i), value); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
		progress.Advance(batchStart + batchCount)
	}

	elapsed := time.Since(start)
	fileInfo, err := os.Stat(cfg.dbPath)
	if err != nil {
		return err
	}
	stat := db.Stat()
	bucketStat := stat.Buckets[string(bucketName)]

	var logicalBytes uint64
	var keyCount uint64
	if bucketStat != nil {
		logicalBytes = bucketStat.BytesInUse
		keyCount = bucketStat.ItemsN
	}

	fmt.Printf("prepare done: elapsed=%s keys/sec=%.0f logical_bytes=%d used_db_size=%d file_bytes=%d db_keys=%d\n",
		elapsed, float64(keys)/elapsed.Seconds(), logicalBytes, stat.UsedDBSize, fileInfo.Size(), keyCount)
	return nil
}

func runRandomReads(db *storage.DB, cfg config) error {
	keys := totalKeys(cfg)
	bucketName := redisStringBucketName(cfg.redisDB)
	rng := rand.New(rand.NewSource(cfg.readSeed))

	fmt.Printf("read: db=%s random_reads=%d keyspace=%d seed=%d\n", cfg.dbPath, cfg.randomReads, keys, cfg.readSeed)

	start := time.Now()
	progress := newProgressPrinter("read", cfg.randomReads, "ops", cfg.progressInterval)
	for i := 0; i < cfg.randomReads; i++ {
		keyID := rng.Intn(keys)
		err := db.View(func(tx *storage.Tx) error {
			bucket, err := tx.GetBucket(bucketName)
			if err != nil {
				return err
			}
			value, found := bucket.Get(redisPerfKey(cfg.keyPrefix, keyID))
			if !found {
				return fmt.Errorf("missing key %d", keyID)
			}
			if len(value) != cfg.valueBytes {
				return fmt.Errorf("unexpected value size for key %d: got=%d want=%d", keyID, len(value), cfg.valueBytes)
			}
			return nil
		})
		if err != nil {
			return err
		}
		progress.Advance(i + 1)
	}
	elapsed := time.Since(start)

	fileInfo, err := os.Stat(cfg.dbPath)
	if err != nil {
		return err
	}
	stat := db.Stat()
	bucketStat := stat.Buckets[string(bucketName)]

	var logicalBytes uint64
	var keyCount uint64
	if bucketStat != nil {
		logicalBytes = bucketStat.BytesInUse
		keyCount = bucketStat.ItemsN
	}

	fmt.Printf("read done: elapsed=%s read_ops/sec=%.0f logical_bytes=%d used_db_size=%d file_bytes=%d db_keys=%d\n",
		elapsed, float64(cfg.randomReads)/elapsed.Seconds(), logicalBytes, stat.UsedDBSize, fileInfo.Size(), keyCount)
	return nil
}

func fatalf(format string, args ...any) {
	_, _ = fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
