package main

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/timson/pirindb/storage"
)

func openRedisCoreBenchmarkDB(b *testing.B) *storage.DB {
	b.Helper()
	path := storage.TempFileName(".db")
	db, err := storage.Open(path, storage.DefaultOptions().
		WithSyncPolicy(storage.SyncPolicyJournal).
		WithCheckpointTxThreshold(1024))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		_ = db.Close()
		_ = os.Remove(path)
		_ = os.Remove(db.GetOptions().TxLogPath)
	})
	return db
}

func prepareRedisListBenchmark(b *testing.B, count int) (*storage.DB, redisNamespace, []byte) {
	b.Helper()
	db := openRedisCoreBenchmarkDB(b)
	ns := redisNamespaceForDB(0)
	key := []byte("benchmark-list")
	values := make([][]byte, count)
	for i := range values {
		values[i] = bytes.Repeat([]byte{byte(i)}, 64)
	}
	if err := db.Update(func(tx *storage.Tx) error {
		_, innerErr := redisPushTx(tx, ns, key, values, false, 0)
		return innerErr
	}); err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(count), "list_values")
	return db, ns, key
}

func BenchmarkRedisStringGet(b *testing.B) {
	db := openRedisCoreBenchmarkDB(b)
	ns := redisNamespaceForDB(0)
	key := []byte("string-key")
	if err := PutRedisBytes(db, ns, key, []byte("value"), 0); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, found, err := GetRedisBytes(db, ns, key, 0); err != nil || !found {
			b.Fatalf("get failed: found=%t err=%v", found, err)
		}
	}
}

func BenchmarkRedisRawStringTypeLookup(b *testing.B) {
	db := openRedisCoreBenchmarkDB(b)
	ns := redisNamespaceForDB(0)
	key := []byte("string-key")
	if err := PutRedisBytes(db, ns, key, []byte("value"), 0); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.View(func(tx *storage.Tx) error {
			keyType, innerErr := redisRawKeyTypeTx(tx, ns, key)
			if innerErr == nil && keyType != redisKeyTypeString {
				b.Fatalf("unexpected key type %q", keyType)
			}
			return innerErr
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRedisListIndexHead50000(b *testing.B) {
	db, ns, key := prepareRedisListBenchmark(b, 50_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.View(func(tx *storage.Tx) error {
			_, found, innerErr := redisListIndexTx(tx, ns, key, 0, 0)
			if innerErr == nil && !found {
				b.Fatal("list value not found")
			}
			return innerErr
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRedisListIndexTail50000(b *testing.B) {
	db, ns, key := prepareRedisListBenchmark(b, 50_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.View(func(tx *storage.Tx) error {
			_, found, innerErr := redisListIndexTx(tx, ns, key, -1, 0)
			if innerErr == nil && !found {
				b.Fatal("list value not found")
			}
			return innerErr
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRedisListRangeHead10Of50000(b *testing.B) {
	db, ns, key := prepareRedisListBenchmark(b, 50_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.View(func(tx *storage.Tx) error {
			values, innerErr := redisListRangeTx(tx, ns, key, 0, 9, 0)
			if innerErr == nil && len(values) != 10 {
				b.Fatalf("unexpected range length %d", len(values))
			}
			return innerErr
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRedisListSetHead50000(b *testing.B) {
	db, ns, key := prepareRedisListBenchmark(b, 50_000)
	values := [][]byte{bytes.Repeat([]byte("a"), 64), bytes.Repeat([]byte("b"), 64)}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Update(func(tx *storage.Tx) error {
			return redisListSetTx(tx, ns, key, 0, values[i%len(values)], 0)
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkRedisStandardPipeline(b *testing.B, depth int) {
	b.Helper()
	path := storage.TempFileName(".db")
	opts := storage.DefaultOptions().
		WithSyncPolicy(storage.SyncPolicyGroup).
		WithGroupCommitTxThreshold(16).
		WithGroupCommitWindow(2 * time.Millisecond)
	db, err := storage.Open(path, opts)
	if err != nil {
		b.Fatal(err)
	}
	cfg := &Config{Redis: &RedisConfig{
		Enabled:             true,
		PipelineMaxCommands: 256,
		PipelineMaxBytes:    8 * 1024 * 1024,
	}}
	srv := NewRedisServer(cfg, db, createLogger("ERROR"))
	serverConn, clientConn := net.Pipe()
	serverDone := make(chan struct{})
	go func() {
		srv.handleConn(serverConn)
		close(serverDone)
	}()
	b.Cleanup(func() {
		_ = clientConn.Close()
		<-serverDone
		_ = db.Close()
		_ = os.Remove(path)
		_ = os.Remove(opts.TxLogPath)
	})

	var payload bytes.Buffer
	for idx := 0; idx < depth; idx++ {
		appendRedisCommand(b, &payload, "INCR", fmt.Sprintf("pipeline-benchmark:%03d", idx))
	}
	payloadBytes := payload.Bytes()
	reader := bufio.NewReader(clientConn)
	b.SetBytes(int64(len(payloadBytes)))
	b.ResetTimer()
	started := time.Now()
	for iteration := 0; iteration < b.N; iteration++ {
		writeDone := make(chan error, 1)
		go func() {
			_, writeErr := clientConn.Write(payloadBytes)
			writeDone <- writeErr
		}()
		for idx := 0; idx < depth; idx++ {
			if _, ok := readRedisReply(b, reader).(int64); !ok {
				b.Fatal("pipeline benchmark received a non-integer INCR reply")
			}
		}
		if err = <-writeDone; err != nil {
			b.Fatal(err)
		}
	}
	elapsed := time.Since(started)
	b.StopTimer()
	commands := float64(b.N * depth)
	b.ReportMetric(float64(depth), "commands/op")
	b.ReportMetric(commands/elapsed.Seconds(), "commands/s")
	b.ReportMetric(float64(elapsed.Nanoseconds())/commands, "ns/command")
}

func BenchmarkRedisStandardPipelineDepth1(b *testing.B) {
	benchmarkRedisStandardPipeline(b, 1)
}

func BenchmarkRedisStandardPipelineDepth16(b *testing.B) {
	benchmarkRedisStandardPipeline(b, 16)
}

func BenchmarkRedisStandardPipelineDepth64(b *testing.B) {
	benchmarkRedisStandardPipeline(b, 64)
}

func BenchmarkRedisStandardPipelineDepth256(b *testing.B) {
	benchmarkRedisStandardPipeline(b, 256)
}
