package main

import (
	"github.com/stretchr/testify/require"
	"github.com/timson/pirindb/storage"
	"log/slog"
	"os"
	"testing"
)

func TestConsistentHash_AddShard(t *testing.T) {
	slog.SetLogLoggerLevel(slog.LevelDebug)
	shardA := &Shard{Name: "aleph", Status: 1, Host: "10.0.0.1", Port: 5432}
	shardB := &Shard{Name: "bet", Status: 1, Host: "10.0.0.2", Port: 5432}
	shardC := &Shard{Name: "gimel", Status: 1, Host: "10.0.0.3", Port: 5432}
	shards := make(map[string]*Shard)
	shards["aleph"] = shardA
	shards["bet"] = shardB
	shards["gimel"] = shardC

	// Create initial consistent hash ring
	ch := NewRingHash(shards)
	testKeys := []string{"user:123", "user:999", "order:456", "session:abc", "user:124"}

	// Store shard results from original ring
	originalShards := make(map[string]string)
	t.Log("===> Key redirection BEFORE save/load:")
	for _, key := range testKeys {
		shard := ch.GetShard(key)
		require.NotNil(t, shard, "shard for key %q should not be nil", key)
		originalShards[key] = shard.Name
		t.Logf("Key %-12s → %s (%s:%d)", key, shard.Name, shard.Host, shard.Port)
	}

	// Persist to DB
	dbPath := "shard-test.db"
	_ = os.Remove(dbPath)
	db, err := storage.Open(dbPath, nil)
	require.NoError(t, err)
	defer db.Close()
	defer os.Remove(dbPath)

	db.Update(func(tx *storage.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("test"))
		require.NoError(t, err)
		bucket.Put([]byte("test1"), []byte("test1"))
		return nil
	})
	err = SaveRingHash(db, ch)
	require.NoError(t, err)
	err = SaveRingHash(db, ch)
	require.NoError(t, err)
	err = SaveRingHash(db, ch)
	require.NoError(t, err)
	db.Close()

	db, err = storage.Open(dbPath, nil)

	// Load from DB
	ch2, err := LoadRingHash(db, shards)
	require.NoError(t, err)

	// Validate ring content
	require.Equal(t, ch.hash, ch2.hash, "hash slices must be equal")
	require.Equal(t, ch.HashMap, ch2.HashMap, "hash maps must be equal")

	t.Log("===> Key redirection AFTER load:")
	for _, key := range testKeys {
		shard := ch2.GetShard(key)
		require.NotNil(t, shard, "shard for key %q should not be nil after load", key)
		t.Logf("Key %-12s → %s (%s:%d)", key, shard.Name, shard.Host, shard.Port)
		require.Equal(t, originalShards[key], shard.Name, "shard assignment for key %q changed", key)
	}
}
