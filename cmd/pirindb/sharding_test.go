package main

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"github.com/timson/pirindb/storage"
	"testing"
)

func TestConsistentHash_AddShard(t *testing.T) {
	ch := NewConsistentHash()

	shardA := &Shard{Name: "aleph", Status: 1, Host: "10.0.0.1", Port: 5432}
	shardB := &Shard{Name: "bet", Status: 1, Host: "10.0.0.2", Port: 5432}
	shardC := &Shard{Name: "gimel", Status: 1, Host: "10.0.0.3", Port: 5432}

	ch.AddShard(shardA)
	ch.AddShard(shardB)
	ch.AddShard(shardC)

	testKeys := []string{"user:123", "user:999", "order:456", "session:abc", "user:124"}

	for _, key := range testKeys {
		shard := ch.GetShard(key)
		if shard != nil {
			fmt.Printf("Key %-12s → %s (%s:%d)\n", key, shard.Name, shard.Host, shard.Port)
		}
	}

	//	_ = os.Remove("shard-test.db")
	db, err := storage.Open("shard-test.db", 0600)
	require.NoError(t, err)
	err = ch.Sync(db)
	require.NoError(t, err)
}
