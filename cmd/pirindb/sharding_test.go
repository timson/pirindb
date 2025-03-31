package main

import (
	"fmt"
	"testing"
)

func TestConsistentHash_AddShard(t *testing.T) {
	ch := NewConsistentHash()

	shardA := &Shard{Name: "aleph", Status: 1, Host: "10.0.0.1", Port: 5432}
	shardB := &Shard{Name: "bet", Status: 1, Host: "10.0.0.2", Port: 5432}

	ch.AddShard(shardA)
	ch.AddShard(shardB)

	testKeys := []string{"user:123", "user:999", "order:456", "session:abc", "user:124"}

	for _, key := range testKeys {
		shard := ch.GetShard(key)
		if shard != nil {
			fmt.Printf("Key %-12s → %s (%s:%d)\n", key, shard.Name, shard.Host, shard.Port)
		}
	}
}
