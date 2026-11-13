package main

import (
	"encoding/binary"
	"hash/fnv"
	"sort"
	"sync"
)

const redisMutationLockShardCount = 256

// redisMutationLocks bounds lock memory independently of key cardinality. A
// collision only serializes unrelated mutations; it cannot weaken isolation.
type redisMutationLocks struct {
	shards [redisMutationLockShardCount]sync.Mutex
}

func redisMutationLockShard(dbIndex int, key []byte) int {
	hasher := fnv.New64a()
	var db [8]byte
	binary.BigEndian.PutUint64(db[:], uint64(dbIndex))
	_, _ = hasher.Write(db[:])
	_, _ = hasher.Write(key)
	return int(hasher.Sum64() % redisMutationLockShardCount)
}

func (locks *redisMutationLocks) lockKeys(dbIndex int, keys [][]byte, all bool) func() {
	indexes := make([]int, 0, len(keys))
	if all {
		indexes = make([]int, redisMutationLockShardCount)
		for idx := range indexes {
			indexes[idx] = idx
		}
	} else {
		seen := make(map[int]struct{}, len(keys))
		for _, key := range keys {
			idx := redisMutationLockShard(dbIndex, key)
			if _, exists := seen[idx]; exists {
				continue
			}
			seen[idx] = struct{}{}
			indexes = append(indexes, idx)
		}
		sort.Ints(indexes)
	}

	for _, idx := range indexes {
		locks.shards[idx].Lock()
	}
	return func() {
		for idx := len(indexes) - 1; idx >= 0; idx-- {
			locks.shards[indexes[idx]].Unlock()
		}
	}
}

func (srv *RedisServer) lockRedisCommandMutations(selectedDB int, args [][]byte) (func(), error) {
	if len(args) == 0 || !isRedisWriteCommand(stringUpper(args[0])) {
		return func() {}, nil
	}
	command := stringUpper(args[0])
	if command == "BLPOP" || command == "BRPOP" || command == "BRPOPLPUSH" {
		return func() {}, nil
	}
	keys, err := redisCommandKeys(args)
	if err != nil {
		return nil, err
	}
	return srv.mutationLocks.lockKeys(selectedDB, keys, command == "FLUSHDB" || command == "FLUSHALL"), nil
}

func stringUpper(value []byte) string {
	// Command names are short; keeping conversion here centralizes lock routing.
	result := make([]byte, len(value))
	for idx, ch := range value {
		if ch >= 'a' && ch <= 'z' {
			ch -= 'a' - 'A'
		}
		result[idx] = ch
	}
	return string(result)
}
