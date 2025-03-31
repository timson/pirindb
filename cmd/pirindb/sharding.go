package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/OneOfOne/xxhash"
	"github.com/timson/pirindb/storage"
	"reflect"
	"sort"
)

const (
	VirtualShardsPerNode = 1000
	ShardInSync          = 0
	ShardSyncing         = 1
	ShardOutSync         = 2
	ShardDead            = 3
)

type Shard struct {
	Name   string
	Status int
	Host   string
	Port   int
}

type ConsistentHash struct {
	Ring      []uint32          `json:"ring"`
	HashMap   map[uint32]string `json:"hash_map"`
	Timestamp uint64            `json:"timestamp"`
	Shards    map[string]*Shard `json:"-"`
}

func NewConsistentHash() *ConsistentHash {
	return &ConsistentHash{
		HashMap: make(map[uint32]string),
		Shards:  make(map[string]*Shard),
	}
}

func hashKey(key string) uint32 {
	return xxhash.ChecksumString32(key)
}

// AddShard adds new shard to ConsistentHash and recalculate ring
func (ch *ConsistentHash) AddShard(shard *Shard) {
	if _, ok := ch.Shards[shard.Name]; ok {
		return
	}
	ch.Shards[shard.Name] = shard

	for idx := 0; idx < VirtualShardsPerNode; idx++ {
		virtualID := fmt.Sprintf("%s-%d", shard.Name, idx)
		hash := hashKey(virtualID)
		ch.Ring = append(ch.Ring, hash)
		ch.HashMap[hash] = shard.Name
	}

	sort.Slice(ch.Ring, func(i, j int) bool {
		return ch.Ring[i] < ch.Ring[j]
	})
}

//func (ch *ConsistentHash) Shards() []*Shard {
//	values := make([]*Shard, 0, len(ch.HashMap))
//	idx := 0
//	for _, shard := range ch.HashMap {
//		values[idx] = shard
//		idx++
//	}
//	return values
//}

func (ch *ConsistentHash) GetShard(key string) *Shard {
	if len(ch.Ring) == 0 {
		return nil
	}
	hash := hashKey(key)
	idx := sort.Search(len(ch.Ring), func(i int) bool {
		return ch.Ring[i] >= hash
	})
	if idx == len(ch.Ring) {
		idx = 0
	}
	shardName := ch.HashMap[ch.Ring[idx]]
	return ch.Shards[shardName]
}

func (ch *ConsistentHash) save(db *storage.DB) error {
	err := db.Update(func(tx *storage.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(ShardBucket)
		if err != nil {
			return err
		}
		data, marshalErr := json.Marshal(ch)
		if marshalErr != nil {
			return marshalErr
		}
		err = bucket.Put([]byte("consistent_hash"), data)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (ch *ConsistentHash) load(db *storage.DB) (*ConsistentHash, error) {
	storedConsistentHash := &ConsistentHash{}

	err := db.View(func(tx *storage.Tx) error {
		bucket, err := tx.GetBucket(ShardBucket)
		if err != nil {
			return err
		}
		data, found := bucket.Get([]byte("consistent_hash"))
		if !found {
			return ErrConsistentHashNotFound
		}
		err = json.Unmarshal(data, &storedConsistentHash)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return storedConsistentHash, nil
}

func (ch *ConsistentHash) Sync(db *storage.DB) error {
	storedConsistentHash, err := ch.load(db)
	if err != nil && !errors.Is(err, ErrConsistentHashNotFound) {
		err = ch.save(db)
		if err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	if reflect.DeepEqual(ch, storedConsistentHash) {
		
	}

}
