package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/OneOfOne/xxhash"
	"github.com/timson/pirindb/storage"
	"sort"
)

const (
	VirtualShardsPerNode = 1000
)

type KVPair struct {
	Key   uint32
	Value string
}

type RingHash struct {
	Positions       []uint32          // Sorted virtual node positions
	HashMap         map[uint32]string // maps position hash to shard name
	SerializedPairs []KVPair          // serialized map slice for deterministic storage
	shards          map[string]*Shard // available shard definitions
	hash            uint64            // serialized ring hash
}

func NewRingHash(shards map[string]*Shard) *RingHash {
	ring := &RingHash{
		HashMap: make(map[uint32]string),
		shards:  shards,
	}
	for shardName, shard := range shards {
		if shard.skip {
			continue
		}
		for idx := 0; idx < VirtualShardsPerNode; idx++ {
			virtualID := fmt.Sprintf("%s-%d", shardName, idx)
			hash := hashKey(virtualID)
			ring.Positions = append(ring.Positions, hash)
			ring.HashMap[hash] = shardName
		}
	}
	sort.Slice(ring.Positions, func(i, j int) bool {
		return ring.Positions[i] < ring.Positions[j]
	})
	data, err := ring.serialize()
	if err != nil {
		return nil
	}
	hash := xxhash.New64()
	_, err = hash.Write(data)
	if err != nil {
		return nil
	}
	ring.hash = hash.Sum64()
	return ring
}

func (rh *RingHash) serialize() ([]byte, error) {
	kv := make([]KVPair, 0, len(rh.HashMap))
	for k, v := range rh.HashMap {
		kv = append(kv, KVPair{Key: k, Value: v})
	}
	sort.Slice(kv, func(i, j int) bool { return kv[i].Key < kv[j].Key })
	ringHashSerialize := RingHash{
		Positions:       rh.Positions,
		SerializedPairs: kv,
	}
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(&ringHashSerialize)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), err
}

func (rh *RingHash) deserialize(data []byte) error {
	err := gob.NewDecoder(bytes.NewReader(data)).Decode(rh)
	if err != nil {
		return err
	}
	rh.HashMap = make(map[uint32]string)
	for _, slicePair := range rh.SerializedPairs {
		rh.HashMap[slicePair.Key] = slicePair.Value
	}

	hash := xxhash.New64()
	_, err = hash.Write(data)
	if err != nil {
		return err
	}
	rh.hash = hash.Sum64()
	return nil
}

func (rh *RingHash) Equal(other *RingHash) bool {
	if other == nil {
		return false
	}
	return rh.hash == other.hash
}

func hashKey(key string) uint32 {
	return xxhash.ChecksumString32(key)
}

// GetShard return shard, which serves the given key
func (rh *RingHash) GetShard(key string) *Shard {
	if len(rh.Positions) == 0 {
		return nil
	}
	hash := hashKey(key)
	idx := sort.Search(len(rh.Positions), func(i int) bool {
		return rh.Positions[i] >= hash
	})
	if idx == len(rh.Positions) {
		idx = 0
	}
	shardName := rh.HashMap[rh.Positions[idx]]
	return rh.shards[shardName]
}

func SaveRingHash(db *storage.DB, ch *RingHash) error {
	return db.Update(func(tx *storage.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(ShardBucket)
		if err != nil {
			return err
		}

		var data []byte
		data, err = ch.serialize()
		if err != nil {
			return err
		}

		err = bucket.Put([]byte("consistent_hash"), data)
		if err != nil {
			return err
		}

		return nil
	})
}

func LoadRingHash(db *storage.DB, shards map[string]*Shard) (*RingHash, error) {
	ch := &RingHash{
		HashMap: make(map[uint32]string),
		shards:  shards,
	}

	err := db.View(func(tx *storage.Tx) error {
		bucket, err := tx.GetBucket(ShardBucket)
		if err != nil {
			return ErrConsistentHashNotFound
		}
		data, found := bucket.Get([]byte("consistent_hash"))
		if !found {
			return ErrConsistentHashNotFound
		}
		err = ch.deserialize(data)
		if err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return ch, err
}
