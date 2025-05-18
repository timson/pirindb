package main

import (
	"fmt"
	"sync"
)

type ShardStatus int

const (
	ShardOffline ShardStatus = iota
	ShardInitializing
	ShardSyncing
	ShardOutOfSync
	ShardSyncFailed
	ShardInSync
)

func (s ShardStatus) String() string {
	switch s {
	case ShardOffline:
		return "offline"
	case ShardInitializing:
		return "initializing"
	case ShardSyncing:
		return "syncing"
	case ShardOutOfSync:
		return "out_of_sync"
	case ShardInSync:
		return "in_sync"
	case ShardSyncFailed:
		return "sync_failed"
	}
	return "unknown"
}

type Shard struct {
	Name                string      // Logical name of the shard (must be unique in the cluster)
	Status              ShardStatus // Current operational status of the shard (online, syncing, etc.)
	Host                string      // Host address where the shard is running
	Port                int         // Application port for client connections
	GossipPort          int         // Port used for memberlist gossip communication
	Scheme              string      // scheme http or https
	currentRing         *RingHash   // Full RingHash structure for the local shard (used only locally)
	previousRing        *RingHash   // Previous RingHash structure loaded from persistent storage (used only locally)
	currentHash         *uint64     // Cached hash of the current RingHash (used for broadcasting and validation)
	previousHash        *uint64     // Cached hash of the previous RingHash (used for comparing stored vs. active state)
	lastUpdateTimestamp int64       // indicates the last time the shard's state was updated in unix nanoseconds
	skip                bool
	mu                  sync.RWMutex
}

func NewShard(name string, host string, port int, scheme string, gossipPort int, skip bool) *Shard {
	return &Shard{
		Name:       name,
		Host:       host,
		Port:       port,
		Scheme:     scheme,
		GossipPort: gossipPort,
		skip:       skip,
	}
}

func (s *Shard) URL() string {
	return fmt.Sprintf("%s://%s:%d", s.Scheme, s.Host, s.Port)
}

func (s *Shard) SetCurrentRingHash(ringHash *RingHash) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.currentRing = ringHash
	s.currentHash = &ringHash.hash
}

func (s *Shard) SetPreviousRingHash(ringHash *RingHash) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.previousRing = ringHash
	s.previousHash = &ringHash.hash
}

func (s *Shard) CurrentRingHash() *RingHash {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.currentRing
}

func (s *Shard) PreviousRingHash() *RingHash {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.previousRing
}

func (s *Shard) CurrentHash() *uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.currentHash
}

func (s *Shard) PreviousHash() *uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.previousHash
}

func (s *Shard) SetStatus(status ShardStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Status = status
}

func (s *Shard) GetStatus() ShardStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Status
}

func (s *Shard) SetLastUpdateTimestamp(timestamp int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastUpdateTimestamp = timestamp
}

func (s *Shard) GetLastUpdateTimestamp() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastUpdateTimestamp
}

func (s *Shard) IsSkip() bool {
	return s.skip
}

func (s *Shard) GetName() string {
	return s.Name
}
