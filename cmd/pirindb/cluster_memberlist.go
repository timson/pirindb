package main

import (
	"encoding/json"
	"github.com/hashicorp/memberlist"
	"sync"
	"time"
)

type ShardMetadata struct {
	CurrentRingHash  *uint64     `json:"current_ring_hash"`
	PreviousRingHash *uint64     `json:"previous_ring_hash"`
	Status           ShardStatus `json:"status"`
	Name             string      `json:"name"`
	Timestamp        int64       `json:"timestamp"`
	HLCClock         string      `json:"hlc_clock"`
}

type ClusterDelegate struct {
	meta        ShardMetadata
	mu          sync.RWMutex
	broadcasts  *memberlist.TransmitLimitedQueue
	metaChan    chan ShardMetadata
	offlineChan chan string
}

type ShardMetadataBroadcast struct {
	msg    []byte
	notify chan struct{}
}

func (b *ShardMetadataBroadcast) Invalidates(other memberlist.Broadcast) bool { return false }
func (b *ShardMetadataBroadcast) Message() []byte                             { return b.msg }
func (b *ShardMetadataBroadcast) Finished() {
	if b.notify != nil {
		close(b.notify)
	}
}

func (d *ClusterDelegate) LocalState(join bool) []byte            { return nil }
func (d *ClusterDelegate) MergeRemoteState(buf []byte, join bool) {}
func (d *ClusterDelegate) NodeMeta(limit int) []byte {
	d.mu.RLock()
	defer d.mu.RUnlock()
	data, _ := json.Marshal(d.meta)
	if len(data) > limit {
		return data[:limit]
	}
	return data
}

func (d *ClusterDelegate) SetMetadata(meta ShardMetadata) {
	d.mu.Lock()
	d.meta = meta
	d.mu.Unlock()

	msg, _ := json.Marshal(meta)
	d.broadcasts.QueueBroadcast(&ShardMetadataBroadcast{
		msg: msg,
	})
}

func (d *ClusterDelegate) GetBroadcasts(overhead, limit int) [][]byte {
	return d.broadcasts.GetBroadcasts(overhead, limit)
}

func (d *ClusterDelegate) NotifyMsg(b []byte) {
	meta := ShardMetadata{}
	if err := json.Unmarshal(b, &meta); err != nil {
		return
	}
	d.metaChan <- meta
}

func (d *ClusterDelegate) NotifyJoin(node *memberlist.Node) {
}

func (d *ClusterDelegate) NotifyLeave(node *memberlist.Node) {
	d.offlineChan <- node.Name
}

func (d *ClusterDelegate) NotifyFail(node *memberlist.Node) {
	d.offlineChan <- node.Name
}

func (d *ClusterDelegate) NotifyUpdate(node *memberlist.Node) {
}

func (c *Cluster) applyMetadata(meta ShardMetadata) {
	c.mu.RLock()
	shard, ok := c.shards[meta.Name]
	c.mu.RUnlock()

	c.Logger.Info("apply metadata", "from", meta.Name, "ok", ok, "meta", meta)

	if ok {
		shard.SetStatus(meta.Status)
		shard.mu.Lock()
		shard.currentHash = meta.CurrentRingHash
		shard.previousHash = meta.PreviousRingHash
		shard.lastUpdateTimestamp = meta.Timestamp
		shard.mu.Unlock()

		remoteHLC, err := ParseHLC(meta.HLCClock)
		if err == nil {
			c.Logger.Debug("update HLC clock: ", "hlc_clock", meta.HLCClock, "from", meta.Name)
			c.clock.Update(remoteHLC)
		} else {
			c.Logger.Debug("failed to parse HLC clock: ", "hlc_clock", meta.HLCClock, "from", meta.Name)
		}
	}
}

func (c *Cluster) publishMetadata() {
	localShard := c.LocalShard()
	meta := ShardMetadata{
		Status:    localShard.GetStatus(),
		Name:      localShard.GetName(),
		Timestamp: time.Now().UnixNano(),
		HLCClock:  c.clock.Now().Serialize(),
	}
	if localShard.CurrentRingHash() != nil {
		meta.CurrentRingHash = localShard.CurrentHash()
	}
	if localShard.previousRing != nil {
		meta.PreviousRingHash = localShard.PreviousHash()
	}
	localShard.SetLastUpdateTimestamp(meta.Timestamp)
	c.delegate.SetMetadata(meta)
	c.Logger.Info("published metadata", "shard", localShard.Name, "status", meta.Status, "timestamp", meta.Timestamp)
}
