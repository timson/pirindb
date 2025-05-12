package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/hashicorp/memberlist"
	"github.com/timson/pirindb/storage"
	"log/slog"
	"sync"
	"time"
)

type ClusterHealthStatus int

const (
	ClusterHealthy ClusterHealthStatus = iota
	ClusterUnhealthy
	ClusterOutOfSync
	ClusterSyncing
)

func (s ClusterHealthStatus) String() string {
	switch s {
	case ClusterHealthy:
		return "healthy"
	case ClusterUnhealthy:
		return "unhealthy"
	case ClusterOutOfSync:
		return "out_of_sync"
	case ClusterSyncing:
		return "syncing"
	}
	return "unknown"
}

type ClusterStatus struct {
	Shards map[string]*Shard `json:"shards"`
	Status string            `json:"status"`
}

type Cluster struct {
	Logger         *slog.Logger
	LocalShardName string // Name of its own shard
	memberlist     *memberlist.Memberlist
	delegate       *ClusterDelegate
	shards         map[string]*Shard
	mu             sync.RWMutex
	metaChan       chan ShardMetadata
	offlineChan    chan string
	eventChan      chan struct{}
	clusterStatus  ClusterStatus
	db             *storage.DB
	config         *Config
	clock          *HLClock
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
}

func NewCluster(db *storage.DB, config *Config, logger *slog.Logger, clock *HLClock) (*Cluster, error) {
	shards := map[string]*Shard{}
	knownPeers := make([]string, 0)
	for _, shardConfig := range config.Shards {
		shardConfig.setDefaults()
		shards[shardConfig.Name] = NewShard(shardConfig.Name, shardConfig.Host, shardConfig.Port, shardConfig.Scheme, shardConfig.GossipPort, shardConfig.Skip)
		knownPeers = append(knownPeers, fmt.Sprintf("%s:%d", shardConfig.Host, shardConfig.GossipPort))
	}

	localShard, ok := shards[config.Server.ShardName]
	if !ok {
		return nil, fmt.Errorf("no shards found in config")
	}
	localShard.Status = ShardInitializing

	metaChan := make(chan ShardMetadata, len(shards))
	offlineChan := make(chan string)

	cfg := memberlist.DefaultLANConfig()
	cfg.Name = localShard.Name
	cfg.LogOutput = &slogWriter{logger: logger}
	shardMeta := ShardMetadata{}
	d := &ClusterDelegate{
		meta:        shardMeta,
		metaChan:    metaChan,
		offlineChan: offlineChan,
	}
	cfg.Delegate = d
	cfg.Events = d
	cfg.BindPort = localShard.GossipPort
	cfg.BindAddr = localShard.Host

	if cfg.BindAddr == "" || cfg.BindPort == 0 {
		return nil, fmt.Errorf("failed to find shardConfig config for %s", localShard.Name)
	}

	ml, err := memberlist.Create(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create memberlist: %w", err)
	}
	d.broadcasts = &memberlist.TransmitLimitedQueue{
		NumNodes: func() int {
			return len(ml.Members())
		},
		RetransmitMult: 1,
	}

	if len(knownPeers) > 0 {
		_, err = ml.Join(knownPeers)
		if err != nil {
			logger.Info("warning: failed to join cluster initially", "err", err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	cluster := &Cluster{
		memberlist:     ml,
		Logger:         logger,
		shards:         shards,
		LocalShardName: localShard.Name,
		mu:             sync.RWMutex{},
		delegate:       d,
		metaChan:       metaChan,
		offlineChan:    offlineChan,
		db:             db,
		clock:          clock,
		config:         config,
		ctx:            ctx,
		cancel:         cancel,
	}
	return cluster, nil
}

func (c *Cluster) LocalShard() *Shard {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.shards[c.LocalShardName]
}

func (c *Cluster) initRing() {
	localShard := c.LocalShard()
	previousRing, err := LoadRingHash(c.db, c.shards)

	//if localShard.skip {
	//	if err != nil {
	//		c.Logger.Warn("unable to load previous ring hash, and shard marked for eviction, unable to continue")
	//		panic(err)
	//	}
	//	localShard.SetCurrentRingHash(previousRing)
	//	localShard.SetPreviousRingHash(previousRing)
	//	localShard.SetStatus(ShardMarkedForEviction)
	//	return
	//}

	localShard.SetCurrentRingHash(NewRingHash(c.shards))

	if err != nil {
		if !errors.Is(err, ErrConsistentHashNotFound) {
			c.Logger.Error("failed to load previous consistent hash", "error", err)
			panic(err)
		}
		c.Logger.Info("no previous consistent hash found, checking for existing shards")
		err = SaveRingHash(c.db, localShard.CurrentRingHash())
		if err != nil {
			c.Logger.Error("unable to save consistent hash", "error", err)
		}
		localShard.SetPreviousRingHash(localShard.CurrentRingHash())
		localShard.SetStatus(ShardInSync)
	} else {
		localShard.SetPreviousRingHash(previousRing)
	}

	currentRing := localShard.CurrentRingHash()
	previousRing = localShard.PreviousRingHash()

	if previousRing != nil {
		if !currentRing.Equal(previousRing) {
			localShard.SetStatus(ShardOutOfSync)
			c.Logger.Info("sharding ring out of sync")
		} else {
			localShard.SetStatus(ShardInSync)
			c.Logger.Info("sharding ring in sync")
		}
	}
}

func (c *Cluster) Shutdown() {
	c.Logger.Info("shutting down cluster")
	c.cancel()

	if err := c.memberlist.Shutdown(); err != nil {
		c.Logger.Error("failed to shutdown memberlist", "error", err)
	} else {
		c.Logger.Info("memberlist shutdown successfully")
	}

	c.Logger.Info("waiting for cluster loop to finish")
	c.wg.Wait()
	c.Logger.Info("cluster shutdown complete")
}

func (c *Cluster) Start() {
	c.initRing()
	c.Logger.Info("starting cluster")
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.loop()
	}()
}

func (c *Cluster) loop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.publishMetadata()
		case meta := <-c.metaChan:
			c.applyMetadata(meta)
		case offlineShardName := <-c.offlineChan:
			c.mu.Lock()
			if shard, ok := c.shards[offlineShardName]; ok {
				c.Logger.Info("shard offline", "shard", shard.Name)
				shard.SetStatus(ShardOffline)
			}
			c.mu.Unlock()
		case <-c.ctx.Done():
			c.Logger.Info("cluster loop stopped")
			return
		}
	}
}

func (c *Cluster) Status() *ClusterStatus {
	c.mu.Lock()
	defer c.mu.Unlock()

	status := ClusterHealthy
	for _, shard := range c.shards {
		if shard.Status == ShardOutOfSync || shard.Status == ShardSyncing {
			status = ClusterOutOfSync
		}
		if shard.Status == ShardOffline {
			status = ClusterUnhealthy
			break
		}
	}

	c.clusterStatus.Shards = c.shards
	c.clusterStatus.Status = status.String()
	return &c.clusterStatus
}

func (c *Cluster) RunResharding() {
	shard := c.LocalShard()
	config := c.config.Resharding
	status := shard.GetStatus()
	if status == ShardOutOfSync {
		shard.SetStatus(ShardSyncing)
		c.Logger.Info("resharding started")
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			c.resharding(c.ctx, config.BatchSize, config.Retries, config.GetRetryTimeout(), config.GetBatchDelay())
		}()
	} else {
		c.Logger.Info("resharding skipped, shard is not out of sync")
	}
}

func (c *Cluster) readBatch(db *storage.DB, startKey []byte, limit int) ([]storage.Item, error) {
	var items []storage.Item
	err := db.View(func(tx *storage.Tx) error {
		bucket, err := tx.GetBucket(DBBucket)
		if err != nil {
			return err
		}
		cursor := bucket.Cursor()
		k, v := cursor.First()
		if startKey != nil {
			k, v = cursor.Seek(startKey)
			if k != nil && bytes.Equal(k, startKey) {
				k, v = cursor.Next()
			}
		}
		for count := 0; k != nil && count < limit; k, v = cursor.Next() {
			items = append(items, storage.Item{
				Key:   append([]byte{}, k...),
				Value: append([]byte{}, v...),
			})
			count++
		}
		return nil
	})
	return items, err
}

func (c *Cluster) reshardBatch(items []storage.Item, ring *RingHash, localShardName string, retries int, retryTimeout time.Duration) ([]storage.Item, error) {
	var toDelete []storage.Item
	client := newHTTPClient(c.config)
	for _, item := range items {
		key := string(item.Key)
		val := string(item.Value)
		destShard := ring.GetShard(key)
		if destShard == nil || destShard.GetName() == localShardName {
			continue
		}

		var err error
		retries = 3
		for attempt := 0; attempt <= retries; attempt++ {
			err = RemotePut(client, destShard.URL(), key, val, true, c.clock)
			if err == nil {
				break
			}
			time.Sleep(retryTimeout)
		}

		if err != nil {
			c.Logger.Error("reshard failed", "key", key, "dest", destShard.GetName(), "error", err)
			continue
		}
		c.Logger.Debug("resharded key", "key", key, "from", localShardName, "to", destShard.GetName())
		toDelete = append(toDelete, item)
	}
	return toDelete, nil
}

func (c *Cluster) deleteKeys(db *storage.DB, items []storage.Item) error {
	return db.Update(func(tx *storage.Tx) error {
		bucket, err := tx.GetBucket(DBBucket)
		if err != nil {
			return err
		}
		for _, item := range items {
			if err := bucket.Remove(item.Key); err != nil {
				c.Logger.Error("key deletion failed", "key", string(item.Key), "error", err)
			}
		}
		return nil
	})
}

func (c *Cluster) resharding(ctx context.Context, batchSize int, retries int, retryTimeout time.Duration, batchDelay time.Duration) {
	var lastKey []byte
	db := c.db
	localShard := c.LocalShard()
	localShardName := localShard.GetName()
	currentRing := localShard.CurrentRingHash()
	fail := false

	for {
		select {
		case <-ctx.Done():
			c.Logger.Info("resharding canceled")
			return
		default:
		}

		keys, err := c.readBatch(db, lastKey, batchSize)
		if err != nil {
			c.Logger.Error("failed to read batch from DB", "error", err, "last_key_tried", string(lastKey))
			time.Sleep(retryTimeout)
			retries--
			if retries < 0 {
				c.Logger.Error("failed to read batch from DB, giving up")
				fail = true
				break
			}
			continue
		}

		if len(keys) == 0 {
			break
		}
		lastKey = keys[len(keys)-1].Key

		toDelete, _ := c.reshardBatch(keys, currentRing, localShardName, retries, retryTimeout)

		if len(toDelete) > 0 {
			if errDel := c.deleteKeys(db, toDelete); errDel != nil {
				c.Logger.Error("failed to delete resharded keys from local DB", "error", errDel)
				fail = true
				break
			}
		}
		time.Sleep(batchDelay)
	}

	if fail {
		c.Logger.Error("resharding failed, marking shard as sync_failed")
		localShard.SetStatus(ShardSyncFailed)
		return
	}

	c.Logger.Info("resharding data movement finished")

	if !localShard.skip {
		localShard.SetPreviousRingHash(currentRing)
		if err := SaveRingHash(db, currentRing); err != nil {
			c.Logger.Error("failed to persist consistent hash", "error", err)
			// TODO: decide what to do here
		}
	}
	localShard.SetStatus(ShardInSync)
	c.Logger.Info("resharding process completed successfully")
}
