package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/timson/pirindb/storage"
)

var redisFlushGenerationSequenceKey = []byte("flush_generation")

type redisFlushBucketSpec struct {
	name                []byte
	generationDirectory bool
}

func redisFlushBucketSpecs(ns redisNamespace) []redisFlushBucketSpec {
	return []redisFlushBucketSpec{
		{name: ns.keyMetaBucket, generationDirectory: true},
		{name: ns.stringBucket},
		{name: ns.listMetaBucket},
		{name: ns.hashMetaBucket},
		{name: ns.bloomMetaBucket},
		{name: ns.topkMetaBucket},
		{name: ns.zsetMetaBucket},
		{name: ns.listDataBucket},
		{name: ns.hashDataBucket},
		{name: ns.bloomDataBucket},
		{name: ns.topkDataBucket},
		{name: ns.zsetMemberBucket},
		{name: ns.zsetScoreBucket},
		{name: ns.slotIndexBucket},
		{name: ns.expireMetaBucket},
		{name: ns.expireIndexBucket},
		{name: ns.buildStateBucket},
	}
}

func nextRedisFlushGenerationTx(tx *storage.Tx, ns redisNamespace) (uint64, error) {
	bucket, err := tx.CreateBucketIfNotExists(ns.gcSysBucket)
	if err != nil {
		return 0, err
	}
	raw, found := bucket.Get(redisFlushGenerationSequenceKey)
	generation := uint64(1)
	if found {
		if len(raw) != 8 {
			return 0, errors.New("corrupted redis flush generation")
		}
		generation = binary.BigEndian.Uint64(raw)
		if generation == ^uint64(0) {
			return 0, errors.New("redis flush generation overflow")
		}
		generation++
	}
	encoded := make([]byte, 8)
	binary.BigEndian.PutUint64(encoded, generation)
	if err = bucket.Put(redisFlushGenerationSequenceKey, encoded); err != nil {
		return 0, err
	}
	return generation, nil
}

func redisFlushedBucketName(ns redisNamespace, generation uint64, index int) []byte {
	return []byte(fmt.Sprintf("__redis_db_%d_gc_generation_%020d_%02d__", ns.dbIndex, generation, index))
}

func flushRedisDBGenerationTx(tx *storage.Tx, ns redisNamespace, nowMs int64) (int, error) {
	generation, err := nextRedisFlushGenerationTx(tx, ns)
	if err != nil {
		return 0, err
	}
	queued := 0
	for index, spec := range redisFlushBucketSpecs(ns) {
		if _, getErr := tx.GetBucket(spec.name); errors.Is(getErr, storage.ErrBucketNotFound) {
			continue
		} else if getErr != nil {
			return queued, getErr
		}
		hiddenName := redisFlushedBucketName(ns, generation, index)
		if _, err = tx.MoveBucket(spec.name, hiddenName); err != nil {
			return queued, err
		}
		if spec.generationDirectory {
			_, err = enqueueRedisGenerationDirectoryGCTx(tx, ns, hiddenName, nowMs)
		} else {
			_, err = enqueueRedisPrivateBucketGCTx(tx, ns, hiddenName, nowMs)
		}
		if err != nil {
			return queued, err
		}
		queued++
	}
	if err = saveRedisKeyDirectoryStateTx(tx, ns, redisKeyDirectoryState{Phase: redisKeyDirectoryPhaseActive, BucketIndex: byte(len(redisLegacyKeySources))}); err != nil {
		return queued, err
	}
	if err = saveRedisObjectMigrationStateTx(tx, ns, redisObjectMigrationState{Phase: redisObjectMigrationPhaseScanning}); err != nil {
		return queued, err
	}
	return queued, nil
}

func (srv *RedisServer) flushRedisDBGeneration(dbIndex int, nowMs int64) error {
	return srv.DB.Update(func(tx *storage.Tx) error {
		_, err := flushRedisDBGenerationTx(tx, srv.redisNamespace(dbIndex), nowMs)
		return err
	})
}

func (srv *RedisServer) flushAllRedisGenerations(nowMs int64) error {
	return srv.DB.Update(func(tx *storage.Tx) error {
		for dbIndex := redisDatabaseMin; dbIndex <= redisDatabaseMax; dbIndex++ {
			if _, err := flushRedisDBGenerationTx(tx, srv.redisNamespace(dbIndex), nowMs); err != nil {
				return err
			}
		}
		return nil
	})
}

func (srv *RedisServer) redisGCQueuesEmpty(dbIndexes []int) (bool, error) {
	empty := true
	err := srv.DB.View(func(tx *storage.Tx) error {
		for _, dbIndex := range dbIndexes {
			bucket, err := tx.GetBucket(srv.redisNamespace(dbIndex).gcQueueBucket)
			if errors.Is(err, storage.ErrBucketNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			if bucket.ItemCount() > 0 {
				empty = false
				return nil
			}
		}
		return nil
	})
	return empty, err
}

func (srv *RedisServer) waitForRedisGC(dbIndexes []int) error {
	settings := redisGCSettingsForConfig(srv.Config)
	if settings.batchSize <= 0 {
		settings.batchSize = 128
	}
	if settings.batchBytes <= 0 {
		settings.batchBytes = 4 * 1024 * 1024
	}
	if settings.maxBatchesPerCycle <= 0 {
		settings.maxBatchesPerCycle = 32
	}
	for {
		empty, err := srv.redisGCQueuesEmpty(dbIndexes)
		if err != nil || empty {
			return err
		}
		select {
		case <-srv.stopCh:
			return errors.New("redis server stopped while waiting for garbage collection")
		default:
		}
		if err = srv.runRedisGCCycle(srv.nowUnixMilli(), settings); err != nil {
			return err
		}
		time.Sleep(time.Millisecond)
	}
}
