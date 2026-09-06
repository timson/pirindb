package main

import (
	"fmt"
	"time"

	"github.com/timson/pirindb/storage"
)

type redisExpiryWorkerSettings struct {
	interval           time.Duration
	batchSize          int
	maxBatchesPerCycle int
}

type redisExpiryWorkerStats struct {
	NextDB              int
	Cycles              uint64
	Batches             uint64
	EntriesExamined     uint64
	KeysRemoved         uint64
	StaleEntriesRemoved uint64
	LastCycleUnixMs     int64
	LastError           string
}

func redisExpirySettings(cfg *Config) redisExpiryWorkerSettings {
	if cfg == nil || cfg.Redis == nil || cfg.Redis.ExpirySweepIntervalMs <= 0 || cfg.Redis.ExpirySweepBatchSize <= 0 || cfg.Redis.ExpirySweepMaxBatchesPerCycle <= 0 {
		return redisExpiryWorkerSettings{}
	}
	return redisExpiryWorkerSettings{
		interval:           time.Duration(cfg.Redis.ExpirySweepIntervalMs) * time.Millisecond,
		batchSize:          cfg.Redis.ExpirySweepBatchSize,
		maxBatchesPerCycle: cfg.Redis.ExpirySweepMaxBatchesPerCycle,
	}
}

func (srv *RedisServer) redisExpiryWorkerEnabled() bool {
	settings := redisExpirySettings(srv.Config)
	return settings.interval > 0 && settings.batchSize > 0 && settings.maxBatchesPerCycle > 0
}

func (srv *RedisServer) wakeExpiryWorker() {
	if srv == nil || !srv.redisExpiryWorkerEnabled() {
		return
	}
	select {
	case srv.expiryWake <- struct{}{}:
	default:
	}
}

func (srv *RedisServer) expiryWorkerStats() redisExpiryWorkerStats {
	if srv == nil {
		return redisExpiryWorkerStats{}
	}
	srv.expiryMu.RLock()
	defer srv.expiryMu.RUnlock()
	return srv.expiryStats
}

func (srv *RedisServer) expiryLoop() {
	defer srv.wg.Done()
	settings := redisExpirySettings(srv.Config)
	ticker := time.NewTicker(settings.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
		case <-srv.expiryWake:
		case <-srv.stopCh:
			return
		}

		if err := srv.runExpirySweepCycle(srv.nowUnixMilli(), settings); err != nil {
			srv.expiryMu.Lock()
			srv.expiryStats.LastError = err.Error()
			srv.expiryStats.LastCycleUnixMs = srv.nowUnixMilli()
			srv.expiryStats.Cycles++
			srv.expiryMu.Unlock()
			srv.Logger.Error("Redis expiry sweep failed", "error", err)
		}
	}
}

func (srv *RedisServer) redisExpiryDue(dbIndex int, nowMs int64) (bool, error) {
	ns := srv.redisNamespace(dbIndex)
	var due bool
	err := srv.DB.View(func(tx *storage.Tx) error {
		var innerErr error
		due, innerErr = redisExpiryDueTx(tx, ns, nowMs)
		return innerErr
	})
	return due, err
}

func (srv *RedisServer) sweepRedisExpiryBatch(dbIndex int, nowMs int64, limit int) (redisExpirySweepResult, error) {
	var result redisExpirySweepResult
	ns := srv.redisNamespace(dbIndex)
	err := srv.DB.Update(func(tx *storage.Tx) error {
		var innerErr error
		result, innerErr = sweepExpiredRedisKeysBatchTx(tx, ns, nowMs, limit)
		return innerErr
	})
	return result, err
}

func (srv *RedisServer) runExpirySweepCycle(nowMs int64, settings redisExpiryWorkerSettings) error {
	if settings.batchSize <= 0 || settings.maxBatchesPerCycle <= 0 {
		return nil
	}

	srv.expiryMu.RLock()
	nextDB := srv.expiryStats.NextDB
	srv.expiryMu.RUnlock()
	if nextDB < redisDatabaseMin || nextDB > redisDatabaseMax {
		nextDB = redisDatabaseMin
	}

	consecutiveIdleDBs := 0
	batches := 0
	var aggregate redisExpirySweepResult
	for batches < settings.maxBatchesPerCycle && consecutiveIdleDBs < redisDatabaseCount {
		select {
		case <-srv.stopCh:
			return nil
		default:
		}

		dbIndex := nextDB
		nextDB++
		if nextDB > redisDatabaseMax {
			nextDB = redisDatabaseMin
		}

		due, err := srv.redisExpiryDue(dbIndex, nowMs)
		if err != nil {
			return fmt.Errorf("check redis db %d expiry backlog: %w", dbIndex, err)
		}
		if !due {
			consecutiveIdleDBs++
			continue
		}

		result, err := srv.sweepRedisExpiryBatch(dbIndex, nowMs, settings.batchSize)
		if err != nil {
			return fmt.Errorf("sweep redis db %d expiry backlog: %w", dbIndex, err)
		}
		batches++
		aggregate.EntriesExamined += result.EntriesExamined
		aggregate.KeysRemoved += result.KeysRemoved
		aggregate.StaleRemoved += result.StaleRemoved
		if result.EntriesExamined == 0 && !result.MoreDue {
			consecutiveIdleDBs++
		} else {
			consecutiveIdleDBs = 0
		}
	}

	srv.expiryMu.Lock()
	srv.expiryStats.NextDB = nextDB
	srv.expiryStats.Cycles++
	srv.expiryStats.Batches += uint64(batches)
	srv.expiryStats.EntriesExamined += uint64(aggregate.EntriesExamined)
	srv.expiryStats.KeysRemoved += uint64(aggregate.KeysRemoved)
	srv.expiryStats.StaleEntriesRemoved += uint64(aggregate.StaleRemoved)
	srv.expiryStats.LastCycleUnixMs = nowMs
	srv.expiryStats.LastError = ""
	srv.expiryMu.Unlock()
	return nil
}
