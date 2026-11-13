package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/timson/pirindb/storage"
)

const (
	redisGCTaskSchemaVersion = 1
	redisGCTaskPrivateBucket = 1
	redisGCTaskObjectPrefix  = 2
	redisGCTaskExactKey      = 3
	redisGCTaskGenerationDir = 4
	redisGCTaskListChain     = 5
	redisGCTaskHeaderSize    = 53
)

type redisGCTask struct {
	Kind        byte
	BucketName  []byte
	Prefix      []byte
	Cursor      []byte
	RecordsDone uint64
	BytesDone   uint64
	CreatedAtMs int64
	UpdatedAtMs int64
	Retries     uint32
	Aux         uint64
}

type redisGCSettings struct {
	interval           time.Duration
	batchSize          int
	batchBytes         int64
	maxBatchesPerCycle int
}

type redisGCWorkerStats struct {
	NextDB           int
	Cycles           uint64
	Batches          uint64
	TasksCompleted   uint64
	RecordsReclaimed uint64
	BytesReclaimed   uint64
	LastCycleUnixMs  int64
	LastError        string
}

func redisGCSettingsForConfig(cfg *Config) redisGCSettings {
	if cfg == nil || cfg.Redis == nil || cfg.Redis.GCSweepIntervalMs <= 0 || cfg.Redis.GCSweepBatchSize <= 0 || cfg.Redis.GCSweepBatchBytes <= 0 || cfg.Redis.GCMaxBatchesPerCycle <= 0 {
		return redisGCSettings{}
	}
	return redisGCSettings{
		interval:           time.Duration(cfg.Redis.GCSweepIntervalMs) * time.Millisecond,
		batchSize:          cfg.Redis.GCSweepBatchSize,
		batchBytes:         cfg.Redis.GCSweepBatchBytes,
		maxBatchesPerCycle: cfg.Redis.GCMaxBatchesPerCycle,
	}
}

func serializeRedisGCTask(task redisGCTask) ([]byte, error) {
	if task.Kind != redisGCTaskPrivateBucket && task.Kind != redisGCTaskObjectPrefix && task.Kind != redisGCTaskExactKey && task.Kind != redisGCTaskGenerationDir && task.Kind != redisGCTaskListChain {
		return nil, errors.New("invalid redis garbage-collection task kind")
	}
	if len(task.BucketName) == 0 || len(task.BucketName) > 0xffff || len(task.Prefix) > 0xffff || len(task.Cursor) > 0xffff {
		return nil, errors.New("redis garbage-collection task field is too large")
	}
	buf := make([]byte, redisGCTaskHeaderSize+len(task.BucketName)+len(task.Prefix)+len(task.Cursor))
	buf[0] = redisGCTaskSchemaVersion
	buf[1] = task.Kind
	binary.BigEndian.PutUint16(buf[2:4], uint16(len(task.BucketName)))
	binary.BigEndian.PutUint16(buf[4:6], uint16(len(task.Prefix)))
	binary.BigEndian.PutUint16(buf[6:8], uint16(len(task.Cursor)))
	binary.BigEndian.PutUint64(buf[8:16], task.RecordsDone)
	binary.BigEndian.PutUint64(buf[16:24], task.BytesDone)
	binary.BigEndian.PutUint64(buf[24:32], uint64(task.CreatedAtMs))
	binary.BigEndian.PutUint64(buf[32:40], uint64(task.UpdatedAtMs))
	binary.BigEndian.PutUint32(buf[40:44], task.Retries)
	binary.BigEndian.PutUint64(buf[44:52], task.Aux)
	offset := redisGCTaskHeaderSize
	copy(buf[offset:], task.BucketName)
	offset += len(task.BucketName)
	copy(buf[offset:], task.Prefix)
	offset += len(task.Prefix)
	copy(buf[offset:], task.Cursor)
	return buf, nil
}

func deserializeRedisGCTask(buf []byte) (redisGCTask, error) {
	if len(buf) < redisGCTaskHeaderSize || buf[0] != redisGCTaskSchemaVersion {
		return redisGCTask{}, errors.New("corrupted redis garbage-collection task")
	}
	bucketLen := int(binary.BigEndian.Uint16(buf[2:4]))
	prefixLen := int(binary.BigEndian.Uint16(buf[4:6]))
	cursorLen := int(binary.BigEndian.Uint16(buf[6:8]))
	if redisGCTaskHeaderSize+bucketLen+prefixLen+cursorLen != len(buf) {
		return redisGCTask{}, errors.New("corrupted redis garbage-collection task length")
	}
	task := redisGCTask{
		Kind:        buf[1],
		RecordsDone: binary.BigEndian.Uint64(buf[8:16]),
		BytesDone:   binary.BigEndian.Uint64(buf[16:24]),
		CreatedAtMs: int64(binary.BigEndian.Uint64(buf[24:32])),
		UpdatedAtMs: int64(binary.BigEndian.Uint64(buf[32:40])),
		Retries:     binary.BigEndian.Uint32(buf[40:44]),
		Aux:         binary.BigEndian.Uint64(buf[44:52]),
	}
	if task.Kind != redisGCTaskPrivateBucket && task.Kind != redisGCTaskObjectPrefix && task.Kind != redisGCTaskExactKey && task.Kind != redisGCTaskGenerationDir && task.Kind != redisGCTaskListChain {
		return redisGCTask{}, errors.New("corrupted redis garbage-collection task kind")
	}
	offset := redisGCTaskHeaderSize
	task.BucketName = cloneBytes(buf[offset : offset+bucketLen])
	offset += bucketLen
	task.Prefix = cloneBytes(buf[offset : offset+prefixLen])
	offset += prefixLen
	task.Cursor = cloneBytes(buf[offset : offset+cursorLen])
	switch task.Kind {
	case redisGCTaskPrivateBucket, redisGCTaskGenerationDir:
		if len(task.Prefix) != 0 {
			return redisGCTask{}, errors.New("corrupted redis garbage-collection bucket task prefix")
		}
	case redisGCTaskObjectPrefix:
		if len(task.Prefix) != 8 {
			return redisGCTask{}, errors.New("corrupted redis garbage-collection object prefix")
		}
	case redisGCTaskListChain:
		if len(task.Cursor) != 8 || (len(task.Prefix) != 0 && len(task.Prefix) != 8) {
			return redisGCTask{}, errors.New("corrupted redis list-chain garbage task")
		}
	}
	return task, nil
}

func enqueueRedisGCTaskTx(tx *storage.Tx, ns redisNamespace, task redisGCTask) (uint64, error) {
	encoded, err := serializeRedisGCTask(task)
	if err != nil {
		return 0, err
	}
	sys, err := tx.CreateBucketIfNotExists(ns.gcSysBucket)
	if err != nil {
		return 0, err
	}
	id, err := sys.NextSequence()
	if err != nil {
		return 0, err
	}
	queue, err := tx.CreateBucketIfNotExists(ns.gcQueueBucket)
	if err != nil {
		return 0, err
	}
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, id)
	return id, queue.Put(key, encoded)
}

func enqueueRedisPrivateBucketGCTx(tx *storage.Tx, ns redisNamespace, bucketName []byte, nowMs int64) (uint64, error) {
	return enqueueRedisGCTaskTx(tx, ns, redisGCTask{Kind: redisGCTaskPrivateBucket, BucketName: cloneBytes(bucketName), CreatedAtMs: nowMs, UpdatedAtMs: nowMs})
}

func enqueueRedisObjectPrefixGCTx(tx *storage.Tx, ns redisNamespace, bucketName []byte, objectID uint64, nowMs int64) (uint64, error) {
	return enqueueRedisGCTaskTx(tx, ns, redisGCTask{Kind: redisGCTaskObjectPrefix, BucketName: cloneBytes(bucketName), Prefix: redisObjectPrefix(objectID), CreatedAtMs: nowMs, UpdatedAtMs: nowMs})
}

func enqueueRedisExactKeyGCTx(tx *storage.Tx, ns redisNamespace, bucketName, key []byte, nowMs int64) (uint64, error) {
	return enqueueRedisGCTaskTx(tx, ns, redisGCTask{Kind: redisGCTaskExactKey, BucketName: cloneBytes(bucketName), Prefix: cloneBytes(key), CreatedAtMs: nowMs, UpdatedAtMs: nowMs})
}

func enqueueRedisGenerationDirectoryGCTx(tx *storage.Tx, ns redisNamespace, bucketName []byte, nowMs int64) (uint64, error) {
	return enqueueRedisGCTaskTx(tx, ns, redisGCTask{Kind: redisGCTaskGenerationDir, BucketName: cloneBytes(bucketName), CreatedAtMs: nowMs, UpdatedAtMs: nowMs})
}

func enqueueRedisListChainGCTx(tx *storage.Tx, ns redisNamespace, meta *redisListMeta, startID, stopBeforeID uint64, nowMs int64) (uint64, error) {
	if startID == 0 || startID == stopBeforeID {
		return 0, nil
	}
	bucketName := redisListBucketName(ns, meta.ID)
	var prefix []byte
	if meta.StorageFormat == redisKeyStorageShared {
		bucketName = ns.listDataBucket
		prefix = redisObjectPrefix(meta.ID)
	}
	cursor := make([]byte, 8)
	binary.BigEndian.PutUint64(cursor, startID)
	return enqueueRedisGCTaskTx(tx, ns, redisGCTask{
		Kind:        redisGCTaskListChain,
		BucketName:  cloneBytes(bucketName),
		Prefix:      prefix,
		Cursor:      cursor,
		Aux:         stopBeforeID,
		CreatedAtMs: nowMs,
		UpdatedAtMs: nowMs,
	})
}

type redisGCBatchResult struct {
	HadTask   bool
	Completed bool
	Records   int
	Bytes     int64
}

func processRedisGCBatchTx(tx *storage.Tx, ns redisNamespace, nowMs int64, recordLimit int, byteLimit int64) (redisGCBatchResult, error) {
	if recordLimit <= 0 || byteLimit <= 0 {
		return redisGCBatchResult{}, nil
	}
	queue, err := tx.GetBucket(ns.gcQueueBucket)
	if errors.Is(err, storage.ErrBucketNotFound) {
		return redisGCBatchResult{}, nil
	}
	if err != nil {
		return redisGCBatchResult{}, err
	}
	taskKey, rawTask := queue.Cursor().First()
	if taskKey == nil {
		return redisGCBatchResult{}, nil
	}
	task, err := deserializeRedisGCTask(rawTask)
	if err != nil {
		return redisGCBatchResult{}, err
	}
	result := redisGCBatchResult{HadTask: true}
	bucket, err := tx.GetBucket(task.BucketName)
	if errors.Is(err, storage.ErrBucketNotFound) {
		if removeErr := queue.Remove(taskKey); removeErr != nil {
			return result, removeErr
		}
		result.Completed = true
		return result, nil
	}
	if err != nil {
		return result, err
	}
	if task.Kind == redisGCTaskListChain {
		return processRedisListChainGCBatchTx(queue, taskKey, bucket, task, nowMs, recordLimit, byteLimit)
	}

	cursor := bucket.Cursor()
	var item *storage.Item
	if task.Kind == redisGCTaskExactKey {
		meta, reused, metaErr := loadRedisKeyMetaTx(tx, ns, task.Prefix)
		if metaErr != nil {
			return result, metaErr
		} else if reused && meta.Type == redisKeyMetaTypeString {
			// A recreated string occupies the same physical key, so deleting it
			// would remove the new value. A key recreated as another type does not
			// own the stale string record and that record is still safe to reclaim.
			if removeErr := queue.Remove(taskKey); removeErr != nil {
				return result, removeErr
			}
			result.Completed = true
			return result, nil
		}
		item = cursor.SeekItem(task.Prefix)
		if item == nil || !bytes.Equal(item.Key, task.Prefix) {
			if removeErr := queue.Remove(taskKey); removeErr != nil {
				return result, removeErr
			}
			result.Completed = true
			return result, nil
		}
	} else if len(task.Cursor) == 0 {
		if len(task.Prefix) == 0 {
			item = cursor.FirstItem()
		} else {
			item = cursor.SeekItem(task.Prefix)
		}
	} else {
		item = cursor.SeekItem(nextScanSeekKey(task.Cursor))
	}
	keys := make([][]byte, 0, recordLimit)
	var estimatedBytes int64
	for item != nil && len(keys) < recordLimit {
		if task.Kind == redisGCTaskExactKey && !bytes.Equal(item.Key, task.Prefix) {
			item = nil
			break
		}
		if task.Kind == redisGCTaskObjectPrefix && !bytes.HasPrefix(item.Key, task.Prefix) {
			item = nil
			break
		}
		valueLen, found, valueErr := bucket.ValueLen(item.Key)
		if valueErr != nil {
			return result, valueErr
		}
		if !found {
			return result, errors.New("redis garbage record disappeared during scan")
		}
		recordBytes := int64(len(item.Key) + valueLen)
		if len(keys) > 0 && estimatedBytes+recordBytes > byteLimit {
			break
		}
		keys = append(keys, cloneBytes(item.Key))
		estimatedBytes += recordBytes
		if task.Kind == redisGCTaskExactKey {
			item = nil
			break
		}
		item = cursor.NextItem()
	}
	if err = cursor.Err(); err != nil {
		return result, err
	}
	more := item != nil && (task.Kind == redisGCTaskPrivateBucket || task.Kind == redisGCTaskGenerationDir ||
		(task.Kind == redisGCTaskObjectPrefix && bytes.HasPrefix(item.Key, task.Prefix)))
	for _, key := range keys {
		if task.Kind == redisGCTaskGenerationDir {
			raw, found := bucket.Get(key)
			if !found {
				return result, errors.New("redis flushed key metadata disappeared during garbage collection")
			}
			meta, metaErr := deserializeRedisKeyMeta(raw)
			if metaErr != nil {
				return result, metaErr
			}
			if meta.StorageFormat == redisKeyStorageLegacy && meta.Type != redisKeyMetaTypeString {
				descriptors, descriptorErr := redisMigrationBuckets(ns, meta.Type, meta.ObjectID)
				if descriptorErr != nil {
					return result, descriptorErr
				}
				for _, descriptor := range descriptors {
					if _, descriptorErr = enqueueRedisPrivateBucketGCTx(tx, ns, descriptor.legacy, nowMs); descriptorErr != nil {
						return result, descriptorErr
					}
				}
			}
		}
		if err = bucket.Remove(key); err != nil && !errors.Is(err, storage.ErrNodeNotFound) {
			return result, err
		}
	}
	result.Records = len(keys)
	result.Bytes = estimatedBytes
	task.RecordsDone += uint64(len(keys))
	task.BytesDone += uint64(estimatedBytes)
	task.UpdatedAtMs = nowMs
	if len(keys) > 0 {
		task.Cursor = cloneBytes(keys[len(keys)-1])
	}

	if !more {
		if task.Kind == redisGCTaskPrivateBucket || task.Kind == redisGCTaskGenerationDir {
			if err = tx.DeleteBucket(task.BucketName); err != nil && !errors.Is(err, storage.ErrBucketNotFound) {
				return result, err
			}
		}
		if err = queue.Remove(taskKey); err != nil && !errors.Is(err, storage.ErrNodeNotFound) {
			return result, err
		}
		result.Completed = true
		return result, nil
	}
	encoded, err := serializeRedisGCTask(task)
	if err != nil {
		return result, err
	}
	if err = queue.Put(taskKey, encoded); err != nil {
		return result, err
	}
	return result, nil
}

func processRedisListChainGCBatchTx(queue *storage.Bucket, taskKey []byte, bucket *storage.Bucket, task redisGCTask, nowMs int64, recordLimit int, byteLimit int64) (redisGCBatchResult, error) {
	result := redisGCBatchResult{HadTask: true}
	if len(task.Cursor) != 8 {
		return result, errors.New("corrupted redis list-chain garbage cursor")
	}
	segmentID := binary.BigEndian.Uint64(task.Cursor)
	for segmentID != 0 && segmentID != task.Aux && result.Records < recordLimit {
		entryKey := make([]byte, len(task.Prefix)+8)
		copy(entryKey, task.Prefix)
		binary.BigEndian.PutUint64(entryKey[len(task.Prefix):], segmentID)
		valueLen, found, err := bucket.ValueLen(entryKey)
		if err != nil {
			return result, err
		}
		if !found {
			segmentID = 0
			break
		}
		recordBytes := int64(len(entryKey) + valueLen)
		if result.Records > 0 && result.Bytes+recordBytes > byteLimit {
			break
		}
		raw, found := bucket.Get(entryKey)
		if !found {
			return result, errors.New("redis list-chain garbage segment disappeared")
		}
		segment, err := deserializeRedisListSegment(raw)
		if err != nil {
			return result, err
		}
		nextID := segment.NextID
		if err = bucket.Remove(entryKey); err != nil && !errors.Is(err, storage.ErrNodeNotFound) {
			return result, err
		}
		result.Records++
		result.Bytes += recordBytes
		segmentID = nextID
	}
	task.RecordsDone += uint64(result.Records)
	task.BytesDone += uint64(result.Bytes)
	task.UpdatedAtMs = nowMs
	if segmentID == 0 || segmentID == task.Aux {
		if err := queue.Remove(taskKey); err != nil && !errors.Is(err, storage.ErrNodeNotFound) {
			return result, err
		}
		result.Completed = true
		return result, nil
	}
	task.Cursor = make([]byte, 8)
	binary.BigEndian.PutUint64(task.Cursor, segmentID)
	encoded, err := serializeRedisGCTask(task)
	if err != nil {
		return result, err
	}
	if err = queue.Put(taskKey, encoded); err != nil {
		return result, err
	}
	return result, nil
}

func (srv *RedisServer) redisGCWorkerEnabled() bool {
	settings := redisGCSettingsForConfig(srv.Config)
	return settings.interval > 0 && settings.batchSize > 0 && settings.batchBytes > 0 && settings.maxBatchesPerCycle > 0
}

func (srv *RedisServer) gcWorkerStats() redisGCWorkerStats {
	srv.gcMu.RLock()
	defer srv.gcMu.RUnlock()
	return srv.gcStats
}

func (srv *RedisServer) gcLoop() {
	defer srv.wg.Done()
	settings := redisGCSettingsForConfig(srv.Config)
	ticker := time.NewTicker(settings.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
		case <-srv.gcWake:
		case <-srv.stopCh:
			return
		}
		if err := srv.runRedisGCCycle(srv.nowUnixMilli(), settings); err != nil {
			srv.gcMu.Lock()
			srv.gcStats.LastError = err.Error()
			srv.gcStats.LastCycleUnixMs = srv.nowUnixMilli()
			srv.gcStats.Cycles++
			srv.gcMu.Unlock()
			if srv.Logger != nil {
				srv.Logger.Error("Redis garbage collection failed", "error", err)
			}
		}
	}
}

func (srv *RedisServer) wakeGCWorker() {
	if srv == nil || !srv.redisGCWorkerEnabled() {
		return
	}
	select {
	case srv.gcWake <- struct{}{}:
	default:
	}
}

func (srv *RedisServer) runRedisGCCycle(nowMs int64, settings redisGCSettings) error {
	if settings.batchSize <= 0 || settings.batchBytes <= 0 || settings.maxBatchesPerCycle <= 0 {
		return nil
	}
	srv.gcMu.RLock()
	nextDB := srv.gcStats.NextDB
	srv.gcMu.RUnlock()
	if nextDB < redisDatabaseMin || nextDB > redisDatabaseMax {
		nextDB = redisDatabaseMin
	}
	idle := 0
	batches := 0
	var records, reclaimedBytes, completed uint64
	for batches < settings.maxBatchesPerCycle && idle < redisDatabaseCount {
		select {
		case <-srv.stopCh:
			return nil
		default:
		}
		dbIndex := nextDB
		nextDB = (nextDB + 1) % redisDatabaseCount
		var result redisGCBatchResult
		err := srv.DB.Update(func(tx *storage.Tx) error {
			var processErr error
			result, processErr = processRedisGCBatchTx(tx, srv.redisNamespace(dbIndex), nowMs, settings.batchSize, settings.batchBytes)
			return processErr
		})
		if err != nil {
			return fmt.Errorf("collect redis db %d garbage: %w", dbIndex, err)
		}
		if !result.HadTask {
			idle++
			continue
		}
		idle = 0
		batches++
		records += uint64(result.Records)
		reclaimedBytes += uint64(result.Bytes)
		if result.Completed {
			completed++
		}
	}
	srv.gcMu.Lock()
	srv.gcStats.NextDB = nextDB
	srv.gcStats.Cycles++
	srv.gcStats.Batches += uint64(batches)
	srv.gcStats.TasksCompleted += completed
	srv.gcStats.RecordsReclaimed += records
	srv.gcStats.BytesReclaimed += reclaimedBytes
	srv.gcStats.LastCycleUnixMs = nowMs
	srv.gcStats.LastError = ""
	srv.gcMu.Unlock()
	return nil
}
