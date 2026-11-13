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
	redisObjectMigrationSchemaVersion = 1
	redisObjectMigrationPhaseScanning = 1
	redisObjectMigrationPhaseCopying  = 2
	redisObjectMigrationPhaseIdle     = 3
	redisObjectMigrationScanBatchSize = 256
)

var redisObjectMigrationStateKey = []byte("object_migration_v1")

type redisObjectMigrationState struct {
	Phase       byte
	Type        byte
	BucketPart  byte
	VerifyPass  byte
	ScanCursor  []byte
	Key         []byte
	EntryCursor []byte
	SourceID    uint64
	DestID      uint64
	Expected    [2]uint64
	Copied      [2]uint64
}

type redisObjectMigrationStats struct {
	ObjectsMigrated uint64
	RecordsCopied   uint64
	BytesCopied     uint64
	Restarts        uint64
	LastError       string
}

func serializeRedisObjectMigrationState(state redisObjectMigrationState) ([]byte, error) {
	if state.Phase != redisObjectMigrationPhaseScanning && state.Phase != redisObjectMigrationPhaseCopying && state.Phase != redisObjectMigrationPhaseIdle {
		return nil, errors.New("invalid redis object migration phase")
	}
	if len(state.ScanCursor) > 0xffff || len(state.Key) > 0xffff || len(state.EntryCursor) > 0xffff {
		return nil, errors.New("redis object migration cursor is too large")
	}
	buf := make([]byte, 64+len(state.ScanCursor)+len(state.Key)+len(state.EntryCursor))
	buf[0] = redisObjectMigrationSchemaVersion
	buf[1] = state.Phase
	buf[2] = state.Type
	buf[3] = state.BucketPart
	buf[10] = state.VerifyPass
	binary.BigEndian.PutUint16(buf[4:6], uint16(len(state.ScanCursor)))
	binary.BigEndian.PutUint16(buf[6:8], uint16(len(state.Key)))
	binary.BigEndian.PutUint16(buf[8:10], uint16(len(state.EntryCursor)))
	binary.BigEndian.PutUint64(buf[16:24], state.SourceID)
	binary.BigEndian.PutUint64(buf[24:32], state.DestID)
	binary.BigEndian.PutUint64(buf[32:40], state.Expected[0])
	binary.BigEndian.PutUint64(buf[40:48], state.Expected[1])
	binary.BigEndian.PutUint64(buf[48:56], state.Copied[0])
	binary.BigEndian.PutUint64(buf[56:64], state.Copied[1])
	offset := 64
	copy(buf[offset:], state.ScanCursor)
	offset += len(state.ScanCursor)
	copy(buf[offset:], state.Key)
	offset += len(state.Key)
	copy(buf[offset:], state.EntryCursor)
	return buf, nil
}

func deserializeRedisObjectMigrationState(buf []byte) (redisObjectMigrationState, error) {
	if len(buf) < 64 || buf[0] != redisObjectMigrationSchemaVersion {
		return redisObjectMigrationState{}, errors.New("corrupted redis object migration state")
	}
	scanLen := int(binary.BigEndian.Uint16(buf[4:6]))
	keyLen := int(binary.BigEndian.Uint16(buf[6:8]))
	entryLen := int(binary.BigEndian.Uint16(buf[8:10]))
	if 64+scanLen+keyLen+entryLen != len(buf) {
		return redisObjectMigrationState{}, errors.New("corrupted redis object migration state length")
	}
	state := redisObjectMigrationState{
		Phase:      buf[1],
		Type:       buf[2],
		BucketPart: buf[3],
		VerifyPass: buf[10],
		SourceID:   binary.BigEndian.Uint64(buf[16:24]),
		DestID:     binary.BigEndian.Uint64(buf[24:32]),
		Expected: [2]uint64{
			binary.BigEndian.Uint64(buf[32:40]),
			binary.BigEndian.Uint64(buf[40:48]),
		},
		Copied: [2]uint64{
			binary.BigEndian.Uint64(buf[48:56]),
			binary.BigEndian.Uint64(buf[56:64]),
		},
	}
	if state.Phase != redisObjectMigrationPhaseScanning && state.Phase != redisObjectMigrationPhaseCopying && state.Phase != redisObjectMigrationPhaseIdle {
		return redisObjectMigrationState{}, errors.New("corrupted redis object migration phase")
	}
	offset := 64
	state.ScanCursor = cloneBytes(buf[offset : offset+scanLen])
	offset += scanLen
	state.Key = cloneBytes(buf[offset : offset+keyLen])
	offset += keyLen
	state.EntryCursor = cloneBytes(buf[offset : offset+entryLen])
	return state, nil
}

func loadRedisObjectMigrationStateTx(tx *storage.Tx, ns redisNamespace) (redisObjectMigrationState, error) {
	bucket, err := tx.GetBucket(ns.keyMetaSysBucket)
	if errors.Is(err, storage.ErrBucketNotFound) {
		return redisObjectMigrationState{Phase: redisObjectMigrationPhaseScanning}, nil
	}
	if err != nil {
		return redisObjectMigrationState{}, err
	}
	raw, found := bucket.Get(redisObjectMigrationStateKey)
	if !found {
		return redisObjectMigrationState{Phase: redisObjectMigrationPhaseScanning}, nil
	}
	return deserializeRedisObjectMigrationState(raw)
}

func saveRedisObjectMigrationStateTx(tx *storage.Tx, ns redisNamespace, state redisObjectMigrationState) error {
	encoded, err := serializeRedisObjectMigrationState(state)
	if err != nil {
		return err
	}
	bucket, err := tx.CreateBucketIfNotExists(ns.keyMetaSysBucket)
	if err != nil {
		return err
	}
	return bucket.Put(redisObjectMigrationStateKey, encoded)
}

func nextRedisObjectIDTx(tx *storage.Tx, ns redisNamespace, typeCode byte) (uint64, error) {
	switch typeCode {
	case redisKeyMetaTypeList:
		return nextRedisListIDTx(tx, ns)
	case redisKeyMetaTypeHash:
		return nextRedisHashIDTx(tx, ns)
	case redisKeyMetaTypeZSet:
		return nextRedisZSetIDTx(tx, ns)
	case redisKeyMetaTypeBloom:
		return nextRedisBloomIDTx(tx, ns)
	case redisKeyMetaTypeTopK:
		return nextRedisTopKIDTx(tx, ns)
	default:
		return 0, errors.New("redis type has no migratable object ID")
	}
}

type redisMigrationBucket struct {
	legacy []byte
	shared []byte
}

func redisMigrationBuckets(ns redisNamespace, typeCode byte, objectID uint64) ([]redisMigrationBucket, error) {
	switch typeCode {
	case redisKeyMetaTypeList:
		return []redisMigrationBucket{{legacy: redisListBucketName(ns, objectID), shared: ns.listDataBucket}}, nil
	case redisKeyMetaTypeHash:
		return []redisMigrationBucket{{legacy: redisHashBucketName(ns, objectID), shared: ns.hashDataBucket}}, nil
	case redisKeyMetaTypeZSet:
		return []redisMigrationBucket{
			{legacy: redisZSetMemberBucketName(ns, objectID), shared: ns.zsetMemberBucket},
			{legacy: redisZSetScoreBucketName(ns, objectID), shared: ns.zsetScoreBucket},
		}, nil
	case redisKeyMetaTypeBloom:
		return []redisMigrationBucket{{legacy: redisBloomDataBucketName(ns, objectID), shared: ns.bloomDataBucket}}, nil
	case redisKeyMetaTypeTopK:
		return []redisMigrationBucket{{legacy: redisTopKDataBucketName(ns, objectID), shared: ns.topkDataBucket}}, nil
	default:
		return nil, errors.New("redis type has no migratable buckets")
	}
}

func beginRedisObjectMigrationTx(tx *storage.Tx, ns redisNamespace) (redisObjectMigrationState, bool, error) {
	state, err := loadRedisObjectMigrationStateTx(tx, ns)
	if err != nil {
		return state, false, err
	}
	if state.Phase == redisObjectMigrationPhaseCopying {
		return state, true, nil
	}
	if state.Phase == redisObjectMigrationPhaseIdle {
		return state, false, nil
	}
	active, err := redisKeyDirectoryActiveTx(tx, ns)
	if err != nil || !active {
		return state, false, err
	}
	bucket, err := tx.GetBucket(ns.keyMetaBucket)
	if errors.Is(err, storage.ErrBucketNotFound) {
		state.ScanCursor = nil
		return state, false, saveRedisObjectMigrationStateTx(tx, ns, state)
	}
	if err != nil {
		return state, false, err
	}
	cursor := bucket.Cursor()
	var item *storage.Item
	if len(state.ScanCursor) == 0 {
		item = cursor.FirstItem()
	} else {
		item = cursor.SeekItem(nextScanSeekKey(state.ScanCursor))
	}
	scanned := 0
	for item != nil && scanned < redisObjectMigrationScanBatchSize {
		rawMeta, exists := bucket.Get(item.Key)
		if !exists {
			return state, false, errors.New("redis key metadata disappeared during migration scan")
		}
		meta, metaErr := deserializeRedisKeyMeta(rawMeta)
		if metaErr != nil {
			return state, false, metaErr
		}
		state.ScanCursor = cloneBytes(item.Key)
		scanned++
		if meta.StorageFormat == redisKeyStorageLegacy && meta.Type != redisKeyMetaTypeString {
			destID, idErr := nextRedisObjectIDTx(tx, ns, meta.Type)
			if idErr != nil {
				return state, false, idErr
			}
			buckets, bucketErr := redisMigrationBuckets(ns, meta.Type, meta.ObjectID)
			if bucketErr != nil {
				return state, false, bucketErr
			}
			state.Phase = redisObjectMigrationPhaseCopying
			state.Type = meta.Type
			state.BucketPart = 0
			state.VerifyPass = 0
			state.Key = cloneBytes(item.Key)
			state.EntryCursor = nil
			state.SourceID = meta.ObjectID
			state.DestID = destID
			state.Expected = [2]uint64{}
			state.Copied = [2]uint64{}
			for idx, descriptor := range buckets {
				source, getErr := tx.GetBucket(descriptor.legacy)
				if getErr != nil {
					return state, false, fmt.Errorf("open redis migration source: %w", getErr)
				}
				state.Expected[idx] = source.ItemCount()
			}
			if saveErr := saveRedisObjectMigrationStateTx(tx, ns, state); saveErr != nil {
				return state, false, saveErr
			}
			return state, true, nil
		}
		item = cursor.NextItem()
	}
	if err = cursor.Err(); err != nil {
		return state, false, err
	}
	if item != nil {
		return state, false, saveRedisObjectMigrationStateTx(tx, ns, state)
	}
	// Verify twice so a legacy key moved behind the persisted scan cursor cannot
	// be missed by a single concurrent rename.
	state.ScanCursor = nil
	if state.VerifyPass == 0 {
		state.VerifyPass = 1
	} else {
		state.VerifyPass = 0
		state.Phase = redisObjectMigrationPhaseIdle
	}
	return state, false, saveRedisObjectMigrationStateTx(tx, ns, state)
}

func resetRedisObjectMigrationTx(tx *storage.Tx, ns redisNamespace, state redisObjectMigrationState, nowMs int64) error {
	if state.DestID != 0 {
		buckets, err := redisMigrationBuckets(ns, state.Type, state.SourceID)
		if err != nil {
			return err
		}
		for _, descriptor := range buckets {
			if _, err = enqueueRedisObjectPrefixGCTx(tx, ns, descriptor.shared, state.DestID, nowMs); err != nil {
				return err
			}
		}
	}
	state.Phase = redisObjectMigrationPhaseScanning
	state.Type = 0
	state.BucketPart = 0
	state.VerifyPass = 0
	state.ScanCursor = nil
	state.Key = nil
	state.EntryCursor = nil
	state.SourceID = 0
	state.DestID = 0
	state.Expected = [2]uint64{}
	state.Copied = [2]uint64{}
	return saveRedisObjectMigrationStateTx(tx, ns, state)
}

func activateRedisObjectMigrationTx(tx *storage.Tx, ns redisNamespace, state redisObjectMigrationState, nowMs int64) error {
	meta, found, err := loadRedisKeyMetaTx(tx, ns, state.Key)
	if err != nil {
		return err
	}
	if !found || meta.Type != state.Type || meta.StorageFormat != redisKeyStorageLegacy || meta.ObjectID != state.SourceID {
		return resetRedisObjectMigrationTx(tx, ns, state, nowMs)
	}
	buckets, err := redisMigrationBuckets(ns, state.Type, state.SourceID)
	if err != nil {
		return err
	}
	for idx := range buckets {
		if state.Copied[idx] != state.Expected[idx] {
			return errors.New("redis object migration record count changed")
		}
	}
	var metaBucketName []byte
	switch state.Type {
	case redisKeyMetaTypeList:
		metaBucketName = ns.listMetaBucket
	case redisKeyMetaTypeHash:
		metaBucketName = ns.hashMetaBucket
	case redisKeyMetaTypeZSet:
		metaBucketName = ns.zsetMetaBucket
	case redisKeyMetaTypeBloom:
		metaBucketName = ns.bloomMetaBucket
	case redisKeyMetaTypeTopK:
		metaBucketName = ns.topkMetaBucket
	default:
		return errors.New("unsupported redis object migration type")
	}
	metaBucket, err := tx.GetBucket(metaBucketName)
	if err != nil {
		return err
	}
	raw, exists := metaBucket.Get(state.Key)
	if !exists || len(raw) < 8 || binary.BigEndian.Uint64(raw[:8]) != state.SourceID {
		return resetRedisObjectMigrationTx(tx, ns, state, nowMs)
	}
	binary.BigEndian.PutUint64(raw[:8], state.DestID)
	if err = metaBucket.Put(state.Key, raw); err != nil {
		return err
	}
	meta.StorageFormat = redisKeyStorageShared
	meta.ObjectID = state.DestID
	meta.Generation++
	if err = putRedisKeyMetaTx(tx, ns, state.Key, meta); err != nil {
		return err
	}
	for _, descriptor := range buckets {
		if _, err = enqueueRedisPrivateBucketGCTx(tx, ns, descriptor.legacy, nowMs); err != nil {
			return err
		}
	}
	state.Phase = redisObjectMigrationPhaseScanning
	state.Type = 0
	state.BucketPart = 0
	state.VerifyPass = 0
	state.ScanCursor = cloneBytes(state.Key)
	state.Key = nil
	state.EntryCursor = nil
	state.SourceID = 0
	state.DestID = 0
	state.Expected = [2]uint64{}
	state.Copied = [2]uint64{}
	return saveRedisObjectMigrationStateTx(tx, ns, state)
}

type redisObjectMigrationBatchResult struct {
	Done    bool
	Records int
	Bytes   int64
}

func copyRedisObjectMigrationBatchTx(tx *storage.Tx, ns redisNamespace, nowMs int64, recordLimit int, byteLimit int64) (redisObjectMigrationBatchResult, error) {
	state, err := loadRedisObjectMigrationStateTx(tx, ns)
	if err != nil {
		return redisObjectMigrationBatchResult{}, err
	}
	if state.Phase != redisObjectMigrationPhaseCopying {
		return redisObjectMigrationBatchResult{Done: true}, nil
	}
	meta, found, err := loadRedisKeyMetaTx(tx, ns, state.Key)
	if err != nil {
		return redisObjectMigrationBatchResult{}, err
	}
	if !found || meta.Type != state.Type || meta.StorageFormat != redisKeyStorageLegacy || meta.ObjectID != state.SourceID {
		return redisObjectMigrationBatchResult{Done: true}, resetRedisObjectMigrationTx(tx, ns, state, nowMs)
	}
	buckets, err := redisMigrationBuckets(ns, state.Type, state.SourceID)
	if err != nil {
		return redisObjectMigrationBatchResult{}, err
	}
	if int(state.BucketPart) >= len(buckets) {
		return redisObjectMigrationBatchResult{Done: true}, activateRedisObjectMigrationTx(tx, ns, state, nowMs)
	}
	part := int(state.BucketPart)
	descriptor := buckets[part]
	source, err := tx.GetBucket(descriptor.legacy)
	if err != nil {
		return redisObjectMigrationBatchResult{}, err
	}
	destinationRaw, err := tx.CreateBucketIfNotExists(descriptor.shared)
	if err != nil {
		return redisObjectMigrationBatchResult{}, err
	}
	destination := newRedisObjectBucket(destinationRaw, state.DestID, true)
	cursor := source.Cursor()
	var item *storage.Item
	if len(state.EntryCursor) == 0 {
		item = cursor.FirstItem()
	} else {
		item = cursor.SeekItem(nextScanSeekKey(state.EntryCursor))
	}
	result := redisObjectMigrationBatchResult{}
	for item != nil && result.Records < recordLimit {
		valueLen, exists, valueErr := source.ValueLen(item.Key)
		if valueErr != nil {
			return result, valueErr
		}
		if !exists {
			return result, errors.New("redis object migration source record disappeared")
		}
		recordBytes := int64(len(item.Key) + valueLen)
		if result.Records > 0 && result.Bytes+recordBytes > byteLimit {
			break
		}
		value, exists := source.Get(item.Key)
		if !exists {
			return result, errors.New("redis object migration source record disappeared")
		}
		if err = destination.Put(item.Key, value); err != nil {
			return result, err
		}
		state.EntryCursor = cloneBytes(item.Key)
		state.Copied[part]++
		result.Records++
		result.Bytes += recordBytes
		item = cursor.NextItem()
	}
	if err = cursor.Err(); err != nil {
		return result, err
	}
	if item == nil {
		if state.Copied[part] != state.Expected[part] {
			return result, errors.New("redis object migration source cardinality changed")
		}
		state.BucketPart++
		state.EntryCursor = nil
	}
	if int(state.BucketPart) >= len(buckets) {
		result.Done = true
		return result, activateRedisObjectMigrationTx(tx, ns, state, nowMs)
	}
	return result, saveRedisObjectMigrationStateTx(tx, ns, state)
}

func (srv *RedisServer) redisObjectMigrationEnabled() bool {
	return srv != nil && srv.Config != nil && srv.Config.Redis != nil && srv.Config.Redis.MigrationBatchSize > 0 && srv.Config.Redis.MigrationBatchBytes > 0
}

func (srv *RedisServer) objectMigrationLoop() {
	defer srv.wg.Done()
	restarted := [redisDatabaseCount]bool{}
	completed := [redisDatabaseCount]bool{}
	remaining := redisDatabaseCount
	for {
		madeProgress := false
		for dbIndex := redisDatabaseMin; dbIndex <= redisDatabaseMax; dbIndex++ {
			if completed[dbIndex] {
				continue
			}
			select {
			case <-srv.stopCh:
				return
			default:
			}
			ns := srv.redisNamespace(dbIndex)
			var state redisObjectMigrationState
			var found bool
			err := srv.DB.Update(func(tx *storage.Tx) error {
				var beginErr error
				state, found, beginErr = beginRedisObjectMigrationTx(tx, ns)
				return beginErr
			})
			if err != nil {
				srv.recordObjectMigrationError(err)
				return
			}
			if !found {
				if state.Phase == redisObjectMigrationPhaseIdle {
					completed[dbIndex] = true
					remaining--
					madeProgress = true
				}
				continue
			}
			madeProgress = true
			unlock := srv.mutationLocks.lockKeys(dbIndex, [][]byte{state.Key}, false)
			if !restarted[dbIndex] && (state.Copied[0] > 0 || state.Copied[1] > 0) {
				err = srv.DB.Update(func(tx *storage.Tx) error {
					current, loadErr := loadRedisObjectMigrationStateTx(tx, ns)
					if loadErr != nil {
						return loadErr
					}
					return resetRedisObjectMigrationTx(tx, ns, current, srv.nowUnixMilli())
				})
				restarted[dbIndex] = true
				unlock()
				if err != nil {
					srv.recordObjectMigrationError(err)
					return
				}
				srv.migrationMu.Lock()
				srv.migrationStats.Restarts++
				srv.migrationMu.Unlock()
				srv.wakeGCWorker()
				continue
			}
			restarted[dbIndex] = true
			for {
				select {
				case <-srv.stopCh:
					unlock()
					return
				default:
				}
				var result redisObjectMigrationBatchResult
				err = srv.DB.Update(func(tx *storage.Tx) error {
					var copyErr error
					result, copyErr = copyRedisObjectMigrationBatchTx(tx, ns, srv.nowUnixMilli(), srv.Config.Redis.MigrationBatchSize, srv.Config.Redis.MigrationBatchBytes)
					return copyErr
				})
				if err != nil {
					unlock()
					srv.recordObjectMigrationError(err)
					return
				}
				srv.migrationMu.Lock()
				srv.migrationStats.RecordsCopied += uint64(result.Records)
				srv.migrationStats.BytesCopied += uint64(result.Bytes)
				if result.Done {
					srv.migrationStats.ObjectsMigrated++
				}
				srv.migrationMu.Unlock()
				if result.Done {
					break
				}
			}
			unlock()
			srv.wakeGCWorker()
		}
		if remaining == 0 {
			return
		}
		if !madeProgress {
			select {
			case <-time.After(250 * time.Millisecond):
			case <-srv.stopCh:
				return
			}
		}
	}
}

func (srv *RedisServer) recordObjectMigrationError(err error) {
	srv.migrationMu.Lock()
	srv.migrationStats.LastError = err.Error()
	srv.migrationMu.Unlock()
	if srv.Logger != nil {
		srv.Logger.Error("Redis object migration failed", "error", err)
	}
}

func (srv *RedisServer) objectMigrationWorkerStats() redisObjectMigrationStats {
	srv.migrationMu.RLock()
	defer srv.migrationMu.RUnlock()
	return srv.migrationStats
}

func redisObjectMigrationStateMatches(state redisObjectMigrationState, key []byte, sourceID uint64) bool {
	return state.Phase == redisObjectMigrationPhaseCopying && state.SourceID == sourceID && bytes.Equal(state.Key, key)
}
