package main

import (
	"errors"
	"fmt"
	"time"

	"github.com/timson/pirindb/storage"
)

const redisKeyDirectoryMigrationBatchSize = 256

type redisLegacyKeySource struct {
	keyType    string
	bucketName func(redisNamespace) []byte
	objectID   func([]byte) (uint64, error)
}

var redisLegacyKeySources = []redisLegacyKeySource{
	{
		keyType:    redisKeyTypeString,
		bucketName: func(ns redisNamespace) []byte { return ns.stringBucket },
		objectID:   func([]byte) (uint64, error) { return 0, nil },
	},
	{
		keyType:    redisKeyTypeList,
		bucketName: func(ns redisNamespace) []byte { return ns.listMetaBucket },
		objectID: func(raw []byte) (uint64, error) {
			meta, err := deserializeRedisListMeta(raw)
			if err != nil {
				return 0, err
			}
			return meta.ID, nil
		},
	},
	{
		keyType:    redisKeyTypeHash,
		bucketName: func(ns redisNamespace) []byte { return ns.hashMetaBucket },
		objectID: func(raw []byte) (uint64, error) {
			meta, err := deserializeRedisHashMeta(raw)
			if err != nil {
				return 0, err
			}
			return meta.ID, nil
		},
	},
	{
		keyType:    redisKeyTypeBloom,
		bucketName: func(ns redisNamespace) []byte { return ns.bloomMetaBucket },
		objectID: func(raw []byte) (uint64, error) {
			meta, err := deserializeRedisBloomMeta(raw)
			if err != nil {
				return 0, err
			}
			return meta.ID, nil
		},
	},
	{
		keyType:    redisKeyTypeTopK,
		bucketName: func(ns redisNamespace) []byte { return ns.topkMetaBucket },
		objectID: func(raw []byte) (uint64, error) {
			meta, err := deserializeRedisTopKMeta(raw)
			if err != nil {
				return 0, err
			}
			return meta.ID, nil
		},
	},
	{
		keyType:    redisKeyTypeZSet,
		bucketName: func(ns redisNamespace) []byte { return ns.zsetMetaBucket },
		objectID: func(raw []byte) (uint64, error) {
			meta, err := deserializeRedisZSetMeta(raw)
			if err != nil {
				return 0, err
			}
			return meta.ID, nil
		},
	},
}

func migrateRedisKeyDirectoryBatchTx(tx *storage.Tx, ns redisNamespace, limit int) (bool, int, error) {
	state, found, err := loadRedisKeyDirectoryStateTx(tx, ns)
	if err != nil {
		return false, 0, err
	}
	if !found {
		state = redisKeyDirectoryState{Phase: redisKeyDirectoryPhaseBuilding}
	}
	if state.Phase == redisKeyDirectoryPhaseActive {
		return true, 0, nil
	}
	if limit <= 0 {
		limit = redisKeyDirectoryMigrationBatchSize
	}

	processed := 0
	for int(state.BucketIndex) < len(redisLegacyKeySources) && processed < limit {
		source := redisLegacyKeySources[state.BucketIndex]
		bucket, bucketErr := tx.GetBucket(source.bucketName(ns))
		if errors.Is(bucketErr, storage.ErrBucketNotFound) {
			state.BucketIndex++
			state.Cursor = nil
			continue
		}
		if bucketErr != nil {
			return false, processed, bucketErr
		}

		cursor := bucket.Cursor()
		var key, value []byte
		if len(state.Cursor) == 0 {
			key, value = cursor.First()
		} else {
			key, value = cursor.Seek(nextScanSeekKey(state.Cursor))
			for key != nil && string(key) <= string(state.Cursor) {
				key, value = cursor.Next()
			}
		}

		lastKey := cloneBytes(state.Cursor)
		for key != nil && processed < limit {
			objectID, objectErr := source.objectID(value)
			if objectErr != nil {
				return false, processed, objectErr
			}
			typeCode, typeErr := redisKeyMetaTypeCode(source.keyType)
			if typeErr != nil {
				return false, processed, typeErr
			}
			existing, exists, loadErr := loadRedisKeyMetaTx(tx, ns, key)
			if loadErr != nil {
				return false, processed, loadErr
			}
			if exists {
				existingType, typeErr := redisKeyTypeFromMetaCode(existing.Type)
				if typeErr != nil {
					return false, processed, typeErr
				}
				if existingType != source.keyType || existing.ObjectID != objectID {
					return false, processed, fmt.Errorf("redis key %q has conflicting legacy ownership", key)
				}
			} else {
				expireAtMs := int64(-1)
				if deadline, hasTTL, expiryErr := loadRedisExpireAtMsTx(tx, ns, key); expiryErr != nil {
					return false, processed, expiryErr
				} else if hasTTL {
					expireAtMs = deadline
				}
				if putErr := putRedisKeyMetaTx(tx, ns, key, redisKeyMeta{
					Type:          typeCode,
					StorageFormat: redisKeyStorageLegacy,
					ObjectID:      objectID,
					ExpireAtMs:    expireAtMs,
				}); putErr != nil {
					return false, processed, putErr
				}
			}
			if slotErr := ensureRedisSlotIndexEntryTx(tx, ns, key); slotErr != nil {
				return false, processed, slotErr
			}
			processed++
			lastKey = cloneBytes(key)
			key, value = cursor.Next()
		}

		if key == nil {
			state.BucketIndex++
			state.Cursor = nil
		} else {
			state.Cursor = lastKey
		}
	}

	if int(state.BucketIndex) >= len(redisLegacyKeySources) {
		state.Phase = redisKeyDirectoryPhaseActive
		state.BucketIndex = byte(len(redisLegacyKeySources))
		state.Cursor = nil
	}
	if err = saveRedisKeyDirectoryStateTx(tx, ns, state); err != nil {
		return false, processed, err
	}
	return state.Phase == redisKeyDirectoryPhaseActive, processed, nil
}

func (srv *RedisServer) migrateRedisKeyDirectoryBatch(dbIndex int) (bool, int, error) {
	var active bool
	var processed int
	err := srv.DB.Update(func(tx *storage.Tx) error {
		var innerErr error
		active, processed, innerErr = migrateRedisKeyDirectoryBatchTx(tx, srv.redisNamespace(dbIndex), redisKeyDirectoryMigrationBatchSize)
		return innerErr
	})
	return active, processed, err
}

func (srv *RedisServer) keyDirectoryMigrationLoop() {
	defer srv.wg.Done()
	active := make([]bool, redisDatabaseCount)
	remaining := redisDatabaseCount
	for remaining > 0 {
		madeProgress := false
		for dbIndex := redisDatabaseMin; dbIndex <= redisDatabaseMax; dbIndex++ {
			if active[dbIndex] {
				continue
			}
			select {
			case <-srv.stopCh:
				return
			default:
			}
			complete, processed, err := srv.migrateRedisKeyDirectoryBatch(dbIndex)
			if err != nil {
				srv.Logger.Error("Redis key-directory migration failed", "db", dbIndex, "error", err)
				return
			}
			if processed > 0 {
				madeProgress = true
			}
			if complete {
				active[dbIndex] = true
				remaining--
				madeProgress = true
			}
		}
		if remaining == 0 {
			return
		}
		if !madeProgress {
			select {
			case <-time.After(5 * time.Millisecond):
			case <-srv.stopCh:
				return
			}
		}
	}
}
