package main

import (
	"encoding/binary"
	"errors"
	"math"
	"time"

	"github.com/timson/pirindb/storage"
)

var errRedisExpireTimeOutOfRange = errors.New("expire time is out of range")

const (
	redisExpireMetaSize        = 8
	redisExpireIndexHeaderSize = 12
	redisLazySweepLimit        = 64
)

type redisExpireIndexEntry struct {
	indexKey   []byte
	key        []byte
	expireAtMs int64
}

type redisExpirySweepResult struct {
	EntriesExamined int
	KeysRemoved     int
	StaleRemoved    int
	MoreDue         bool
}

func redisNowUnixMilli(t time.Time) int64 {
	return t.UnixMilli()
}

func redisExpirationDeadline(nowMs int64, durationMs int64) (int64, error) {
	if durationMs <= 0 {
		return nowMs, nil
	}
	if nowMs > math.MaxInt64-durationMs {
		return 0, errRedisExpireTimeOutOfRange
	}
	return nowMs + durationMs, nil
}

func encodeRedisExpireAtMs(expireAtMs int64) []byte {
	buf := make([]byte, redisExpireMetaSize)
	binary.BigEndian.PutUint64(buf, uint64(expireAtMs))
	return buf
}

func decodeRedisExpireAtMs(buf []byte) (int64, error) {
	if len(buf) != redisExpireMetaSize {
		return 0, errors.New("corrupted redis expire metadata")
	}
	return int64(binary.BigEndian.Uint64(buf)), nil
}

func redisExpireIndexKey(expireAtMs int64, key []byte) []byte {
	buf := make([]byte, redisExpireIndexHeaderSize+len(key))
	binary.BigEndian.PutUint64(buf[0:8], uint64(expireAtMs))
	binary.BigEndian.PutUint32(buf[8:12], uint32(len(key)))
	copy(buf[12:], key)
	return buf
}

func decodeRedisExpireIndexKey(buf []byte) (int64, []byte, error) {
	if len(buf) < redisExpireIndexHeaderSize {
		return 0, nil, errors.New("corrupted redis expire index")
	}

	keyLen := int(binary.BigEndian.Uint32(buf[8:12]))
	if keyLen < 0 || len(buf) != redisExpireIndexHeaderSize+keyLen {
		return 0, nil, errors.New("corrupted redis expire index")
	}

	return int64(binary.BigEndian.Uint64(buf[0:8])), cloneBytes(buf[12:]), nil
}

func loadRedisExpireAtMsTx(tx *storage.Tx, ns redisNamespace, key []byte) (int64, bool, error) {
	bucket, err := tx.GetBucket(ns.expireMetaBucket)
	if errors.Is(err, storage.ErrBucketNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}

	raw, found := bucket.Get(key)
	if !found {
		return 0, false, nil
	}

	expireAtMs, err := decodeRedisExpireAtMs(raw)
	if err != nil {
		return 0, false, err
	}
	return expireAtMs, true, nil
}

func deleteRedisExpireIndexEntryTx(tx *storage.Tx, ns redisNamespace, indexKey []byte) error {
	bucket, err := tx.GetBucket(ns.expireIndexBucket)
	if errors.Is(err, storage.ErrBucketNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	err = bucket.Remove(indexKey)
	if errors.Is(err, storage.ErrNodeNotFound) {
		return nil
	}
	return err
}

func deleteRedisExpireAtMsTx(tx *storage.Tx, ns redisNamespace, key []byte) error {
	expireAtMs, found, err := loadRedisExpireAtMsTx(tx, ns, key)
	if err != nil || !found {
		return err
	}

	metaBucket, err := tx.GetBucket(ns.expireMetaBucket)
	if errors.Is(err, storage.ErrBucketNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	err = metaBucket.Remove(key)
	if err != nil && !errors.Is(err, storage.ErrNodeNotFound) {
		return err
	}

	if err = deleteRedisExpireIndexEntryTx(tx, ns, redisExpireIndexKey(expireAtMs, key)); err != nil {
		return err
	}
	return updateRedisKeyMetaExpiryTx(tx, ns, key, -1)
}

func setRedisExpireAtMsTx(tx *storage.Tx, ns redisNamespace, key []byte, expireAtMs int64) error {
	if err := deleteRedisExpireAtMsTx(tx, ns, key); err != nil {
		return err
	}

	metaBucket, err := tx.CreateBucketIfNotExists(ns.expireMetaBucket)
	if err != nil {
		return err
	}
	if err = metaBucket.Put(key, encodeRedisExpireAtMs(expireAtMs)); err != nil {
		return err
	}

	indexBucket, err := tx.CreateBucketIfNotExists(ns.expireIndexBucket)
	if err != nil {
		return err
	}
	if err = indexBucket.Put(redisExpireIndexKey(expireAtMs, key), []byte{}); err != nil {
		return err
	}
	return updateRedisKeyMetaExpiryTx(tx, ns, key, expireAtMs)
}

func redisKeyExpiredTx(tx *storage.Tx, ns redisNamespace, key []byte, nowMs int64) (bool, error) {
	expireAtMs, found, err := loadRedisExpireAtMsTx(tx, ns, key)
	if err != nil || !found {
		return false, err
	}
	return expireAtMs <= nowMs, nil
}

func redisRawKeyTypeTx(tx *storage.Tx, ns redisNamespace, key []byte) (string, error) {
	keyMeta, found, err := loadRedisKeyMetaTx(tx, ns, key)
	if err != nil {
		return redisKeyTypeNone, err
	}
	if found {
		return redisKeyTypeFromMetaCode(keyMeta.Type)
	}
	active, err := redisKeyDirectoryActiveTx(tx, ns)
	if err != nil {
		return redisKeyTypeNone, err
	}
	if active {
		return redisKeyTypeNone, nil
	}
	if _, found, err := loadRedisHashMetaTx(tx, ns, key); err != nil {
		return redisKeyTypeNone, err
	} else if found {
		return redisKeyTypeHash, nil
	}
	if _, found, err := loadRedisBloomMetaTx(tx, ns, key); err != nil {
		return redisKeyTypeNone, err
	} else if found {
		return redisKeyTypeBloom, nil
	}
	if _, found, err := loadRedisTopKMetaTx(tx, ns, key); err != nil {
		return redisKeyTypeNone, err
	} else if found {
		return redisKeyTypeTopK, nil
	}
	if _, found, err := loadRedisZSetMetaTx(tx, ns, key); err != nil {
		return redisKeyTypeNone, err
	} else if found {
		return redisKeyTypeZSet, nil
	}
	if _, found, err := loadRedisListMetaTx(tx, ns, key); err != nil {
		return redisKeyTypeNone, err
	} else if found {
		return redisKeyTypeList, nil
	}
	found, err = redisStringKeyExistsTx(tx, ns, key)
	if err != nil {
		return redisKeyTypeNone, err
	}
	if found {
		return redisKeyTypeString, nil
	}
	return redisKeyTypeNone, nil
}

func redisKeyTypeTx(tx *storage.Tx, ns redisNamespace, key []byte, nowMs int64) (string, error) {
	keyType, err := redisRawKeyTypeTx(tx, ns, key)
	if err != nil || keyType == redisKeyTypeNone {
		return keyType, err
	}

	expired, err := redisKeyExpiredTx(tx, ns, key, nowMs)
	if err != nil {
		return redisKeyTypeNone, err
	}
	if expired {
		return redisKeyTypeNone, nil
	}
	return keyType, nil
}

func deleteRedisKeyByRawTypeTx(tx *storage.Tx, ns redisNamespace, key []byte, keyType string) (bool, error) {
	removed := false
	switch keyType {
	case redisKeyTypeNone:
	case redisKeyTypeString:
		bucket, err := tx.GetBucket(ns.stringBucket)
		if errors.Is(err, storage.ErrBucketNotFound) {
			break
		}
		if err != nil {
			return false, err
		}
		err = bucket.Remove(key)
		if errors.Is(err, storage.ErrNodeNotFound) {
			err = nil
		}
		if err != nil {
			return false, err
		}
		removed = true
		if err := deleteRedisKeyMetaTx(tx, ns, key); err != nil {
			return false, err
		}
		if err := deleteRedisSlotIndexEntryTx(tx, ns, key); err != nil {
			return false, err
		}
	case redisKeyTypeList:
		meta, found, err := loadRedisListMetaTx(tx, ns, key)
		if err != nil {
			return false, err
		}
		if !found {
			break
		}
		if err = deleteRedisListTx(tx, ns, key, meta); err != nil {
			return false, err
		}
		removed = true
	case redisKeyTypeHash:
		meta, found, err := loadRedisHashMetaTx(tx, ns, key)
		if err != nil {
			return false, err
		}
		if !found {
			break
		}
		if err = deleteRedisHashTx(tx, ns, key, meta); err != nil {
			return false, err
		}
		removed = true
	case redisKeyTypeBloom:
		meta, found, err := loadRedisBloomMetaTx(tx, ns, key)
		if err != nil {
			return false, err
		}
		if !found {
			break
		}
		if err = deleteRedisBloomTx(tx, ns, key, meta); err != nil {
			return false, err
		}
		removed = true
	case redisKeyTypeTopK:
		meta, found, err := loadRedisTopKMetaTx(tx, ns, key)
		if err != nil {
			return false, err
		}
		if !found {
			break
		}
		if err = deleteRedisTopKTx(tx, ns, key, meta); err != nil {
			return false, err
		}
		removed = true
	case redisKeyTypeZSet:
		meta, found, err := loadRedisZSetMetaTx(tx, ns, key)
		if err != nil {
			return false, err
		}
		if !found {
			break
		}
		if err = deleteRedisZSetTx(tx, ns, key, meta); err != nil {
			return false, err
		}
		removed = true
	default:
		return false, errors.New("unsupported redis key type")
	}

	if err := deleteRedisExpireAtMsTx(tx, ns, key); err != nil {
		return false, err
	}
	return removed, nil
}

func enqueueRedisObjectForDeletionTx(tx *storage.Tx, ns redisNamespace, storageFormat byte, objectID uint64, sharedBucket, legacyBucket []byte, nowMs int64) error {
	if storageFormat == redisKeyStorageShared {
		_, err := enqueueRedisObjectPrefixGCTx(tx, ns, sharedBucket, objectID, nowMs)
		return err
	}
	_, err := enqueueRedisPrivateBucketGCTx(tx, ns, legacyBucket, nowMs)
	return err
}

func unlinkRedisKeyByRawTypeTx(tx *storage.Tx, ns redisNamespace, key []byte, keyType string, nowMs int64) (bool, error) {
	removed := false
	switch keyType {
	case redisKeyTypeNone:
	case redisKeyTypeString:
		if _, err := enqueueRedisExactKeyGCTx(tx, ns, ns.stringBucket, key, nowMs); err != nil {
			return false, err
		}
		if err := deleteRedisKeyMetaTx(tx, ns, key); err != nil {
			return false, err
		}
		if err := deleteRedisSlotIndexEntryTx(tx, ns, key); err != nil {
			return false, err
		}
		removed = true
	case redisKeyTypeList:
		meta, found, err := loadRedisListMetaTx(tx, ns, key)
		if err != nil || !found {
			return false, err
		}
		if err = enqueueRedisObjectForDeletionTx(tx, ns, meta.StorageFormat, meta.ID, ns.listDataBucket, redisListBucketName(ns, meta.ID), nowMs); err != nil {
			return false, err
		}
		if err = deleteRedisListMetaTx(tx, ns, key); err != nil {
			return false, err
		}
		removed = true
	case redisKeyTypeHash:
		meta, found, err := loadRedisHashMetaTx(tx, ns, key)
		if err != nil || !found {
			return false, err
		}
		if err = enqueueRedisObjectForDeletionTx(tx, ns, meta.StorageFormat, meta.ID, ns.hashDataBucket, redisHashBucketName(ns, meta.ID), nowMs); err != nil {
			return false, err
		}
		if err = deleteRedisHashMetaTx(tx, ns, key); err != nil {
			return false, err
		}
		removed = true
	case redisKeyTypeZSet:
		meta, found, err := loadRedisZSetMetaTx(tx, ns, key)
		if err != nil || !found {
			return false, err
		}
		if err = enqueueRedisObjectForDeletionTx(tx, ns, meta.StorageFormat, meta.ID, ns.zsetMemberBucket, redisZSetMemberBucketName(ns, meta.ID), nowMs); err != nil {
			return false, err
		}
		if err = enqueueRedisObjectForDeletionTx(tx, ns, meta.StorageFormat, meta.ID, ns.zsetScoreBucket, redisZSetScoreBucketName(ns, meta.ID), nowMs); err != nil {
			return false, err
		}
		if err = deleteRedisZSetMetaTx(tx, ns, key); err != nil {
			return false, err
		}
		removed = true
	case redisKeyTypeBloom:
		meta, found, err := loadRedisBloomMetaTx(tx, ns, key)
		if err != nil || !found {
			return false, err
		}
		if err = enqueueRedisObjectForDeletionTx(tx, ns, meta.StorageFormat, meta.ID, ns.bloomDataBucket, redisBloomDataBucketName(ns, meta.ID), nowMs); err != nil {
			return false, err
		}
		if err = deleteRedisBloomMetaTx(tx, ns, key); err != nil {
			return false, err
		}
		removed = true
	case redisKeyTypeTopK:
		meta, found, err := loadRedisTopKMetaTx(tx, ns, key)
		if err != nil || !found {
			return false, err
		}
		if err = enqueueRedisObjectForDeletionTx(tx, ns, meta.StorageFormat, meta.ID, ns.topkDataBucket, redisTopKDataBucketName(ns, meta.ID), nowMs); err != nil {
			return false, err
		}
		if err = deleteRedisTopKMetaTx(tx, ns, key); err != nil {
			return false, err
		}
		removed = true
	default:
		return false, errors.New("unsupported redis key type")
	}
	if err := deleteRedisExpireAtMsTx(tx, ns, key); err != nil {
		return false, err
	}
	return removed, nil
}

func redisKeyPhysicalDeleteShouldQueueTx(tx *storage.Tx, ns redisNamespace, key []byte, keyType string) (bool, error) {
	const collectionRecordThreshold = uint64(4096)
	switch keyType {
	case redisKeyTypeString:
		bucket, err := optionalRedisBucket(tx, ns.stringBucket)
		if err != nil || bucket == nil {
			return false, err
		}
		length, found, err := bucket.ValueLen(key)
		return found && length > 4*1024*1024, err
	case redisKeyTypeList:
		meta, found, err := loadRedisListMetaTx(tx, ns, key)
		return found && meta.Length > collectionRecordThreshold, err
	case redisKeyTypeHash:
		meta, found, err := loadRedisHashMetaTx(tx, ns, key)
		return found && meta.FieldCount > collectionRecordThreshold, err
	case redisKeyTypeZSet:
		meta, found, err := loadRedisZSetMetaTx(tx, ns, key)
		return found && meta.Cardinality > collectionRecordThreshold, err
	case redisKeyTypeBloom:
		meta, found, err := loadRedisBloomMetaTx(tx, ns, key)
		return found && (meta.InitialCapacity > collectionRecordThreshold || meta.SubFilterCount > 4), err
	case redisKeyTypeTopK:
		meta, found, err := loadRedisTopKMetaTx(tx, ns, key)
		if err != nil || !found {
			return false, err
		}
		return meta.Width > 0 && meta.Depth > collectionRecordThreshold/meta.Width, nil
	default:
		return false, nil
	}
}

func deleteRedisKeyAdaptivelyTx(tx *storage.Tx, ns redisNamespace, key []byte, keyType string, nowMs int64) (bool, error) {
	queued, err := redisKeyPhysicalDeleteShouldQueueTx(tx, ns, key, keyType)
	if err != nil {
		return false, err
	}
	if queued {
		return unlinkRedisKeyByRawTypeTx(tx, ns, key, keyType, nowMs)
	}
	return deleteRedisKeyByRawTypeTx(tx, ns, key, keyType)
}

func purgeExpiredRedisKeyTx(tx *storage.Tx, ns redisNamespace, key []byte, nowMs int64) (bool, error) {
	expired, err := redisKeyExpiredTx(tx, ns, key, nowMs)
	if err != nil || !expired {
		return false, err
	}

	keyType, err := redisRawKeyTypeTx(tx, ns, key)
	if err != nil {
		return false, err
	}

	_, err = unlinkRedisKeyByRawTypeTx(tx, ns, key, keyType, nowMs)
	return true, err
}

func redisExpireKeyAtTx(tx *storage.Tx, ns redisNamespace, key []byte, expireAtMs int64, nowMs int64) (int64, error) {
	if _, err := purgeExpiredRedisKeyTx(tx, ns, key, nowMs); err != nil {
		return 0, err
	}

	keyType, err := redisRawKeyTypeTx(tx, ns, key)
	if err != nil {
		return 0, err
	}
	if keyType == redisKeyTypeNone {
		return 0, nil
	}

	if expireAtMs <= nowMs {
		_, err = unlinkRedisKeyByRawTypeTx(tx, ns, key, keyType, nowMs)
		if err != nil {
			return 0, err
		}
		return 1, nil
	}

	if err = setRedisExpireAtMsTx(tx, ns, key, expireAtMs); err != nil {
		return 0, err
	}
	return 1, nil
}

func redisPersistTTLTx(tx *storage.Tx, ns redisNamespace, key []byte, nowMs int64) (int64, error) {
	if _, err := purgeExpiredRedisKeyTx(tx, ns, key, nowMs); err != nil {
		return 0, err
	}

	keyType, err := redisRawKeyTypeTx(tx, ns, key)
	if err != nil {
		return 0, err
	}
	if keyType == redisKeyTypeNone {
		return 0, nil
	}

	if _, found, err := loadRedisExpireAtMsTx(tx, ns, key); err != nil {
		return 0, err
	} else if !found {
		return 0, nil
	}

	if err = deleteRedisExpireAtMsTx(tx, ns, key); err != nil {
		return 0, err
	}
	return 1, nil
}

func redisPTTLTx(tx *storage.Tx, ns redisNamespace, key []byte, nowMs int64) (int64, error) {
	keyType, err := redisRawKeyTypeTx(tx, ns, key)
	if err != nil {
		return 0, err
	}
	if keyType == redisKeyTypeNone {
		return -2, nil
	}

	expireAtMs, found, err := loadRedisExpireAtMsTx(tx, ns, key)
	if err != nil {
		return 0, err
	}
	if found && expireAtMs <= nowMs {
		return -2, nil
	}
	if !found {
		return -1, nil
	}
	return expireAtMs - nowMs, nil
}

func redisTTLTx(tx *storage.Tx, ns redisNamespace, key []byte, nowMs int64) (int64, error) {
	pttl, err := redisPTTLTx(tx, ns, key, nowMs)
	if err != nil || pttl < 0 {
		return pttl, err
	}
	return pttl / 1000, nil
}

func redisExpiryDueTx(tx *storage.Tx, ns redisNamespace, nowMs int64) (bool, error) {
	indexBucket, err := tx.GetBucket(ns.expireIndexBucket)
	if errors.Is(err, storage.ErrBucketNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	indexKey, _ := indexBucket.Cursor().First()
	if indexKey == nil {
		return false, nil
	}
	expireAtMs, _, err := decodeRedisExpireIndexKey(indexKey)
	if err != nil {
		return false, err
	}
	return expireAtMs <= nowMs, nil
}

func sweepExpiredRedisKeysBatchTx(tx *storage.Tx, ns redisNamespace, nowMs int64, limit int) (redisExpirySweepResult, error) {
	var result redisExpirySweepResult
	indexBucket, err := tx.GetBucket(ns.expireIndexBucket)
	if errors.Is(err, storage.ErrBucketNotFound) {
		return result, nil
	}
	if err != nil {
		return result, err
	}

	entries := make([]redisExpireIndexEntry, 0)
	cursor := indexBucket.Cursor()
	for indexKey, _ := cursor.First(); indexKey != nil; indexKey, _ = cursor.Next() {
		expireAtMs, key, decodeErr := decodeRedisExpireIndexKey(indexKey)
		if decodeErr != nil {
			return result, decodeErr
		}
		if expireAtMs > nowMs {
			break
		}
		if limit > 0 && len(entries) >= limit {
			result.MoreDue = true
			break
		}
		entries = append(entries, redisExpireIndexEntry{
			indexKey:   cloneBytes(indexKey),
			key:        key,
			expireAtMs: expireAtMs,
		})
	}
	result.EntriesExamined = len(entries)

	for _, entry := range entries {
		expireAtMs, found, err := loadRedisExpireAtMsTx(tx, ns, entry.key)
		if err != nil {
			return result, err
		}
		if !found || expireAtMs != entry.expireAtMs {
			if err = deleteRedisExpireIndexEntryTx(tx, ns, entry.indexKey); err != nil {
				return result, err
			}
			result.StaleRemoved++
			continue
		}
		if _, err = purgeExpiredRedisKeyTx(tx, ns, entry.key, nowMs); err != nil {
			return result, err
		}
		result.KeysRemoved++
	}

	return result, nil
}

func sweepExpiredRedisKeysTx(tx *storage.Tx, ns redisNamespace, nowMs int64, limit int) (int, error) {
	result, err := sweepExpiredRedisKeysBatchTx(tx, ns, nowMs, limit)
	return result.KeysRemoved, err
}

func SweepExpiredRedisDB(db *storage.DB, ns redisNamespace, nowMs int64, limit int) error {
	return db.Update(func(tx *storage.Tx) error {
		_, err := sweepExpiredRedisKeysTx(tx, ns, nowMs, limit)
		return err
	})
}

func SweepAllExpiredRedis(db *storage.DB, nowMs int64, limitPerDB int) error {
	return db.Update(func(tx *storage.Tx) error {
		for dbIndex := redisDatabaseMin; dbIndex <= redisDatabaseMax; dbIndex++ {
			if _, err := sweepExpiredRedisKeysTx(tx, redisNamespaceForDB(dbIndex), nowMs, limitPerDB); err != nil {
				return err
			}
		}
		return nil
	})
}
