package main

import (
	"encoding/binary"
	"errors"
	"math"

	"github.com/timson/pirindb/storage"
)

const (
	redisBuildStateSchemaVersion  = 1
	redisBuildTypeList            = 1
	redisBuildTypeZSet            = 2
	redisLargeCollectionThreshold = uint64(4096)
	redisListCOWSegmentsPerBatch  = 32
	redisBuildRecoveryBatchSize   = 128
)

var errRedisLargeOperationRequiresStandalone = errors.New("large collection operation requires a standalone command")

func (srv *RedisServer) redisCOWBatchLimits() (int, int64) {
	records := 128
	bytes := int64(4 * 1024 * 1024)
	if srv != nil && srv.Config != nil && srv.Config.Redis != nil {
		if srv.Config.Redis.MigrationBatchSize > 0 {
			records = srv.Config.Redis.MigrationBatchSize
		}
		if srv.Config.Redis.MigrationBatchBytes > 0 {
			bytes = srv.Config.Redis.MigrationBatchBytes
		}
	}
	// Rebuilds may update two indexes and several B-tree paths per logical
	// record. Keep the configured payload budget below a conservative fraction
	// of the storage dirty-state ceiling.
	if srv != nil && srv.DB != nil && srv.DB.GetOptions() != nil {
		maxBytes := srv.DB.GetOptions().MaxTransactionBytes / 4
		if maxBytes > 0 && bytes > maxBytes {
			bytes = maxBytes
		}
	}
	if records <= 0 {
		records = 1
	}
	if bytes <= 0 {
		bytes = 1
	}
	return records, bytes
}

type redisBuildState struct {
	Type   byte
	DestID uint64
}

func serializeRedisBuildState(state redisBuildState) []byte {
	buf := make([]byte, 16)
	buf[0] = redisBuildStateSchemaVersion
	buf[1] = state.Type
	binary.BigEndian.PutUint64(buf[8:16], state.DestID)
	return buf
}

func deserializeRedisBuildState(buf []byte) (redisBuildState, error) {
	if len(buf) != 16 || buf[0] != redisBuildStateSchemaVersion || (buf[1] != redisBuildTypeList && buf[1] != redisBuildTypeZSet) {
		return redisBuildState{}, errors.New("corrupted redis hidden-build state")
	}
	state := redisBuildState{Type: buf[1], DestID: binary.BigEndian.Uint64(buf[8:16])}
	if state.DestID == 0 {
		return redisBuildState{}, errors.New("corrupted redis hidden-build object ID")
	}
	return state, nil
}

func saveRedisBuildStateTx(tx *storage.Tx, ns redisNamespace, key []byte, state redisBuildState) error {
	bucket, err := tx.CreateBucketIfNotExists(ns.buildStateBucket)
	if err != nil {
		return err
	}
	return bucket.Put(key, serializeRedisBuildState(state))
}

func deleteRedisBuildStateTx(tx *storage.Tx, ns redisNamespace, key []byte) error {
	bucket, err := optionalRedisBucket(tx, ns.buildStateBucket)
	if err != nil || bucket == nil {
		return err
	}
	err = bucket.Remove(key)
	if errors.Is(err, storage.ErrNodeNotFound) {
		return nil
	}
	return err
}

func enqueueRedisBuildDestinationGCTx(tx *storage.Tx, ns redisNamespace, state redisBuildState, nowMs int64) error {
	switch state.Type {
	case redisBuildTypeList:
		_, err := enqueueRedisObjectPrefixGCTx(tx, ns, ns.listDataBucket, state.DestID, nowMs)
		return err
	case redisBuildTypeZSet:
		if _, err := enqueueRedisObjectPrefixGCTx(tx, ns, ns.zsetMemberBucket, state.DestID, nowMs); err != nil {
			return err
		}
		_, err := enqueueRedisObjectPrefixGCTx(tx, ns, ns.zsetScoreBucket, state.DestID, nowMs)
		return err
	default:
		return errors.New("unsupported redis hidden-build type")
	}
}

func (srv *RedisServer) recoverRedisAbandonedBuilds() error {
	for dbIndex := redisDatabaseMin; dbIndex <= redisDatabaseMax; dbIndex++ {
		ns := srv.redisNamespace(dbIndex)
		for {
			processed := 0
			if err := srv.DB.Update(func(tx *storage.Tx) error {
				bucket, err := optionalRedisBucket(tx, ns.buildStateBucket)
				if err != nil || bucket == nil {
					return err
				}
				entries := make([]struct {
					key   []byte
					state redisBuildState
				}, 0, redisBuildRecoveryBatchSize)
				cursor := bucket.Cursor()
				for key, raw := cursor.First(); key != nil && len(entries) < redisBuildRecoveryBatchSize; key, raw = cursor.Next() {
					state, decodeErr := deserializeRedisBuildState(raw)
					if decodeErr != nil {
						return decodeErr
					}
					entries = append(entries, struct {
						key   []byte
						state redisBuildState
					}{cloneBytes(key), state})
				}
				if err = cursor.Err(); err != nil {
					return err
				}
				for _, entry := range entries {
					if err = enqueueRedisBuildDestinationGCTx(tx, ns, entry.state, srv.nowUnixMilli()); err != nil {
						return err
					}
					if err = bucket.Remove(entry.key); err != nil && !errors.Is(err, storage.ErrNodeNotFound) {
						return err
					}
				}
				processed = len(entries)
				return nil
			}); err != nil {
				return err
			}
			if processed == 0 {
				break
			}
		}
	}
	return nil
}

func (srv *RedisServer) redisListNeedsCOW(ns redisNamespace, key []byte) (bool, error) {
	var large bool
	err := srv.DB.View(func(tx *storage.Tx) error {
		meta, found, err := loadRedisListMetaTx(tx, ns, key)
		if err != nil {
			return err
		}
		large = found && meta.Length > redisLargeCollectionThreshold
		return nil
	})
	return large, err
}

type redisListCOWProgress struct {
	source     *redisListMeta
	dest       *redisListMeta
	nextSource uint64
	reverse    bool
	remaining  uint64
	unlimited  bool
	removed    uint64
}

func appendRedisListCOWSegmentTx(tx *storage.Tx, ns redisNamespace, progress *redisListCOWProgress, values [][]byte) error {
	if len(values) == 0 {
		return nil
	}
	segmentID, err := nextRedisListSegmentIDTx(tx, ns, progress.dest)
	if err != nil {
		return err
	}
	segment := &redisListSegment{}
	for _, value := range values {
		segment.appendValue(value)
	}
	if progress.reverse {
		segment.NextID = progress.dest.HeadSegID
		if progress.dest.HeadSegID != 0 {
			head, loadErr := loadRedisListSegmentTx(tx, ns, progress.dest, progress.dest.HeadSegID)
			if loadErr != nil {
				return loadErr
			}
			head.PrevID = segmentID
			if err = saveRedisListSegmentTx(tx, ns, progress.dest, progress.dest.HeadSegID, head); err != nil {
				return err
			}
		} else {
			progress.dest.TailSegID = segmentID
		}
		progress.dest.HeadSegID = segmentID
	} else {
		segment.PrevID = progress.dest.TailSegID
		if progress.dest.TailSegID != 0 {
			tail, loadErr := loadRedisListSegmentTx(tx, ns, progress.dest, progress.dest.TailSegID)
			if loadErr != nil {
				return loadErr
			}
			tail.NextID = segmentID
			if err = saveRedisListSegmentTx(tx, ns, progress.dest, progress.dest.TailSegID, tail); err != nil {
				return err
			}
		} else {
			progress.dest.HeadSegID = segmentID
		}
		progress.dest.TailSegID = segmentID
	}
	progress.dest.Length += uint64(len(values))
	return saveRedisListSegmentTx(tx, ns, progress.dest, segmentID, segment)
}

func (srv *RedisServer) cleanupRedisListCOW(ns redisNamespace, key []byte, destID uint64, nowMs int64) {
	_ = srv.DB.Update(func(tx *storage.Tx) error {
		if _, err := enqueueRedisObjectPrefixGCTx(tx, ns, ns.listDataBucket, destID, nowMs); err != nil {
			return err
		}
		return deleteRedisBuildStateTx(tx, ns, key)
	})
	srv.wakeGCWorker()
}

func (srv *RedisServer) redisListRemCOW(ns redisNamespace, key []byte, count int64, element []byte, nowMs int64) (int64, error) {
	progress := redisListCOWProgress{}
	err := srv.DB.Update(func(tx *storage.Tx) error {
		if _, err := purgeExpiredRedisKeyTx(tx, ns, key, nowMs); err != nil {
			return err
		}
		meta, found, err := loadRedisListMetaTx(tx, ns, key)
		if err != nil {
			return err
		}
		if !found {
			keyType, typeErr := redisKeyTypeTx(tx, ns, key, nowMs)
			if typeErr != nil {
				return typeErr
			}
			if keyType != redisKeyTypeNone {
				return errRedisWrongType
			}
			return nil
		}
		progress.source = meta
		progress.reverse = count < 0
		progress.unlimited = count == 0
		if count > 0 {
			progress.remaining = uint64(count)
		} else if count < 0 {
			progress.remaining = uint64(-(count + 1)) + 1
		} else {
			progress.remaining = math.MaxUint64
		}
		if progress.reverse {
			progress.nextSource = meta.TailSegID
		} else {
			progress.nextSource = meta.HeadSegID
		}
		destID, idErr := nextRedisListIDTx(tx, ns)
		if idErr != nil {
			return idErr
		}
		progress.dest = &redisListMeta{ID: destID, StorageFormat: redisKeyStorageShared}
		if _, idErr = ensureRedisListDataBucketTx(tx, ns, progress.dest); idErr != nil {
			return idErr
		}
		return saveRedisBuildStateTx(tx, ns, key, redisBuildState{Type: redisBuildTypeList, DestID: destID})
	})
	if err != nil || progress.source == nil {
		return 0, err
	}
	_, batchByteLimit := srv.redisCOWBatchLimits()

	for progress.nextSource != 0 {
		err = srv.DB.Update(func(tx *storage.Tx) error {
			keyMeta, found, loadErr := loadRedisKeyMetaTx(tx, ns, key)
			if loadErr != nil {
				return loadErr
			}
			if !found || keyMeta.Type != redisKeyMetaTypeList || keyMeta.ObjectID != progress.source.ID || keyMeta.StorageFormat != progress.source.StorageFormat {
				return errors.New("redis list changed during hidden rebuild")
			}
			var batchBytes int64
			for batch := 0; batch < redisListCOWSegmentsPerBatch && progress.nextSource != 0; batch++ {
				segment, segmentErr := loadRedisListSegmentTx(tx, ns, progress.source, progress.nextSource)
				if segmentErr != nil {
					return segmentErr
				}
				segmentBytes := int64(len(segment.serialize()))
				if batch > 0 && batchBytes+segmentBytes > batchByteLimit {
					break
				}
				next := segment.NextID
				if progress.reverse {
					next = segment.PrevID
				}
				values := segment.Values
				if progress.unlimited || progress.remaining > 0 {
					limit := int64(len(values))
					if !progress.unlimited && progress.remaining < uint64(limit) {
						limit = int64(progress.remaining)
					}
					var removed int64
					values, removed = removeRedisListSegmentValues(values, element, limit, progress.reverse)
					progress.removed += uint64(removed)
					if !progress.unlimited {
						progress.remaining -= uint64(removed)
					}
				}
				if appendErr := appendRedisListCOWSegmentTx(tx, ns, &progress, values); appendErr != nil {
					return appendErr
				}
				batchBytes += segmentBytes
				progress.nextSource = next
			}
			return nil
		})
		if err != nil {
			srv.cleanupRedisListCOW(ns, key, progress.dest.ID, nowMs)
			return 0, err
		}
	}

	err = srv.DB.Update(func(tx *storage.Tx) error {
		if progress.removed == 0 {
			if _, err := enqueueRedisObjectPrefixGCTx(tx, ns, ns.listDataBucket, progress.dest.ID, nowMs); err != nil {
				return err
			}
			return deleteRedisBuildStateTx(tx, ns, key)
		}
		keyMeta, found, err := loadRedisKeyMetaTx(tx, ns, key)
		if err != nil || !found || keyMeta.Type != redisKeyMetaTypeList || keyMeta.ObjectID != progress.source.ID {
			return errors.New("redis list changed before hidden rebuild activation")
		}
		if progress.dest.Length == 0 {
			if _, err = unlinkRedisKeyByRawTypeTx(tx, ns, key, redisKeyTypeList, nowMs); err != nil {
				return err
			}
			if _, err = enqueueRedisObjectPrefixGCTx(tx, ns, ns.listDataBucket, progress.dest.ID, nowMs); err != nil {
				return err
			}
			return deleteRedisBuildStateTx(tx, ns, key)
		}
		metaBucket, err := tx.GetBucket(ns.listMetaBucket)
		if err != nil {
			return err
		}
		if err = metaBucket.Put(key, progress.dest.serialize()); err != nil {
			return err
		}
		keyMeta.ObjectID = progress.dest.ID
		keyMeta.StorageFormat = redisKeyStorageShared
		keyMeta.Generation++
		if err = putRedisKeyMetaTx(tx, ns, key, keyMeta); err != nil {
			return err
		}
		if err = enqueueRedisObjectForDeletionTx(tx, ns, progress.source.StorageFormat, progress.source.ID, ns.listDataBucket, redisListBucketName(ns, progress.source.ID), nowMs); err != nil {
			return err
		}
		return deleteRedisBuildStateTx(tx, ns, key)
	})
	if err != nil {
		srv.cleanupRedisListCOW(ns, key, progress.dest.ID, nowMs)
		return 0, err
	}
	srv.wakeGCWorker()
	return int64(progress.removed), nil
}

func (srv *RedisServer) redisZSetNeedsCOW(ns redisNamespace, key []byte) (bool, error) {
	var large bool
	err := srv.DB.View(func(tx *storage.Tx) error {
		meta, found, err := loadRedisZSetMetaTx(tx, ns, key)
		if err != nil {
			return err
		}
		large = found && meta.Cardinality > redisLargeCollectionThreshold
		return nil
	})
	return large, err
}

type redisZSetCOWMatcher func(member, encodedScore []byte) (bool, error)

func (srv *RedisServer) cleanupRedisZSetCOW(ns redisNamespace, key []byte, destID uint64, nowMs int64) {
	_ = srv.DB.Update(func(tx *storage.Tx) error {
		if _, err := enqueueRedisObjectPrefixGCTx(tx, ns, ns.zsetMemberBucket, destID, nowMs); err != nil {
			return err
		}
		if _, err := enqueueRedisObjectPrefixGCTx(tx, ns, ns.zsetScoreBucket, destID, nowMs); err != nil {
			return err
		}
		return deleteRedisBuildStateTx(tx, ns, key)
	})
	srv.wakeGCWorker()
}

func (srv *RedisServer) redisZSetRemRangeCOW(ns redisNamespace, key []byte, nowMs int64, matcher redisZSetCOWMatcher) (int64, error) {
	var source *redisZSetMeta
	var destID uint64
	err := srv.DB.Update(func(tx *storage.Tx) error {
		if _, err := purgeExpiredRedisKeyTx(tx, ns, key, nowMs); err != nil {
			return err
		}
		meta, found, err := loadRedisZSetMetaTx(tx, ns, key)
		if err != nil {
			return err
		}
		if !found {
			keyType, typeErr := redisKeyTypeTx(tx, ns, key, nowMs)
			if typeErr != nil {
				return typeErr
			}
			if keyType != redisKeyTypeNone {
				return errRedisWrongType
			}
			return nil
		}
		source = meta
		destID, err = nextRedisZSetIDTx(tx, ns)
		if err != nil {
			return err
		}
		dest := &redisZSetMeta{ID: destID, StorageFormat: redisKeyStorageShared}
		if _, err = ensureRedisZSetMemberBucketTx(tx, ns, dest); err != nil {
			return err
		}
		if _, err = ensureRedisZSetScoreBucketTx(tx, ns, dest); err != nil {
			return err
		}
		return saveRedisBuildStateTx(tx, ns, key, redisBuildState{Type: redisBuildTypeZSet, DestID: destID})
	})
	if err != nil || source == nil {
		return 0, err
	}

	batchSize, batchByteLimit := srv.redisCOWBatchLimits()
	var cursorKey []byte
	var kept, removed uint64
	for {
		done := false
		err = srv.DB.Update(func(tx *storage.Tx) error {
			keyMeta, found, loadErr := loadRedisKeyMetaTx(tx, ns, key)
			if loadErr != nil {
				return loadErr
			}
			if !found || keyMeta.Type != redisKeyMetaTypeZSet || keyMeta.ObjectID != source.ID || keyMeta.StorageFormat != source.StorageFormat {
				return errors.New("redis sorted set changed during hidden rebuild")
			}
			sourceMembers, err := openRedisObjectBucketTx(tx, ns.zsetMemberBucket, redisZSetMemberBucketName(ns, source.ID), source.ID, source.StorageFormat, false)
			if err != nil {
				return err
			}
			dest := &redisZSetMeta{ID: destID, StorageFormat: redisKeyStorageShared}
			destMembers, err := ensureRedisZSetMemberBucketTx(tx, ns, dest)
			if err != nil {
				return err
			}
			destScores, err := ensureRedisZSetScoreBucketTx(tx, ns, dest)
			if err != nil {
				return err
			}
			cursor := sourceMembers.Cursor()
			var member, encodedScore []byte
			if len(cursorKey) == 0 {
				member, encodedScore = cursor.First()
			} else {
				member, encodedScore = cursor.Seek(nextScanSeekKey(cursorKey))
			}
			processed := 0
			var processedBytes int64
			for member != nil && processed < batchSize {
				recordBytes := int64(2*len(member) + 2*len(encodedScore) + 64)
				if processed > 0 && processedBytes+recordBytes > batchByteLimit {
					break
				}
				remove, matchErr := matcher(member, encodedScore)
				if matchErr != nil {
					return matchErr
				}
				if remove {
					removed++
				} else {
					if err = destMembers.Put(member, encodedScore); err != nil {
						return err
					}
					if err = destScores.Put(redisZSetScoreIndexKeyFromEncoded(encodedScore, member), []byte{}); err != nil {
						return err
					}
					kept++
				}
				cursorKey = cloneBytes(member)
				processed++
				processedBytes += recordBytes
				member, encodedScore = cursor.Next()
			}
			if err = cursor.Err(); err != nil {
				return err
			}
			done = member == nil
			return nil
		})
		if err != nil {
			srv.cleanupRedisZSetCOW(ns, key, destID, nowMs)
			return 0, err
		}
		if done {
			break
		}
	}

	err = srv.DB.Update(func(tx *storage.Tx) error {
		if removed == 0 {
			if err := enqueueRedisBuildDestinationGCTx(tx, ns, redisBuildState{Type: redisBuildTypeZSet, DestID: destID}, nowMs); err != nil {
				return err
			}
			return deleteRedisBuildStateTx(tx, ns, key)
		}
		keyMeta, found, err := loadRedisKeyMetaTx(tx, ns, key)
		if err != nil || !found || keyMeta.Type != redisKeyMetaTypeZSet || keyMeta.ObjectID != source.ID {
			return errors.New("redis sorted set changed before hidden rebuild activation")
		}
		if kept == 0 {
			if _, err = unlinkRedisKeyByRawTypeTx(tx, ns, key, redisKeyTypeZSet, nowMs); err != nil {
				return err
			}
			if err = enqueueRedisBuildDestinationGCTx(tx, ns, redisBuildState{Type: redisBuildTypeZSet, DestID: destID}, nowMs); err != nil {
				return err
			}
			return deleteRedisBuildStateTx(tx, ns, key)
		}
		metaBucket, err := tx.GetBucket(ns.zsetMetaBucket)
		if err != nil {
			return err
		}
		if err = metaBucket.Put(key, (&redisZSetMeta{ID: destID, Cardinality: kept}).serialize()); err != nil {
			return err
		}
		keyMeta.ObjectID = destID
		keyMeta.StorageFormat = redisKeyStorageShared
		keyMeta.Generation++
		if err = putRedisKeyMetaTx(tx, ns, key, keyMeta); err != nil {
			return err
		}
		if err = enqueueRedisObjectForDeletionTx(tx, ns, source.StorageFormat, source.ID, ns.zsetMemberBucket, redisZSetMemberBucketName(ns, source.ID), nowMs); err != nil {
			return err
		}
		if err = enqueueRedisObjectForDeletionTx(tx, ns, source.StorageFormat, source.ID, ns.zsetScoreBucket, redisZSetScoreBucketName(ns, source.ID), nowMs); err != nil {
			return err
		}
		return deleteRedisBuildStateTx(tx, ns, key)
	})
	if err != nil {
		srv.cleanupRedisZSetCOW(ns, key, destID, nowMs)
		return 0, err
	}
	srv.wakeGCWorker()
	return int64(removed), nil
}

func redisZSetScoreRangeCOWMatcher(min, max redisZSetScoreBound) redisZSetCOWMatcher {
	return func(_ []byte, encodedScore []byte) (bool, error) {
		score, err := decodeRedisZSetSortableScore(encodedScore)
		if err != nil {
			return false, err
		}
		return !redisZSetScoreBelowLower(score, min) && !redisZSetScoreAboveUpper(score, max), nil
	}
}

func redisZSetLexRangeCOWMatcher(min, max redisZSetLexBound) redisZSetCOWMatcher {
	return func(member, _ []byte) (bool, error) {
		return matchesRedisZSetLexLower(member, min) && matchesRedisZSetLexUpper(member, max), nil
	}
}
