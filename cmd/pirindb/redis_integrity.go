package main

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/timson/pirindb/storage"
)

type RedisIntegrityReport struct {
	DatabasesChecked int
	KeysChecked      uint64
	Problems         []string
}

func (report *RedisIntegrityReport) add(dbIndex int, key []byte, format string, args ...any) {
	prefix := fmt.Sprintf("db%d", dbIndex)
	if key != nil {
		prefix += fmt.Sprintf(" key %q", key)
	}
	report.Problems = append(report.Problems, prefix+": "+fmt.Sprintf(format, args...))
}

func CheckRedisIntegrity(db *storage.DB, slotCount int) (*RedisIntegrityReport, error) {
	report := &RedisIntegrityReport{}
	if db == nil {
		return report, errors.New("redis database is nil")
	}
	if slotCount <= 0 {
		slotCount = 16384
	}
	err := db.View(func(tx *storage.Tx) error {
		for dbIndex := redisDatabaseMin; dbIndex <= redisDatabaseMax; dbIndex++ {
			ns := redisNamespaceForDB(dbIndex, slotCount)
			if err := checkRedisDBIntegrityTx(tx, ns, report); err != nil {
				return err
			}
			report.DatabasesChecked++
		}
		return nil
	})
	return report, err
}

func optionalRedisBucket(tx *storage.Tx, name []byte) (*storage.Bucket, error) {
	bucket, err := tx.GetBucket(name)
	if errors.Is(err, storage.ErrBucketNotFound) {
		return nil, nil
	}
	return bucket, err
}

func checkRedisDBIntegrityTx(tx *storage.Tx, ns redisNamespace, report *RedisIntegrityReport) error {
	active, err := redisKeyDirectoryActiveTx(tx, ns)
	if err != nil {
		return err
	}
	if !active {
		report.add(ns.dbIndex, nil, "key directory is not active")
	}
	directory := make(map[string]redisKeyMeta)
	keyBucket, err := optionalRedisBucket(tx, ns.keyMetaBucket)
	if err != nil {
		return err
	}
	if keyBucket != nil {
		cursor := keyBucket.Cursor()
		for key, raw := cursor.First(); key != nil; key, raw = cursor.Next() {
			meta, decodeErr := deserializeRedisKeyMeta(raw)
			if decodeErr != nil {
				report.add(ns.dbIndex, key, "%v", decodeErr)
				continue
			}
			directory[string(key)] = meta
			report.KeysChecked++
			checkRedisKeyIntegrityTx(tx, ns, key, meta, report)
		}
		if err = cursor.Err(); err != nil {
			return err
		}
	}
	if err = checkRedisTypeOwnershipTx(tx, ns, directory, report); err != nil {
		return err
	}
	if err = checkRedisExpiryIndexesTx(tx, ns, directory, report); err != nil {
		return err
	}
	if err = checkRedisSlotIndexTx(tx, ns, directory, report); err != nil {
		return err
	}
	if err = checkRedisMaintenanceStateTx(tx, ns, report); err != nil {
		return err
	}
	return nil
}

func checkRedisKeyIntegrityTx(tx *storage.Tx, ns redisNamespace, key []byte, meta redisKeyMeta, report *RedisIntegrityReport) {
	keyType, err := redisKeyTypeFromMetaCode(meta.Type)
	if err != nil {
		report.add(ns.dbIndex, key, "%v", err)
		return
	}
	deadline, hasTTL, expiryErr := loadRedisExpireAtMsTx(tx, ns, key)
	if expiryErr != nil {
		report.add(ns.dbIndex, key, "load expiry: %v", expiryErr)
	} else if (meta.ExpireAtMs >= 0) != hasTTL || (hasTTL && deadline != meta.ExpireAtMs) {
		report.add(ns.dbIndex, key, "key-directory and expiry metadata disagree")
	}
	slotBucket, slotErr := optionalRedisBucket(tx, ns.slotIndexBucket)
	if slotErr != nil {
		report.add(ns.dbIndex, key, "load slot index: %v", slotErr)
	} else if slotBucket == nil {
		report.add(ns.dbIndex, key, "slot index bucket is missing")
	} else if _, found := slotBucket.Get(redisSlotIndexKey(ClusterKeySlot(key, ns.slotCount), key)); !found {
		report.add(ns.dbIndex, key, "slot index entry is missing")
	}

	switch keyType {
	case redisKeyTypeString:
		bucket, bucketErr := optionalRedisBucket(tx, ns.stringBucket)
		if bucketErr != nil || bucket == nil {
			report.add(ns.dbIndex, key, "string bucket is missing: %v", bucketErr)
		} else if _, found, valueErr := bucket.ValueLen(key); valueErr != nil || !found {
			report.add(ns.dbIndex, key, "string value is missing: %v", valueErr)
		}
	case redisKeyTypeList:
		checkRedisListIntegrityTx(tx, ns, key, meta, report)
	case redisKeyTypeHash:
		checkRedisHashIntegrityTx(tx, ns, key, meta, report)
	case redisKeyTypeZSet:
		checkRedisZSetIntegrityTx(tx, ns, key, meta, report)
	case redisKeyTypeBloom:
		checkRedisBloomIntegrityTx(tx, ns, key, meta, report)
	case redisKeyTypeTopK:
		checkRedisTopKIntegrityTx(tx, ns, key, meta, report)
	}
}

func rawRedisObjectMeta(tx *storage.Tx, bucketName, key []byte) ([]byte, bool, error) {
	bucket, err := optionalRedisBucket(tx, bucketName)
	if err != nil || bucket == nil {
		return nil, false, err
	}
	raw, found := bucket.Get(key)
	return raw, found, nil
}

func checkRedisListIntegrityTx(tx *storage.Tx, ns redisNamespace, key []byte, keyMeta redisKeyMeta, report *RedisIntegrityReport) {
	raw, found, err := rawRedisObjectMeta(tx, ns.listMetaBucket, key)
	if err != nil || !found {
		report.add(ns.dbIndex, key, "list metadata is missing: %v", err)
		return
	}
	meta, err := deserializeRedisListMeta(raw)
	if err != nil || meta.ID != keyMeta.ObjectID {
		report.add(ns.dbIndex, key, "invalid list metadata or object ID: %v", err)
		return
	}
	meta.StorageFormat = keyMeta.StorageFormat
	store, err := openRedisObjectBucketTx(tx, ns.listDataBucket, redisListBucketName(ns, meta.ID), meta.ID, meta.StorageFormat, false)
	if err != nil {
		report.add(ns.dbIndex, key, "list object is missing: %v", err)
		return
	}
	visited := make(map[uint64]struct{})
	segmentID := meta.HeadSegID
	previous := uint64(0)
	var length uint64
	for segmentID != 0 {
		if _, exists := visited[segmentID]; exists {
			report.add(ns.dbIndex, key, "list segment cycle at %d", segmentID)
			return
		}
		visited[segmentID] = struct{}{}
		rawSegment, exists := store.Get(redisListSegmentKey(segmentID))
		if !exists {
			report.add(ns.dbIndex, key, "list segment %d is missing", segmentID)
			return
		}
		segment, decodeErr := deserializeRedisListSegment(rawSegment)
		if decodeErr != nil {
			report.add(ns.dbIndex, key, "list segment %d: %v", segmentID, decodeErr)
			return
		}
		if segment.PrevID != previous {
			report.add(ns.dbIndex, key, "list segment %d has non-reciprocal previous link", segmentID)
		}
		if len(segment.Values) == 0 {
			report.add(ns.dbIndex, key, "list segment %d is empty", segmentID)
		}
		length += uint64(len(segment.Values))
		previous = segmentID
		segmentID = segment.NextID
	}
	if length != meta.Length || (meta.Length > 0 && (meta.HeadSegID == 0 || previous != meta.TailSegID)) {
		report.add(ns.dbIndex, key, "list length/head/tail metadata does not match its chain")
	}
	if meta.Length == 0 {
		report.add(ns.dbIndex, key, "empty list has persistent metadata")
	}
	storedSegments, countErr := countRedisObjectEntriesChecked(store)
	if countErr != nil {
		report.add(ns.dbIndex, key, "scan list object: %v", countErr)
	} else if storedSegments != len(visited) {
		report.add(ns.dbIndex, key, "list object contains %d unreachable segment(s)", storedSegments-len(visited))
	}
}

func checkRedisHashIntegrityTx(tx *storage.Tx, ns redisNamespace, key []byte, keyMeta redisKeyMeta, report *RedisIntegrityReport) {
	raw, found, err := rawRedisObjectMeta(tx, ns.hashMetaBucket, key)
	if err != nil || !found {
		report.add(ns.dbIndex, key, "hash metadata is missing: %v", err)
		return
	}
	meta, err := deserializeRedisHashMeta(raw)
	if err != nil || meta.ID != keyMeta.ObjectID {
		report.add(ns.dbIndex, key, "invalid hash metadata or object ID: %v", err)
		return
	}
	meta.StorageFormat = keyMeta.StorageFormat
	store, err := openRedisObjectBucketTx(tx, ns.hashDataBucket, redisHashBucketName(ns, meta.ID), meta.ID, meta.StorageFormat, false)
	if err != nil {
		report.add(ns.dbIndex, key, "hash object is missing: %v", err)
		return
	}
	count, countErr := countRedisObjectEntriesChecked(store)
	if countErr != nil {
		report.add(ns.dbIndex, key, "scan hash object: %v", countErr)
	} else if count != int(meta.FieldCount) {
		report.add(ns.dbIndex, key, "hash field count does not match stored records")
	}
}

func checkRedisZSetIntegrityTx(tx *storage.Tx, ns redisNamespace, key []byte, keyMeta redisKeyMeta, report *RedisIntegrityReport) {
	raw, found, err := rawRedisObjectMeta(tx, ns.zsetMetaBucket, key)
	if err != nil || !found {
		report.add(ns.dbIndex, key, "sorted-set metadata is missing: %v", err)
		return
	}
	meta, err := deserializeRedisZSetMeta(raw)
	if err != nil || meta.ID != keyMeta.ObjectID {
		report.add(ns.dbIndex, key, "invalid sorted-set metadata or object ID: %v", err)
		return
	}
	memberStore, err := openRedisObjectBucketTx(tx, ns.zsetMemberBucket, redisZSetMemberBucketName(ns, meta.ID), meta.ID, keyMeta.StorageFormat, false)
	if err != nil {
		report.add(ns.dbIndex, key, "sorted-set member index is missing: %v", err)
		return
	}
	scoreStore, err := openRedisObjectBucketTx(tx, ns.zsetScoreBucket, redisZSetScoreBucketName(ns, meta.ID), meta.ID, keyMeta.StorageFormat, false)
	if err != nil {
		report.add(ns.dbIndex, key, "sorted-set score index is missing: %v", err)
		return
	}
	members, memberCountErr := countRedisObjectEntriesChecked(memberStore)
	scores, scoreCountErr := countRedisObjectEntriesChecked(scoreStore)
	if memberCountErr != nil {
		report.add(ns.dbIndex, key, "scan sorted-set member index: %v", memberCountErr)
	}
	if scoreCountErr != nil {
		report.add(ns.dbIndex, key, "scan sorted-set score index: %v", scoreCountErr)
	}
	if members != scores || members != int(meta.Cardinality) {
		report.add(ns.dbIndex, key, "sorted-set cardinality or index counts disagree")
	}
	if err = memberStore.ForEach(func(member, encodedScore []byte) error {
		if _, exists := scoreStore.Get(redisZSetScoreIndexKeyFromEncoded(encodedScore, member)); !exists {
			report.add(ns.dbIndex, key, "sorted-set score index is missing member %q", member)
		}
		return nil
	}); err != nil {
		report.add(ns.dbIndex, key, "scan sorted-set member index: %v", err)
	}
	if err = scoreStore.ForEach(func(scoreKey, _ []byte) error {
		if len(scoreKey) < redisZSetIndexPrefixSize {
			report.add(ns.dbIndex, key, "sorted-set score key is malformed")
			return nil
		}
		member := scoreKey[redisZSetIndexPrefixSize:]
		encoded, exists := memberStore.Get(member)
		if !exists || !bytes.Equal(encoded, scoreKey[:redisZSetSortableSize]) {
			report.add(ns.dbIndex, key, "sorted-set member and score indexes are not inverse")
		}
		return nil
	}); err != nil {
		report.add(ns.dbIndex, key, "scan sorted-set score index: %v", err)
	}
}

func checkRedisBloomIntegrityTx(tx *storage.Tx, ns redisNamespace, key []byte, keyMeta redisKeyMeta, report *RedisIntegrityReport) {
	raw, found, err := rawRedisObjectMeta(tx, ns.bloomMetaBucket, key)
	if err != nil || !found {
		report.add(ns.dbIndex, key, "Bloom metadata is missing: %v", err)
		return
	}
	meta, err := deserializeRedisBloomMeta(raw)
	if err != nil || meta.ID != keyMeta.ObjectID || meta.SubFilterCount == 0 {
		report.add(ns.dbIndex, key, "invalid Bloom metadata or object ID: %v", err)
		return
	}
	meta.StorageFormat = keyMeta.StorageFormat
	store, err := getRedisBloomDataBucketTx(tx, ns, meta)
	if err != nil {
		report.add(ns.dbIndex, key, "Bloom object is missing: %v", err)
		return
	}
	for idx := uint64(0); idx < meta.SubFilterCount; idx++ {
		sub, exists, loadErr := loadRedisBloomSubFilterMeta(store, idx)
		if loadErr != nil || !exists || sub.Capacity == 0 || sub.InsertedCount > sub.Capacity {
			report.add(ns.dbIndex, key, "Bloom sub-filter %d is invalid: %v", idx, loadErr)
		}
	}
}

func checkRedisTopKIntegrityTx(tx *storage.Tx, ns redisNamespace, key []byte, keyMeta redisKeyMeta, report *RedisIntegrityReport) {
	raw, found, err := rawRedisObjectMeta(tx, ns.topkMetaBucket, key)
	if err != nil || !found {
		report.add(ns.dbIndex, key, "TopK metadata is missing: %v", err)
		return
	}
	meta, err := deserializeRedisTopKMeta(raw)
	if err != nil || meta.ID != keyMeta.ObjectID || meta.HeapSize > meta.K {
		report.add(ns.dbIndex, key, "invalid TopK metadata or object ID: %v", err)
		return
	}
	meta.StorageFormat = keyMeta.StorageFormat
	store, err := getRedisTopKDataBucketTx(tx, ns, meta)
	if err != nil {
		report.add(ns.dbIndex, key, "TopK object is missing: %v", err)
		return
	}
	if _, err = loadRedisTopKHeap(store, meta.HeapSize); err != nil {
		report.add(ns.dbIndex, key, "TopK heap is invalid: %v", err)
	}
}

func checkRedisTypeOwnershipTx(tx *storage.Tx, ns redisNamespace, directory map[string]redisKeyMeta, report *RedisIntegrityReport) error {
	stringsBucket, err := optionalRedisBucket(tx, ns.stringBucket)
	if err != nil {
		return err
	}
	if stringsBucket != nil {
		cursor := stringsBucket.Cursor()
		for key, _ := cursor.First(); key != nil; key, _ = cursor.Next() {
			meta, exists := directory[string(key)]
			if !exists || meta.Type != redisKeyMetaTypeString {
				report.add(ns.dbIndex, key, "string value has no matching key-directory owner")
			}
		}
		if err = cursor.Err(); err != nil {
			return err
		}
	}
	sources := []struct {
		typeCode byte
		bucket   []byte
	}{
		{redisKeyMetaTypeList, ns.listMetaBucket}, {redisKeyMetaTypeHash, ns.hashMetaBucket}, {redisKeyMetaTypeZSet, ns.zsetMetaBucket},
		{redisKeyMetaTypeBloom, ns.bloomMetaBucket}, {redisKeyMetaTypeTopK, ns.topkMetaBucket},
	}
	for _, source := range sources {
		bucket, err := optionalRedisBucket(tx, source.bucket)
		if err != nil {
			return err
		}
		if bucket == nil {
			continue
		}
		cursor := bucket.Cursor()
		for key, _ := cursor.First(); key != nil; key, _ = cursor.Next() {
			meta, exists := directory[string(key)]
			if !exists || meta.Type != source.typeCode {
				report.add(ns.dbIndex, key, "type metadata has no matching key-directory owner")
			}
		}
		if err = cursor.Err(); err != nil {
			return err
		}
	}
	return nil
}

func checkRedisExpiryIndexesTx(tx *storage.Tx, ns redisNamespace, directory map[string]redisKeyMeta, report *RedisIntegrityReport) error {
	index, err := optionalRedisBucket(tx, ns.expireIndexBucket)
	if err != nil {
		return err
	}
	metadata, err := optionalRedisBucket(tx, ns.expireMetaBucket)
	if err != nil {
		return err
	}
	if metadata != nil {
		cursor := metadata.Cursor()
		for key, raw := cursor.First(); key != nil; key, raw = cursor.Next() {
			deadline, decodeErr := decodeRedisExpireAtMs(raw)
			if decodeErr != nil {
				report.add(ns.dbIndex, key, "%v", decodeErr)
				continue
			}
			meta, exists := directory[string(key)]
			if !exists || meta.ExpireAtMs != deadline {
				report.add(ns.dbIndex, key, "expiry metadata has no matching key-directory deadline")
			}
			if index == nil {
				report.add(ns.dbIndex, key, "expiry index bucket is missing")
			} else if _, found := index.Get(redisExpireIndexKey(deadline, key)); !found {
				report.add(ns.dbIndex, key, "expiry index entry is missing")
			}
		}
		if err = cursor.Err(); err != nil {
			return err
		}
	}
	if index == nil {
		return nil
	}
	cursor := index.Cursor()
	for indexKey, _ := cursor.First(); indexKey != nil; indexKey, _ = cursor.Next() {
		deadline, key, decodeErr := decodeRedisExpireIndexKey(indexKey)
		if decodeErr != nil {
			report.add(ns.dbIndex, nil, "%v", decodeErr)
			continue
		}
		meta, exists := directory[string(key)]
		if !exists || meta.ExpireAtMs != deadline {
			report.add(ns.dbIndex, key, "expiry index has no matching key-directory deadline")
		}
	}
	return cursor.Err()
}

func checkRedisSlotIndexTx(tx *storage.Tx, ns redisNamespace, directory map[string]redisKeyMeta, report *RedisIntegrityReport) error {
	index, err := optionalRedisBucket(tx, ns.slotIndexBucket)
	if err != nil || index == nil {
		return err
	}
	cursor := index.Cursor()
	for indexKey, _ := cursor.First(); indexKey != nil; indexKey, _ = cursor.Next() {
		slot, key, decodeErr := decodeRedisSlotIndexKey(indexKey)
		if decodeErr != nil {
			return decodeErr
		}
		if _, exists := directory[string(key)]; !exists || slot != ClusterKeySlot(key, ns.slotCount) {
			report.add(ns.dbIndex, key, "slot index has no matching active key")
		}
	}
	return cursor.Err()
}

func checkRedisMaintenanceStateTx(tx *storage.Tx, ns redisNamespace, report *RedisIntegrityReport) error {
	queue, err := optionalRedisBucket(tx, ns.gcQueueBucket)
	if err != nil {
		return err
	}
	if queue != nil {
		cursor := queue.Cursor()
		for taskKey, raw := cursor.First(); taskKey != nil; taskKey, raw = cursor.Next() {
			if _, decodeErr := deserializeRedisGCTask(raw); decodeErr != nil {
				report.add(ns.dbIndex, nil, "garbage task %x is invalid: %v", taskKey, decodeErr)
			}
		}
		if err = cursor.Err(); err != nil {
			return err
		}
	}
	builds, err := optionalRedisBucket(tx, ns.buildStateBucket)
	if err != nil {
		return err
	}
	if builds != nil {
		cursor := builds.Cursor()
		for key, raw := cursor.First(); key != nil; key, raw = cursor.Next() {
			if _, decodeErr := deserializeRedisBuildState(raw); decodeErr != nil {
				report.add(ns.dbIndex, key, "hidden-build state is invalid: %v", decodeErr)
			}
		}
		if err = cursor.Err(); err != nil {
			return err
		}
	}
	state, err := loadRedisObjectMigrationStateTx(tx, ns)
	if err != nil {
		report.add(ns.dbIndex, nil, "object migration state is invalid: %v", err)
		return nil
	}
	if state.Phase == redisObjectMigrationPhaseCopying && (len(state.Key) == 0 || state.SourceID == 0 || state.DestID == 0) {
		report.add(ns.dbIndex, nil, "object migration state has incomplete object identity")
	}
	return nil
}
