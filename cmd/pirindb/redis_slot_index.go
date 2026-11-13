package main

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/timson/pirindb/storage"
)

const (
	redisSlotIndexPrefixSize = 4
)

var (
	errRedisSlotIterationStopBefore = errors.New("stop redis slot iteration before current key")
	errRedisSlotIterationStopAfter  = errors.New("stop redis slot iteration after current key")
)

func redisSlotIndexKey(slot int, key []byte) []byte {
	buf := make([]byte, redisSlotIndexPrefixSize+len(key))
	binary.BigEndian.PutUint32(buf[:redisSlotIndexPrefixSize], uint32(slot))
	copy(buf[redisSlotIndexPrefixSize:], key)
	return buf
}

func redisSlotIndexPrefix(slot int) []byte {
	buf := make([]byte, redisSlotIndexPrefixSize)
	binary.BigEndian.PutUint32(buf, uint32(slot))
	return buf
}

func decodeRedisSlotIndexKey(indexKey []byte) (int, []byte, error) {
	if len(indexKey) < redisSlotIndexPrefixSize {
		return 0, nil, errors.New("corrupted redis slot index")
	}
	slot := int(binary.BigEndian.Uint32(indexKey[:redisSlotIndexPrefixSize]))
	return slot, cloneBytes(indexKey[redisSlotIndexPrefixSize:]), nil
}

func ensureRedisSlotIndexEntryTx(tx *storage.Tx, ns redisNamespace, key []byte) error {
	bucket, err := tx.CreateBucketIfNotExists(ns.slotIndexBucket)
	if err != nil {
		return err
	}
	slot := ClusterKeySlot(key, ns.slotCount)
	indexKey := redisSlotIndexKey(slot, key)
	if _, found := bucket.Get(indexKey); found {
		return nil
	}
	return bucket.Put(indexKey, []byte{})
}

func deleteRedisSlotIndexEntryTx(tx *storage.Tx, ns redisNamespace, key []byte) error {
	bucket, err := tx.GetBucket(ns.slotIndexBucket)
	if errors.Is(err, storage.ErrBucketNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	slot := ClusterKeySlot(key, ns.slotCount)
	err = bucket.Remove(redisSlotIndexKey(slot, key))
	if errors.Is(err, storage.ErrNodeNotFound) {
		return nil
	}
	return err
}

func listRedisKeysBySlotRangeTx(tx *storage.Tx, ns redisNamespace, startSlot int, endSlot int, nowMs int64) ([][]byte, error) {
	keys := make([][]byte, 0)
	_, _, _, err := forEachRedisKeyBySlotRangeFromIndexKeyTx(tx, ns, startSlot, endSlot, nowMs, nil, 0, func(key []byte) error {
		keys = append(keys, key)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return keys, nil
}

func forEachRedisKeyBySlotRangeFromIndexKeyTx(tx *storage.Tx, ns redisNamespace, startSlot int, endSlot int, nowMs int64, resumeAfter []byte, limit int, fn func([]byte) error) (uint64, []byte, bool, error) {
	if ns.slotCount <= 0 || startSlot < 0 || endSlot < startSlot || endSlot >= ns.slotCount {
		return 0, nil, false, errors.New("slot range is invalid")
	}

	bucket, err := tx.GetBucket(ns.slotIndexBucket)
	if errors.Is(err, storage.ErrBucketNotFound) {
		return 0, nil, true, nil
	}
	if err != nil {
		return 0, nil, false, err
	}

	cursor := bucket.Cursor()
	startKey := redisSlotIndexPrefix(startSlot)
	if len(resumeAfter) > 0 {
		if len(resumeAfter) >= redisSlotIndexPrefixSize {
			resumeSlot := int(binary.BigEndian.Uint32(resumeAfter[:redisSlotIndexPrefixSize]))
			if resumeSlot >= startSlot {
				startKey = redisSlotIndexSuccessorSeekKey(resumeAfter)
			}
		}
	}
	var endExclusive []byte
	if endSlot < int(^uint32(0)) {
		endExclusive = redisSlotIndexPrefix(endSlot + 1)
	}

	var yielded uint64
	var lastIndexKey []byte
	exhausted := true
	for indexKey, _ := cursor.Seek(startKey); indexKey != nil; indexKey, _ = cursor.Next() {
		if len(indexKey) < redisSlotIndexPrefixSize {
			return 0, nil, false, errors.New("corrupted redis slot index")
		}
		if endExclusive != nil && bytes.Compare(indexKey[:redisSlotIndexPrefixSize], endExclusive) >= 0 {
			break
		}
		slot, key, decodeErr := decodeRedisSlotIndexKey(indexKey)
		if decodeErr != nil {
			return 0, nil, false, decodeErr
		}
		if slot > endSlot {
			break
		}
		if slot < startSlot {
			continue
		}
		keyType, typeErr := redisKeyTypeTx(tx, ns, key, nowMs)
		if typeErr != nil {
			return 0, nil, false, typeErr
		}
		if keyType == redisKeyTypeNone {
			continue
		}
		if limit > 0 && int(yielded) >= limit {
			exhausted = false
			break
		}
		if err := fn(key); err != nil {
			if errors.Is(err, errRedisSlotIterationStopBefore) {
				exhausted = false
				break
			}
			if errors.Is(err, errRedisSlotIterationStopAfter) {
				yielded++
				lastIndexKey = cloneBytes(indexKey)
				exhausted = false
				break
			}
			return yielded, cloneBytes(lastIndexKey), false, err
		}
		yielded++
		lastIndexKey = cloneBytes(indexKey)
	}

	return yielded, lastIndexKey, exhausted, nil
}

func redisSlotIndexSuccessorSeekKey(indexKey []byte) []byte {
	seek := make([]byte, len(indexKey)+1)
	copy(seek, indexKey)
	return seek
}
