package main

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/timson/pirindb/storage"
)

var (
	redisListNextIDKey = []byte("next_id")
	errRedisWrongType  = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	errRedisNoSuchKey  = errors.New("no such key")
	errRedisBadIndex   = errors.New("index out of range")
)

const (
	redisListMetaSize                 = 32
	redisListSegmentHeaderSize        = 20
	redisListMaxSegmentEntries        = 64
	redisListTargetSegmentPayloadSize = 8 * 1024
)

type redisListMeta struct {
	ID        uint64
	Length    uint64
	HeadSegID uint64
	TailSegID uint64
}

type redisListSegment struct {
	PrevID       uint64
	NextID       uint64
	Values       [][]byte
	payloadBytes int
}

func (meta *redisListMeta) Len() int64 {
	return int64(meta.Length)
}

func (meta *redisListMeta) serialize() []byte {
	buf := make([]byte, redisListMetaSize)
	binary.BigEndian.PutUint64(buf[0:8], meta.ID)
	binary.BigEndian.PutUint64(buf[8:16], meta.Length)
	binary.BigEndian.PutUint64(buf[16:24], meta.HeadSegID)
	binary.BigEndian.PutUint64(buf[24:32], meta.TailSegID)
	return buf
}

func deserializeRedisListMeta(buf []byte) (*redisListMeta, error) {
	if len(buf) != redisListMetaSize {
		return nil, errors.New("corrupted redis list metadata")
	}
	return &redisListMeta{
		ID:        binary.BigEndian.Uint64(buf[0:8]),
		Length:    binary.BigEndian.Uint64(buf[8:16]),
		HeadSegID: binary.BigEndian.Uint64(buf[16:24]),
		TailSegID: binary.BigEndian.Uint64(buf[24:32]),
	}, nil
}

func (segment *redisListSegment) canFit(value []byte) bool {
	if len(segment.Values) >= redisListMaxSegmentEntries {
		return false
	}
	if len(segment.Values) == 0 {
		return true
	}
	return segment.payloadBytes+len(value)+storage.UInt32Size <= redisListTargetSegmentPayloadSize
}

func (segment *redisListSegment) appendValue(value []byte) {
	segment.Values = append(segment.Values, cloneBytes(value))
	segment.payloadBytes += storage.UInt32Size + len(value)
}

func (segment *redisListSegment) prependValue(value []byte) {
	cloned := cloneBytes(value)
	segment.Values = append(segment.Values, nil)
	copy(segment.Values[1:], segment.Values[:len(segment.Values)-1])
	segment.Values[0] = cloned
	segment.payloadBytes += storage.UInt32Size + len(value)
}

func (segment *redisListSegment) popLeft() ([]byte, bool) {
	if len(segment.Values) == 0 {
		return nil, false
	}
	value := segment.Values[0]
	segment.Values = segment.Values[1:]
	segment.payloadBytes -= storage.UInt32Size + len(value)
	return value, true
}

func (segment *redisListSegment) popRight() ([]byte, bool) {
	if len(segment.Values) == 0 {
		return nil, false
	}
	last := len(segment.Values) - 1
	value := segment.Values[last]
	segment.Values = segment.Values[:last]
	segment.payloadBytes -= storage.UInt32Size + len(value)
	return value, true
}

func (segment *redisListSegment) serialize() []byte {
	buf := make([]byte, redisListSegmentHeaderSize+segment.payloadBytes)
	binary.BigEndian.PutUint64(buf[0:8], segment.PrevID)
	binary.BigEndian.PutUint64(buf[8:16], segment.NextID)
	binary.BigEndian.PutUint32(buf[16:20], uint32(len(segment.Values)))
	pos := redisListSegmentHeaderSize
	for _, value := range segment.Values {
		binary.BigEndian.PutUint32(buf[pos:pos+4], uint32(len(value)))
		pos += 4
		copy(buf[pos:], value)
		pos += len(value)
	}
	return buf
}

func deserializeRedisListSegment(buf []byte) (*redisListSegment, error) {
	if len(buf) < redisListSegmentHeaderSize {
		return nil, errors.New("corrupted redis list segment")
	}

	segment := &redisListSegment{
		PrevID: binary.BigEndian.Uint64(buf[0:8]),
		NextID: binary.BigEndian.Uint64(buf[8:16]),
	}

	count := int(binary.BigEndian.Uint32(buf[16:20]))
	segment.Values = make([][]byte, 0, count)

	pos := redisListSegmentHeaderSize
	for i := 0; i < count; i++ {
		if pos+4 > len(buf) {
			return nil, errors.New("corrupted redis list segment")
		}
		valueLen := int(binary.BigEndian.Uint32(buf[pos : pos+4]))
		pos += 4
		if valueLen < 0 || pos+valueLen > len(buf) {
			return nil, errors.New("corrupted redis list segment")
		}
		segment.Values = append(segment.Values, cloneBytes(buf[pos:pos+valueLen]))
		segment.payloadBytes += storage.UInt32Size + valueLen
		pos += valueLen
	}

	if pos != len(buf) {
		return nil, errors.New("corrupted redis list segment")
	}

	return segment, nil
}

func redisListBucketName(ns redisNamespace, id uint64) []byte {
	name := make([]byte, len(ns.listDataPrefix)+8)
	copy(name, ns.listDataPrefix)
	binary.BigEndian.PutUint64(name[len(ns.listDataPrefix):], id)
	return name
}

func redisListSegmentKey(id uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, id)
	return buf
}

func redisStringKeyExistsTx(tx *storage.Tx, ns redisNamespace, key []byte) (bool, error) {
	bucket, err := tx.GetBucket(ns.stringBucket)
	if errors.Is(err, storage.ErrBucketNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	_, found := bucket.Get(key)
	return found, nil
}

func loadRedisListMetaTx(tx *storage.Tx, ns redisNamespace, key []byte) (*redisListMeta, bool, error) {
	bucket, err := tx.GetBucket(ns.listMetaBucket)
	if errors.Is(err, storage.ErrBucketNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}

	raw, found := bucket.Get(key)
	if !found {
		return nil, false, nil
	}

	meta, err := deserializeRedisListMeta(raw)
	if err != nil {
		return nil, false, err
	}
	return meta, true, nil
}

func saveRedisListMetaTx(tx *storage.Tx, ns redisNamespace, key []byte, meta *redisListMeta) error {
	bucket, err := tx.CreateBucketIfNotExists(ns.listMetaBucket)
	if err != nil {
		return err
	}
	if err = bucket.Put(key, meta.serialize()); err != nil {
		return err
	}
	return ensureRedisSlotIndexEntryTx(tx, ns, key)
}

func deleteRedisListMetaTx(tx *storage.Tx, ns redisNamespace, key []byte) error {
	bucket, err := tx.GetBucket(ns.listMetaBucket)
	if errors.Is(err, storage.ErrBucketNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	err = bucket.Remove(key)
	if errors.Is(err, storage.ErrNodeNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	return deleteRedisSlotIndexEntryTx(tx, ns, key)
}

func nextRedisListIDTx(tx *storage.Tx, ns redisNamespace) (uint64, error) {
	bucket, err := tx.CreateBucketIfNotExists(ns.listSysBucket)
	if err != nil {
		return 0, err
	}

	raw, found := bucket.Get(redisListNextIDKey)
	if !found {
		initial := make([]byte, 8)
		binary.BigEndian.PutUint64(initial, 2)
		if err = bucket.Put(redisListNextIDKey, initial); err != nil {
			return 0, err
		}
		return 1, nil
	}

	if len(raw) != 8 {
		return 0, errors.New("corrupted redis list id counter")
	}

	id := binary.BigEndian.Uint64(raw)
	next := make([]byte, 8)
	binary.BigEndian.PutUint64(next, id+1)
	if err = bucket.Put(redisListNextIDKey, next); err != nil {
		return 0, err
	}
	return id, nil
}

func ensureRedisListDataBucketTx(tx *storage.Tx, ns redisNamespace, meta *redisListMeta) (*storage.Bucket, error) {
	return tx.CreateBucketIfNotExists(redisListBucketName(ns, meta.ID))
}

func loadRedisListSegmentTx(tx *storage.Tx, ns redisNamespace, meta *redisListMeta, segmentID uint64) (*redisListSegment, error) {
	bucket, err := tx.GetBucket(redisListBucketName(ns, meta.ID))
	if err != nil {
		return nil, err
	}
	raw, found := bucket.Get(redisListSegmentKey(segmentID))
	if !found {
		return nil, errors.New("corrupted redis list segment")
	}
	return deserializeRedisListSegment(raw)
}

func saveRedisListSegmentTx(tx *storage.Tx, ns redisNamespace, meta *redisListMeta, segmentID uint64, segment *redisListSegment) error {
	bucket, err := ensureRedisListDataBucketTx(tx, ns, meta)
	if err != nil {
		return err
	}
	return bucket.Put(redisListSegmentKey(segmentID), segment.serialize())
}

func deleteRedisListSegmentTx(tx *storage.Tx, ns redisNamespace, meta *redisListMeta, segmentID uint64) error {
	bucket, err := tx.GetBucket(redisListBucketName(ns, meta.ID))
	if err != nil {
		return err
	}
	err = bucket.Remove(redisListSegmentKey(segmentID))
	if errors.Is(err, storage.ErrNodeNotFound) {
		return nil
	}
	return err
}

func nextRedisListSegmentIDTx(tx *storage.Tx, ns redisNamespace, meta *redisListMeta) (uint64, error) {
	bucket, err := ensureRedisListDataBucketTx(tx, ns, meta)
	if err != nil {
		return 0, err
	}
	return bucket.NextSequence()
}

func deleteRedisListTx(tx *storage.Tx, ns redisNamespace, key []byte, meta *redisListMeta) error {
	err := tx.DeleteBucket(redisListBucketName(ns, meta.ID))
	if err != nil && !errors.Is(err, storage.ErrBucketNotFound) {
		return err
	}
	if err = deleteRedisListMetaTx(tx, ns, key); err != nil {
		return err
	}
	return deleteRedisExpireAtMsTx(tx, ns, key)
}

func deleteRedisListIfExistsTx(tx *storage.Tx, ns redisNamespace, key []byte) (bool, error) {
	meta, found, err := loadRedisListMetaTx(tx, ns, key)
	if err != nil || !found {
		return false, err
	}
	return true, deleteRedisListTx(tx, ns, key, meta)
}

func initRedisListMetaTx(tx *storage.Tx, ns redisNamespace, key []byte, nowMs int64) (*redisListMeta, error) {
	keyType, err := redisKeyTypeTx(tx, ns, key, nowMs)
	if err != nil {
		return nil, err
	}
	if keyType == redisKeyTypeString || keyType == redisKeyTypeHash || keyType == redisKeyTypeZSet || keyType == redisKeyTypeBloom || keyType == redisKeyTypeTopK {
		return nil, errRedisWrongType
	}

	id, err := nextRedisListIDTx(tx, ns)
	if err != nil {
		return nil, err
	}

	meta := &redisListMeta{ID: id}
	if _, err = ensureRedisListDataBucketTx(tx, ns, meta); err != nil {
		return nil, err
	}
	return meta, nil
}

func loadRedisListMetaForReadTx(tx *storage.Tx, ns redisNamespace, key []byte, nowMs int64) (*redisListMeta, bool, error) {
	meta, found, err := loadRedisListMetaTx(tx, ns, key)
	if err != nil || found {
		if err != nil || !found {
			return meta, found, err
		}
		expired, err := redisKeyExpiredTx(tx, ns, key, nowMs)
		if err != nil {
			return nil, false, err
		}
		if expired {
			return nil, false, nil
		}
		return meta, true, nil
	}

	keyType, err := redisKeyTypeTx(tx, ns, key, nowMs)
	if err != nil {
		return nil, false, err
	}
	if keyType == redisKeyTypeString || keyType == redisKeyTypeHash || keyType == redisKeyTypeZSet || keyType == redisKeyTypeBloom || keyType == redisKeyTypeTopK {
		return nil, false, errRedisWrongType
	}
	return nil, false, nil
}

func normalizeRedisListIndex(length int64, index int64) (int64, bool) {
	if index < 0 {
		index += length
	}
	if index < 0 || index >= length {
		return 0, false
	}
	return index, true
}

func normalizeRedisListRange(length int64, start int64, stop int64) (int64, int64, bool) {
	if length <= 0 {
		return 0, 0, false
	}
	if start < 0 {
		start += length
	}
	if stop < 0 {
		stop += length
	}
	if start < 0 {
		start = 0
	}
	if stop < 0 {
		return 0, 0, false
	}
	if start >= length {
		return 0, 0, false
	}
	if stop >= length {
		stop = length - 1
	}
	if start > stop {
		return 0, 0, false
	}
	return start, stop, true
}

func readRedisListValuesTx(tx *storage.Tx, ns redisNamespace, meta *redisListMeta) ([][]byte, []uint64, error) {
	if meta.Length == 0 {
		return nil, nil, nil
	}

	values := make([][]byte, 0, meta.Length)
	segmentIDs := make([]uint64, 0)
	for segmentID := meta.HeadSegID; segmentID != 0; {
		segment, err := loadRedisListSegmentTx(tx, ns, meta, segmentID)
		if err != nil {
			return nil, nil, err
		}
		segmentIDs = append(segmentIDs, segmentID)
		for _, value := range segment.Values {
			values = append(values, cloneBytes(value))
		}
		segmentID = segment.NextID
	}

	if int64(len(values)) != meta.Len() {
		return nil, nil, errors.New("corrupted redis list segment")
	}
	return values, segmentIDs, nil
}

func rewriteRedisListValuesTx(tx *storage.Tx, ns redisNamespace, key []byte, meta *redisListMeta, segmentIDs []uint64, values [][]byte) error {
	if len(values) == 0 {
		return deleteRedisListTx(tx, ns, key, meta)
	}

	bucket, err := ensureRedisListDataBucketTx(tx, ns, meta)
	if err != nil {
		return err
	}
	for _, segmentID := range segmentIDs {
		err = bucket.Remove(redisListSegmentKey(segmentID))
		if err != nil && !errors.Is(err, storage.ErrNodeNotFound) {
			return err
		}
	}

	meta.Length = uint64(len(values))
	meta.HeadSegID = 0
	meta.TailSegID = 0

	var (
		segment   *redisListSegment
		segmentID uint64
	)
	for _, value := range values {
		if segment == nil {
			segmentID, err = bucket.NextSequence()
			if err != nil {
				return err
			}
			segment = &redisListSegment{}
			meta.HeadSegID = segmentID
			meta.TailSegID = segmentID
		} else if !segment.canFit(value) {
			nextSegmentID, nextErr := bucket.NextSequence()
			if nextErr != nil {
				return nextErr
			}
			segment.NextID = nextSegmentID
			if err = bucket.Put(redisListSegmentKey(segmentID), segment.serialize()); err != nil {
				return err
			}
			nextSegment := &redisListSegment{PrevID: segmentID}
			segmentID = nextSegmentID
			segment = nextSegment
			meta.TailSegID = segmentID
		}
		segment.appendValue(value)
	}

	if segment == nil {
		return deleteRedisListTx(tx, ns, key, meta)
	}
	if err = bucket.Put(redisListSegmentKey(segmentID), segment.serialize()); err != nil {
		return err
	}
	return saveRedisListMetaTx(tx, ns, key, meta)
}

func redisListLenTx(tx *storage.Tx, ns redisNamespace, key []byte, nowMs int64) (int64, error) {
	meta, found, err := loadRedisListMetaForReadTx(tx, ns, key, nowMs)
	if err != nil || !found {
		return 0, err
	}
	return meta.Len(), nil
}

func redisListIndexTx(tx *storage.Tx, ns redisNamespace, key []byte, index int64, nowMs int64) ([]byte, bool, error) {
	meta, found, err := loadRedisListMetaForReadTx(tx, ns, key, nowMs)
	if err != nil || !found {
		return nil, false, err
	}

	index, ok := normalizeRedisListIndex(meta.Len(), index)
	if !ok {
		return nil, false, nil
	}

	values, _, err := readRedisListValuesTx(tx, ns, meta)
	if err != nil {
		return nil, false, err
	}
	return cloneBytes(values[int(index)]), true, nil
}

func redisListRangeTx(tx *storage.Tx, ns redisNamespace, key []byte, start int64, stop int64, nowMs int64) ([][]byte, error) {
	meta, found, err := loadRedisListMetaForReadTx(tx, ns, key, nowMs)
	if err != nil || !found {
		return nil, err
	}

	start, stop, ok := normalizeRedisListRange(meta.Len(), start, stop)
	if !ok {
		return [][]byte{}, nil
	}

	values, _, err := readRedisListValuesTx(tx, ns, meta)
	if err != nil {
		return nil, err
	}

	result := make([][]byte, 0, int(stop-start+1))
	for i := start; i <= stop; i++ {
		result = append(result, cloneBytes(values[int(i)]))
	}
	return result, nil
}

func redisListSetTx(tx *storage.Tx, ns redisNamespace, key []byte, index int64, value []byte, nowMs int64) error {
	if _, err := purgeExpiredRedisKeyTx(tx, ns, key, nowMs); err != nil {
		return err
	}

	meta, found, err := loadRedisListMetaTx(tx, ns, key)
	if err != nil {
		return err
	}
	if !found {
		keyType, err := redisKeyTypeTx(tx, ns, key, nowMs)
		if err != nil {
			return err
		}
		if keyType == redisKeyTypeString || keyType == redisKeyTypeHash || keyType == redisKeyTypeZSet || keyType == redisKeyTypeBloom || keyType == redisKeyTypeTopK {
			return errRedisWrongType
		}
		return errRedisNoSuchKey
	}

	index, ok := normalizeRedisListIndex(meta.Len(), index)
	if !ok {
		return errRedisBadIndex
	}

	values, segmentIDs, err := readRedisListValuesTx(tx, ns, meta)
	if err != nil {
		return err
	}
	values[int(index)] = cloneBytes(value)
	return rewriteRedisListValuesTx(tx, ns, key, meta, segmentIDs, values)
}

func redisListTrimTx(tx *storage.Tx, ns redisNamespace, key []byte, start int64, stop int64, nowMs int64) error {
	if _, err := purgeExpiredRedisKeyTx(tx, ns, key, nowMs); err != nil {
		return err
	}

	meta, found, err := loadRedisListMetaTx(tx, ns, key)
	if err != nil {
		return err
	}
	if !found {
		keyType, err := redisKeyTypeTx(tx, ns, key, nowMs)
		if err != nil {
			return err
		}
		if keyType == redisKeyTypeString || keyType == redisKeyTypeHash || keyType == redisKeyTypeZSet || keyType == redisKeyTypeBloom || keyType == redisKeyTypeTopK {
			return errRedisWrongType
		}
		return nil
	}

	values, segmentIDs, err := readRedisListValuesTx(tx, ns, meta)
	if err != nil {
		return err
	}

	start, stop, ok := normalizeRedisListRange(meta.Len(), start, stop)
	if !ok {
		return rewriteRedisListValuesTx(tx, ns, key, meta, segmentIDs, nil)
	}

	trimmed := make([][]byte, 0, int(stop-start+1))
	for i := start; i <= stop; i++ {
		trimmed = append(trimmed, values[int(i)])
	}
	return rewriteRedisListValuesTx(tx, ns, key, meta, segmentIDs, trimmed)
}

func redisListRemTx(tx *storage.Tx, ns redisNamespace, key []byte, count int64, element []byte, nowMs int64) (int64, error) {
	if _, err := purgeExpiredRedisKeyTx(tx, ns, key, nowMs); err != nil {
		return 0, err
	}

	meta, found, err := loadRedisListMetaTx(tx, ns, key)
	if err != nil {
		return 0, err
	}
	if !found {
		keyType, err := redisKeyTypeTx(tx, ns, key, nowMs)
		if err != nil {
			return 0, err
		}
		if keyType == redisKeyTypeString || keyType == redisKeyTypeHash || keyType == redisKeyTypeZSet || keyType == redisKeyTypeBloom || keyType == redisKeyTypeTopK {
			return 0, errRedisWrongType
		}
		return 0, nil
	}

	values, segmentIDs, err := readRedisListValuesTx(tx, ns, meta)
	if err != nil {
		return 0, err
	}

	removed := int64(0)
	var remaining [][]byte
	switch {
	case count == 0:
		remaining = make([][]byte, 0, len(values))
		for _, value := range values {
			if bytes.Equal(value, element) {
				removed++
				continue
			}
			remaining = append(remaining, value)
		}
	case count > 0:
		remaining = make([][]byte, 0, len(values))
		for _, value := range values {
			if removed < count && bytes.Equal(value, element) {
				removed++
				continue
			}
			remaining = append(remaining, value)
		}
	default:
		limit := -count
		reversed := make([][]byte, 0, len(values))
		for i := len(values) - 1; i >= 0; i-- {
			value := values[i]
			if removed < limit && bytes.Equal(value, element) {
				removed++
				continue
			}
			reversed = append(reversed, value)
		}
		remaining = make([][]byte, 0, len(reversed))
		for i := len(reversed) - 1; i >= 0; i-- {
			remaining = append(remaining, reversed[i])
		}
	}

	if removed == 0 {
		return 0, nil
	}
	if err = rewriteRedisListValuesTx(tx, ns, key, meta, segmentIDs, remaining); err != nil {
		return 0, err
	}
	return removed, nil
}

func redisPushTx(tx *storage.Tx, ns redisNamespace, key []byte, values [][]byte, left bool, nowMs int64) (int64, error) {
	if _, err := purgeExpiredRedisKeyTx(tx, ns, key, nowMs); err != nil {
		return 0, err
	}

	meta, found, err := loadRedisListMetaTx(tx, ns, key)
	if err != nil {
		return 0, err
	}
	if !found {
		meta, err = initRedisListMetaTx(tx, ns, key, nowMs)
		if err != nil {
			return 0, err
		}
	}

	var (
		segment   *redisListSegment
		segmentID uint64
	)

	if meta.Length == 0 {
		segmentID, err = nextRedisListSegmentIDTx(tx, ns, meta)
		if err != nil {
			return 0, err
		}
		segment = &redisListSegment{}
		meta.HeadSegID = segmentID
		meta.TailSegID = segmentID
	} else {
		if left {
			segmentID = meta.HeadSegID
		} else {
			segmentID = meta.TailSegID
		}
		segment, err = loadRedisListSegmentTx(tx, ns, meta, segmentID)
		if err != nil {
			return 0, err
		}
	}

	for _, value := range values {
		if !segment.canFit(value) {
			newSegmentID, idErr := nextRedisListSegmentIDTx(tx, ns, meta)
			if idErr != nil {
				return 0, idErr
			}

			newSegment := &redisListSegment{}
			if left {
				newSegment.NextID = meta.HeadSegID
				segment.PrevID = newSegmentID
				if err = saveRedisListSegmentTx(tx, ns, meta, segmentID, segment); err != nil {
					return 0, err
				}
				meta.HeadSegID = newSegmentID
			} else {
				newSegment.PrevID = meta.TailSegID
				segment.NextID = newSegmentID
				if err = saveRedisListSegmentTx(tx, ns, meta, segmentID, segment); err != nil {
					return 0, err
				}
				meta.TailSegID = newSegmentID
			}
			segmentID = newSegmentID
			segment = newSegment
		}

		if left {
			segment.prependValue(value)
		} else {
			segment.appendValue(value)
		}
		meta.Length++
	}

	if err = saveRedisListSegmentTx(tx, ns, meta, segmentID, segment); err != nil {
		return 0, err
	}
	if err = saveRedisListMetaTx(tx, ns, key, meta); err != nil {
		return 0, err
	}
	return meta.Len(), nil
}

func redisPopTx(tx *storage.Tx, ns redisNamespace, key []byte, left bool, nowMs int64) ([]byte, bool, error) {
	if _, err := purgeExpiredRedisKeyTx(tx, ns, key, nowMs); err != nil {
		return nil, false, err
	}

	meta, found, err := loadRedisListMetaTx(tx, ns, key)
	if err != nil {
		return nil, false, err
	}
	if !found {
		keyType, err := redisKeyTypeTx(tx, ns, key, nowMs)
		if err != nil {
			return nil, false, err
		}
		if keyType == redisKeyTypeString || keyType == redisKeyTypeHash || keyType == redisKeyTypeZSet || keyType == redisKeyTypeBloom || keyType == redisKeyTypeTopK {
			return nil, false, errRedisWrongType
		}
		return nil, false, nil
	}

	if meta.Length == 0 {
		if err = deleteRedisListTx(tx, ns, key, meta); err != nil {
			return nil, false, err
		}
		return nil, false, nil
	}

	segmentID := meta.HeadSegID
	if !left {
		segmentID = meta.TailSegID
	}

	segment, err := loadRedisListSegmentTx(tx, ns, meta, segmentID)
	if err != nil {
		return nil, false, err
	}

	var value []byte
	if left {
		value, found = segment.popLeft()
	} else {
		value, found = segment.popRight()
	}
	if !found {
		return nil, false, errors.New("corrupted redis list segment")
	}

	meta.Length--
	if len(segment.Values) > 0 {
		if err = saveRedisListSegmentTx(tx, ns, meta, segmentID, segment); err != nil {
			return nil, false, err
		}
		if err = saveRedisListMetaTx(tx, ns, key, meta); err != nil {
			return nil, false, err
		}
		return value, true, nil
	}

	prevID := segment.PrevID
	nextID := segment.NextID
	if err = deleteRedisListSegmentTx(tx, ns, meta, segmentID); err != nil {
		return nil, false, err
	}

	if meta.Length == 0 {
		if err = deleteRedisListTx(tx, ns, key, meta); err != nil {
			return nil, false, err
		}
		return value, true, nil
	}

	if left {
		meta.HeadSegID = nextID
		if nextID != 0 {
			nextSegment, nextErr := loadRedisListSegmentTx(tx, ns, meta, nextID)
			if nextErr != nil {
				return nil, false, nextErr
			}
			nextSegment.PrevID = 0
			if err = saveRedisListSegmentTx(tx, ns, meta, nextID, nextSegment); err != nil {
				return nil, false, err
			}
		}
	} else {
		meta.TailSegID = prevID
		if prevID != 0 {
			prevSegment, prevErr := loadRedisListSegmentTx(tx, ns, meta, prevID)
			if prevErr != nil {
				return nil, false, prevErr
			}
			prevSegment.NextID = 0
			if err = saveRedisListSegmentTx(tx, ns, meta, prevID, prevSegment); err != nil {
				return nil, false, err
			}
		}
	}

	if err = saveRedisListMetaTx(tx, ns, key, meta); err != nil {
		return nil, false, err
	}
	return value, true, nil
}

func redisMoveTx(tx *storage.Tx, ns redisNamespace, source, destination []byte, nowMs int64) ([]byte, bool, error) {
	value, found, err := redisPopTx(tx, ns, source, false, nowMs)
	if err != nil || !found {
		return value, found, err
	}
	if _, err = redisPushTx(tx, ns, destination, [][]byte{value}, true, nowMs); err != nil {
		return nil, false, err
	}
	return value, true, nil
}
