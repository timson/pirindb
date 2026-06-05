package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"io"

	"github.com/timson/pirindb/storage"
)

const (
	clusterSlotStreamVersion     uint16 = 1
	clusterSlotStreamFrameEntry  uint8  = 1
	clusterSlotStreamFrameEnd    uint8  = 255
	clusterImportBatchEntryLimit        = 256
)

var clusterSlotStreamMagic = [8]byte{'P', 'I', 'R', 'C', 'L', 'S', 'T', 'R'}

type clusterSlotStreamHeader struct {
	Version   uint16
	SlotCount int
	StartSlot int
	EndSlot   int
}

type clusterSlotStreamEntry struct {
	DBIndex        int
	Key            []byte
	Type           string
	TTLMillis      int64
	StringValueLen uint64
	ListValues     [][]byte
	HashPairs      []clusterSnapshotHashPair
	ZSetPairs      []clusterSnapshotZSetPair
	Bloom          *clusterSnapshotBloom
	TopK           *clusterSnapshotTopK
}

type clusterSnapshotHashPair struct {
	Field []byte
	Value []byte
}

type clusterSnapshotZSetPair struct {
	Score  float64
	Member []byte
}

type clusterSnapshotBucketEntry struct {
	Key   []byte
	Value []byte
}

type clusterSnapshotBloom struct {
	InitialCapacity uint64
	ErrorRateBits   uint64
	Expansion       uint64
	Flags           uint64
	SubFilterCount  uint64
	Data            []clusterSnapshotBucketEntry
}

type clusterSnapshotTopK struct {
	K         uint64
	Width     uint64
	Depth     uint64
	HeapSize  uint64
	DecayBits uint64
	Data      []clusterSnapshotBucketEntry
}

type clusterSlotTransferStats struct {
	EntriesTransferred uint64 `json:"entries_transferred"`
	BytesTransferred   uint64 `json:"bytes_transferred"`
	EOF                bool   `json:"eof"`
	NextCursorDB       int    `json:"-"`
	NextCursorKey      []byte `json:"-"`
}

type clusterSlotDeleteStats struct {
	DeletedKeys   uint64 `json:"deleted_keys"`
	EOF           bool   `json:"eof"`
	NextCursorDB  int    `json:"-"`
	NextCursorKey []byte `json:"-"`
}

func streamClusterSlotRange(db *storage.DB, w io.Writer, slotCount int, startSlot int, endSlot int, nowMs int64) error {
	_, err := streamClusterSlotRangeChunk(db, w, slotCount, startSlot, endSlot, nowMs, redisDatabaseMin, nil, 0)
	return err
}

func streamClusterSlotRangeChunk(db *storage.DB, w io.Writer, slotCount int, startSlot int, endSlot int, nowMs int64, cursorDB int, cursorKey []byte, limit int) (clusterSlotTransferStats, error) {
	buffered := bufio.NewWriter(w)
	header := clusterSlotStreamHeader{
		Version:   clusterSlotStreamVersion,
		SlotCount: slotCount,
		StartSlot: startSlot,
		EndSlot:   endSlot,
	}
	if err := writeClusterSlotStreamHeader(buffered, header); err != nil {
		return clusterSlotTransferStats{}, err
	}
	counter := &countingWriter{w: buffered}
	stats := clusterSlotTransferStats{}
	if cursorDB < redisDatabaseMin || cursorDB > redisDatabaseMax {
		cursorDB = redisDatabaseMin
		cursorKey = nil
	}
	err := db.View(func(tx *storage.Tx) error {
		for dbIndex := cursorDB; dbIndex <= redisDatabaseMax; dbIndex++ {
			ns := redisNamespaceForDB(dbIndex, slotCount)
			currentLimit := 0
			if limit > 0 {
				currentLimit = limit - int(stats.EntriesTransferred)
				if currentLimit <= 0 {
					stats.EOF = false
					return nil
				}
			}
			resumeAfter := []byte(nil)
			if dbIndex == cursorDB {
				resumeAfter = cursorKey
			}
			yielded, lastIndexKey, exhausted, err := forEachRedisKeyBySlotRangeFromIndexKeyTx(tx, ns, startSlot, endSlot, nowMs, resumeAfter, currentLimit, func(key []byte) error {
				return writeClusterSlotStreamEntryTx(tx, counter, ns, dbIndex, key, nowMs)
			})
			if err != nil {
				return err
			}
			if yielded > 0 {
				stats.NextCursorDB = dbIndex
				stats.NextCursorKey = cloneBytes(lastIndexKey)
			}
			stats.EntriesTransferred += yielded
			if limit > 0 && int(stats.EntriesTransferred) >= limit {
				if exhausted && dbIndex == redisDatabaseMax {
					stats.EOF = true
					stats.NextCursorKey = nil
					stats.NextCursorDB = redisDatabaseMin
				} else if exhausted {
					stats.EOF = false
					stats.NextCursorDB = dbIndex + 1
					stats.NextCursorKey = nil
				} else {
					stats.EOF = false
				}
				return nil
			}
			if !exhausted {
				stats.EOF = false
				return nil
			}
			stats.NextCursorDB = dbIndex + 1
			stats.NextCursorKey = nil
		}
		stats.EOF = true
		stats.NextCursorDB = redisDatabaseMin
		stats.NextCursorKey = nil
		return nil
	})
	if err != nil {
		return clusterSlotTransferStats{}, err
	}
	if err = writeClusterSlotStreamFrameType(counter, clusterSlotStreamFrameEnd); err != nil {
		return clusterSlotTransferStats{}, err
	}
	if err = buffered.Flush(); err != nil {
		return clusterSlotTransferStats{}, err
	}
	stats.BytesTransferred = uint64(counter.n)
	return stats, nil
}

type countingWriter struct {
	w io.Writer
	n int64
}

func (w *countingWriter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	w.n += int64(n)
	return n, err
}

type countingReader struct {
	r io.Reader
	n int64
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	r.n += int64(n)
	return n, err
}

func importClusterSlotStream(db *storage.DB, r io.Reader, nowMs int64) (clusterSlotTransferStats, error) {
	buffered := bufio.NewReader(r)
	counter := &countingReader{r: buffered}
	header, err := readClusterSlotStreamHeader(counter)
	if err != nil {
		return clusterSlotTransferStats{}, err
	}
	slotCount := header.SlotCount
	if slotCount <= 0 {
		slotCount = 16384
	}

	tx := db.Begin(true)
	defer tx.Rollback()

	stats := clusterSlotTransferStats{}
	entriesInBatch := 0
	for {
		frameType, err := readClusterSlotStreamFrameType(counter)
		if err != nil {
			return clusterSlotTransferStats{}, err
		}
		switch frameType {
		case clusterSlotStreamFrameEntry:
			entry, err := readClusterSlotStreamEntryMeta(counter)
			if err != nil {
				return clusterSlotTransferStats{}, err
			}
			if err = importClusterSlotStreamEntryTx(tx, counter, entry, slotCount, nowMs); err != nil {
				return clusterSlotTransferStats{}, err
			}
			stats.EntriesTransferred++
			entriesInBatch++
			if entriesInBatch >= clusterImportBatchEntryLimit {
				if err = tx.Commit(); err != nil {
					return clusterSlotTransferStats{}, err
				}
				tx = db.Begin(true)
				entriesInBatch = 0
			}
		case clusterSlotStreamFrameEnd:
			if err = tx.Commit(); err != nil {
				return clusterSlotTransferStats{}, err
			}
			stats.BytesTransferred = uint64(counter.n)
			stats.EOF = true
			return stats, nil
		default:
			return clusterSlotTransferStats{}, fmt.Errorf("unsupported cluster slot stream frame type %d", frameType)
		}
	}
}

func dumpBucketEntries(bucket *storage.Bucket) ([]clusterSnapshotBucketEntry, error) {
	entries := make([]clusterSnapshotBucketEntry, 0)
	if err := bucket.ForEach(func(k, v []byte) error {
		entries = append(entries, clusterSnapshotBucketEntry{
			Key:   cloneBytes(k),
			Value: cloneBytes(v),
		})
		return nil
	}); err != nil {
		return nil, err
	}
	return entries, nil
}

func writeClusterSlotStreamEntryTx(tx *storage.Tx, w io.Writer, ns redisNamespace, dbIndex int, key []byte, nowMs int64) error {
	entry, err := exportClusterSlotStreamEntryTx(tx, ns, dbIndex, key, nowMs)
	if err != nil || entry == nil {
		return err
	}
	if err = writeClusterSlotStreamFrameType(w, clusterSlotStreamFrameEntry); err != nil {
		return err
	}
	if err = writeClusterSlotStreamEntryMeta(w, entry); err != nil {
		return err
	}
	if entry.Type == redisKeyTypeString && entry.StringValueLen > 0 {
		return writeRedisStringValueTx(tx, ns, key, w)
	}
	return nil
}

func exportClusterSlotStreamEntryTx(tx *storage.Tx, ns redisNamespace, dbIndex int, key []byte, nowMs int64) (*clusterSlotStreamEntry, error) {
	keyType, err := redisKeyTypeTx(tx, ns, key, nowMs)
	if err != nil || keyType == redisKeyTypeNone {
		return nil, err
	}
	ttlMillis, err := redisPTTLTx(tx, ns, key, nowMs)
	if err != nil {
		return nil, err
	}
	if ttlMillis < 0 {
		ttlMillis = -1
	}
	entry := &clusterSlotStreamEntry{
		DBIndex:   dbIndex,
		Key:       cloneBytes(key),
		Type:      keyType,
		TTLMillis: ttlMillis,
	}
	switch keyType {
	case redisKeyTypeString:
		bucket, err := tx.GetBucket(ns.stringBucket)
		if errors.Is(err, storage.ErrBucketNotFound) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		valueLen, found, err := bucket.ValueLen(key)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, nil
		}
		entry.StringValueLen = uint64(valueLen)
	case redisKeyTypeList:
		meta, found, loadErr := loadRedisListMetaForReadTx(tx, ns, key, nowMs)
		if loadErr != nil {
			return nil, loadErr
		}
		if !found {
			return nil, nil
		}
		values, _, readErr := readRedisListValuesTx(tx, ns, meta)
		if readErr != nil {
			return nil, readErr
		}
		entry.ListValues = make([][]byte, 0, len(values))
		for _, value := range values {
			entry.ListValues = append(entry.ListValues, cloneBytes(value))
		}
	case redisKeyTypeHash:
		bucket, _, found, loadErr := getRedisHashBucketTx(tx, ns, key, nowMs)
		if loadErr != nil {
			return nil, loadErr
		}
		if !found {
			return nil, nil
		}
		pairs := make([]clusterSnapshotHashPair, 0)
		if err = bucket.ForEach(func(field, value []byte) error {
			pairs = append(pairs, clusterSnapshotHashPair{
				Field: cloneBytes(field),
				Value: cloneBytes(value),
			})
			return nil
		}); err != nil {
			return nil, err
		}
		entry.HashPairs = pairs
	case redisKeyTypeZSet:
		memberBucket, _, _, found, loadErr := getRedisZSetBucketsTx(tx, ns, key, nowMs)
		if loadErr != nil {
			return nil, loadErr
		}
		if !found {
			return nil, nil
		}
		pairs := make([]clusterSnapshotZSetPair, 0)
		if err = memberBucket.ForEach(func(member, rawScore []byte) error {
			score, scoreErr := decodeRedisZSetSortableScore(rawScore)
			if scoreErr != nil {
				return scoreErr
			}
			pairs = append(pairs, clusterSnapshotZSetPair{
				Score:  score,
				Member: cloneBytes(member),
			})
			return nil
		}); err != nil {
			return nil, err
		}
		entry.ZSetPairs = pairs
	case redisKeyTypeBloom:
		meta, found, loadErr := loadRedisBloomMetaForReadTx(tx, ns, key, nowMs)
		if loadErr != nil {
			return nil, loadErr
		}
		if !found {
			return nil, nil
		}
		dataBucket, bucketErr := getRedisBloomDataBucketTx(tx, ns, meta)
		if bucketErr != nil {
			return nil, bucketErr
		}
		dataEntries, dumpErr := dumpBucketEntries(dataBucket)
		if dumpErr != nil {
			return nil, dumpErr
		}
		entry.Bloom = &clusterSnapshotBloom{
			InitialCapacity: meta.InitialCapacity,
			ErrorRateBits:   meta.ErrorRateBits,
			Expansion:       meta.Expansion,
			Flags:           meta.Flags,
			SubFilterCount:  meta.SubFilterCount,
			Data:            dataEntries,
		}
	case redisKeyTypeTopK:
		meta, found, loadErr := loadRedisTopKMetaForReadTx(tx, ns, key, nowMs)
		if loadErr != nil {
			return nil, loadErr
		}
		if !found {
			return nil, nil
		}
		dataBucket, bucketErr := getRedisTopKDataBucketTx(tx, ns, meta)
		if bucketErr != nil {
			return nil, bucketErr
		}
		dataEntries, dumpErr := dumpBucketEntries(dataBucket)
		if dumpErr != nil {
			return nil, dumpErr
		}
		entry.TopK = &clusterSnapshotTopK{
			K:         meta.K,
			Width:     meta.Width,
			Depth:     meta.Depth,
			HeapSize:  meta.HeapSize,
			DecayBits: meta.DecayBits,
			Data:      dataEntries,
		}
	default:
		return nil, errors.New("unsupported redis type in cluster stream export")
	}
	return entry, nil
}

func importClusterSlotStreamEntryTx(tx *storage.Tx, r io.Reader, entry *clusterSlotStreamEntry, slotCount int, nowMs int64) error {
	ns := redisNamespaceForDB(entry.DBIndex, slotCount)
	rawType, err := redisRawKeyTypeTx(tx, ns, entry.Key)
	if err != nil {
		return err
	}
	if _, err = deleteRedisKeyByRawTypeTx(tx, ns, entry.Key, rawType); err != nil {
		return err
	}
	switch entry.Type {
	case redisKeyTypeString:
		if err = putRedisStringReaderTx(tx, ns, entry.Key, r, int64(entry.StringValueLen)); err != nil {
			return err
		}
	case redisKeyTypeList:
		if _, err = redisPushTx(tx, ns, entry.Key, entry.ListValues, false, nowMs); err != nil {
			return err
		}
	case redisKeyTypeHash:
		pairs := make([]hashFieldValuePair, 0, len(entry.HashPairs))
		for _, pair := range entry.HashPairs {
			pairs = append(pairs, hashFieldValuePair{
				field: cloneBytes(pair.Field),
				value: cloneBytes(pair.Value),
			})
		}
		if _, err = redisHashSetTx(tx, ns, entry.Key, pairs, nowMs); err != nil {
			return err
		}
	case redisKeyTypeZSet:
		pairs := make([]redisZSetScoreMemberPair, 0, len(entry.ZSetPairs))
		for _, pair := range entry.ZSetPairs {
			pairs = append(pairs, redisZSetScoreMemberPair{
				score:  pair.Score,
				member: cloneBytes(pair.Member),
			})
		}
		if _, err = redisZSetAddTx(tx, ns, entry.Key, pairs, nowMs); err != nil {
			return err
		}
	case redisKeyTypeBloom:
		if entry.Bloom == nil {
			return errors.New("corrupted bloom stream entry")
		}
		id, idErr := nextRedisBloomIDTx(tx, ns)
		if idErr != nil {
			return idErr
		}
		meta := &redisBloomMeta{
			ID:              id,
			InitialCapacity: entry.Bloom.InitialCapacity,
			ErrorRateBits:   entry.Bloom.ErrorRateBits,
			Expansion:       entry.Bloom.Expansion,
			Flags:           entry.Bloom.Flags,
			SubFilterCount:  entry.Bloom.SubFilterCount,
		}
		bucket, bucketErr := ensureRedisBloomDataBucketTx(tx, ns, meta)
		if bucketErr != nil {
			return bucketErr
		}
		for _, item := range entry.Bloom.Data {
			if err = bucket.Put(item.Key, item.Value); err != nil {
				return err
			}
		}
		if err = saveRedisBloomMetaTx(tx, ns, entry.Key, meta); err != nil {
			return err
		}
	case redisKeyTypeTopK:
		if entry.TopK == nil {
			return errors.New("corrupted topk stream entry")
		}
		id, idErr := nextRedisTopKIDTx(tx, ns)
		if idErr != nil {
			return idErr
		}
		meta := &redisTopKMeta{
			ID:        id,
			K:         entry.TopK.K,
			Width:     entry.TopK.Width,
			Depth:     entry.TopK.Depth,
			HeapSize:  entry.TopK.HeapSize,
			DecayBits: entry.TopK.DecayBits,
		}
		bucket, bucketErr := ensureRedisTopKDataBucketTx(tx, ns, meta)
		if bucketErr != nil {
			return bucketErr
		}
		for _, item := range entry.TopK.Data {
			if err = bucket.Put(item.Key, item.Value); err != nil {
				return err
			}
		}
		if err = saveRedisTopKMetaTx(tx, ns, entry.Key, meta); err != nil {
			return err
		}
	default:
		return errors.New("unsupported redis type in cluster stream import")
	}
	if entry.TTLMillis >= 0 {
		_, err = redisExpireKeyAtTx(tx, ns, entry.Key, nowMs+entry.TTLMillis, nowMs)
		return err
	}
	return nil
}

func deleteClusterSlotRangeChunk(db *storage.DB, slotCount int, startSlot int, endSlot int, nowMs int64, cursorDB int, cursorKey []byte, limit int) (clusterSlotDeleteStats, error) {
	stats := clusterSlotDeleteStats{}
	if cursorDB < redisDatabaseMin || cursorDB > redisDatabaseMax {
		cursorDB = redisDatabaseMin
		cursorKey = nil
	}
	err := db.Update(func(tx *storage.Tx) error {
		for dbIndex := cursorDB; dbIndex <= redisDatabaseMax; dbIndex++ {
			ns := redisNamespaceForDB(dbIndex, slotCount)
			currentLimit := 0
			if limit > 0 {
				currentLimit = limit - int(stats.DeletedKeys)
				if currentLimit <= 0 {
					stats.EOF = false
					return nil
				}
			}
			resumeAfter := []byte(nil)
			if dbIndex == cursorDB {
				resumeAfter = cursorKey
			}
			toDelete := make([][]byte, 0)
			deleted, lastIndexKey, exhausted, err := forEachRedisKeyBySlotRangeFromIndexKeyTx(tx, ns, startSlot, endSlot, nowMs, resumeAfter, currentLimit, func(key []byte) error {
				toDelete = append(toDelete, cloneBytes(key))
				return nil
			})
			if err != nil {
				return err
			}
			if len(toDelete) > 0 {
				if _, err = deleteManyRedisBytesTx(tx, ns, toDelete, nowMs); err != nil {
					return err
				}
				stats.DeletedKeys += deleted
				stats.NextCursorDB = dbIndex
				stats.NextCursorKey = cloneBytes(lastIndexKey)
			}
			if limit > 0 && int(stats.DeletedKeys) >= limit {
				if exhausted && dbIndex == redisDatabaseMax {
					stats.EOF = true
					stats.NextCursorDB = redisDatabaseMin
					stats.NextCursorKey = nil
				} else if exhausted {
					stats.EOF = false
					stats.NextCursorDB = dbIndex + 1
					stats.NextCursorKey = nil
				} else {
					stats.EOF = false
				}
				return nil
			}
			if !exhausted {
				stats.EOF = false
				return nil
			}
			stats.NextCursorDB = dbIndex + 1
			stats.NextCursorKey = nil
		}
		stats.EOF = true
		stats.NextCursorDB = redisDatabaseMin
		stats.NextCursorKey = nil
		return nil
	})
	if err != nil {
		return clusterSlotDeleteStats{}, err
	}
	return stats, nil
}

func deleteClusterSlotRange(db *storage.DB, slotCount int, startSlot int, endSlot int, nowMs int64) error {
	cursorDB := redisDatabaseMin
	var cursorKey []byte
	for {
		stats, err := deleteClusterSlotRangeChunk(db, slotCount, startSlot, endSlot, nowMs, cursorDB, cursorKey, clusterImportBatchEntryLimit)
		if err != nil {
			return err
		}
		if stats.EOF {
			return nil
		}
		cursorDB = stats.NextCursorDB
		cursorKey = cloneBytes(stats.NextCursorKey)
	}
}

func writeRedisStringValueTx(tx *storage.Tx, ns redisNamespace, key []byte, w io.Writer) error {
	bucket, err := tx.GetBucket(ns.stringBucket)
	if err != nil {
		return err
	}
	_, found, err := bucket.WriteValueTo(key, w)
	if err != nil {
		return err
	}
	if !found {
		return errRedisNoSuchKey
	}
	return nil
}

func putRedisStringReaderTx(tx *storage.Tx, ns redisNamespace, key []byte, r io.Reader, valueLen int64) error {
	bucket, err := tx.CreateBucketIfNotExists(ns.stringBucket)
	if err != nil {
		return err
	}
	if err = bucket.PutReader(key, r, valueLen); err != nil {
		return err
	}
	if err = ensureRedisSlotIndexEntryTx(tx, ns, key); err != nil {
		return err
	}
	return deleteRedisExpireAtMsTx(tx, ns, key)
}

func writeClusterSlotStreamHeader(w io.Writer, header clusterSlotStreamHeader) error {
	if _, err := w.Write(clusterSlotStreamMagic[:]); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, header.Version); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, uint32(header.SlotCount)); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, uint32(header.StartSlot)); err != nil {
		return err
	}
	return binary.Write(w, binary.LittleEndian, uint32(header.EndSlot))
}

func readClusterSlotStreamHeader(r io.Reader) (clusterSlotStreamHeader, error) {
	var magic [8]byte
	if _, err := io.ReadFull(r, magic[:]); err != nil {
		return clusterSlotStreamHeader{}, err
	}
	if magic != clusterSlotStreamMagic {
		return clusterSlotStreamHeader{}, errors.New("invalid cluster slot stream magic")
	}
	var version uint16
	if err := binary.Read(r, binary.LittleEndian, &version); err != nil {
		return clusterSlotStreamHeader{}, err
	}
	if version != clusterSlotStreamVersion {
		return clusterSlotStreamHeader{}, errors.New("unsupported cluster slot stream version")
	}
	var slotCount uint32
	if err := binary.Read(r, binary.LittleEndian, &slotCount); err != nil {
		return clusterSlotStreamHeader{}, err
	}
	var startSlot uint32
	if err := binary.Read(r, binary.LittleEndian, &startSlot); err != nil {
		return clusterSlotStreamHeader{}, err
	}
	var endSlot uint32
	if err := binary.Read(r, binary.LittleEndian, &endSlot); err != nil {
		return clusterSlotStreamHeader{}, err
	}
	return clusterSlotStreamHeader{
		Version:   version,
		SlotCount: int(slotCount),
		StartSlot: int(startSlot),
		EndSlot:   int(endSlot),
	}, nil
}

func writeClusterSlotStreamFrameType(w io.Writer, frameType uint8) error {
	return binary.Write(w, binary.LittleEndian, frameType)
}

func readClusterSlotStreamFrameType(r io.Reader) (uint8, error) {
	var frameType uint8
	if err := binary.Read(r, binary.LittleEndian, &frameType); err != nil {
		return 0, err
	}
	return frameType, nil
}

func writeClusterSlotStreamEntryMeta(w io.Writer, entry *clusterSlotStreamEntry) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(entry); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, uint32(buf.Len())); err != nil {
		return err
	}
	_, err := w.Write(buf.Bytes())
	return err
}

func readClusterSlotStreamEntryMeta(r io.Reader) (*clusterSlotStreamEntry, error) {
	var payloadLen uint32
	if err := binary.Read(r, binary.LittleEndian, &payloadLen); err != nil {
		return nil, err
	}
	payload := make([]byte, int(payloadLen))
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}
	var entry clusterSlotStreamEntry
	if err := gob.NewDecoder(bytes.NewReader(payload)).Decode(&entry); err != nil {
		return nil, err
	}
	return &entry, nil
}
