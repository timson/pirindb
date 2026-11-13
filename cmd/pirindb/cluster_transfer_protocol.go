package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/timson/pirindb/storage"
)

const (
	clusterTransferProtocolVersion uint16 = 2

	clusterTransferRecordManifest uint8 = iota + 1
	clusterTransferRecordKeyBegin
	clusterTransferRecordBucketEntryBegin
	clusterTransferRecordValueChunk
	clusterTransferRecordBucketEntryEnd
	clusterTransferRecordKeyEnd
	clusterTransferRecordKeyDelete
	clusterTransferRecordStreamEnd

	clusterTransferValueChunkSize                = 64 * 1024
	clusterTransferMaxFramePayload               = 1024 * 1024
	clusterTransferDefaultTargetChunkBytes int64 = 32 * 1024 * 1024
	clusterTransferDefaultMaxChunkBytes    int64 = 64 * 1024 * 1024
	clusterTransferDefaultMaxChunkRecords        = 65536
	clusterTransferMaxConfiguredChunkBytes int64 = 1024 * 1024 * 1024
	clusterTransferMaxConfiguredRecords          = 1000000

	clusterTransferBucketString     = "string"
	clusterTransferBucketListData   = "list-data"
	clusterTransferBucketHashData   = "hash-data"
	clusterTransferBucketZSetMember = "zset-member"
	clusterTransferBucketZSetScore  = "zset-score"
	clusterTransferBucketBloomData  = "bloom-data"
	clusterTransferBucketTopKData   = "topk-data"

	clusterImportReceiptsBucketNameConst = "__pirin_cluster_import_receipts__"
	clusterImportReceiptBucketPrefix     = "__pirin_cluster_import_receipts__:"
	clusterImportStageBucketPrefix       = "__pirin_cluster_import_stage__:"
	clusterImportKeyStateBucketNameConst = "__pirin_cluster_import_key_state__"
	clusterImportKeyStateSchemaVersion   = 1
)

var clusterTransferCRC32Table = crc32.MakeTable(crc32.Castagnoli)

type clusterTransferRecord struct {
	Type       uint8
	DBIndex    int
	Key        []byte
	RedisType  string
	ExpireAtMs int64

	BucketKind string
	EntryKey   []byte
	ValueLen   uint64
	Offset     uint64
	Data       []byte

	Meta1 uint64
	Meta2 uint64
	Meta3 uint64
	Meta4 uint64
	Meta5 uint64
	Meta6 uint64

	ClusterID         string
	MoveID            string
	SourceNodeID      string
	DestinationNodeID string
	Epoch             uint64
}

type clusterTransferIdentity struct {
	ClusterID         string
	MoveID            string
	SourceNodeID      string
	DestinationNodeID string
	Epoch             uint64
}

type clusterTransferFrameWriter struct {
	w        io.Writer
	sequence uint64
}

type clusterTransferChunkPolicy struct {
	TargetBytes int64
	MaxBytes    int64
	MaxRecords  uint64
	MaxKeys     int
}

type clusterImportReceiptContext struct {
	JobID      string
	ChunkIndex uint64
}

type clusterTransferDecodedFrame struct {
	sequence uint64
	checksum uint32
	record   clusterTransferRecord
}

type clusterImportStageMeta struct {
	ValueLen   uint64
	NextOffset uint64
}

type clusterImportKeyState struct {
	SchemaVersion  uint8
	Begin          clusterTransferRecord
	ReceiptContext clusterImportReceiptContext
	AllocatedID    uint64
}

type clusterImportManifest struct {
	SchemaVersion uint8
	LastSequence  uint64
	FramesApplied uint64
	KeysCompleted uint64
	Complete      bool
	UpdatedAtMs   int64
}

func (w *clusterTransferFrameWriter) writeRecord(record clusterTransferRecord) error {
	var payload bytes.Buffer
	if err := gob.NewEncoder(&payload).Encode(record); err != nil {
		return err
	}
	if payload.Len() > clusterTransferMaxFramePayload {
		return fmt.Errorf("cluster transfer frame exceeds %d bytes", clusterTransferMaxFramePayload)
	}
	w.sequence++
	checksum := crc32.Checksum(payload.Bytes(), clusterTransferCRC32Table)
	if err := binary.Write(w.w, binary.LittleEndian, w.sequence); err != nil {
		return err
	}
	if err := binary.Write(w.w, binary.LittleEndian, uint32(payload.Len())); err != nil {
		return err
	}
	if err := binary.Write(w.w, binary.LittleEndian, checksum); err != nil {
		return err
	}
	_, err := w.w.Write(payload.Bytes())
	return err
}

func readClusterTransferRecord(r io.Reader) (uint64, uint32, clusterTransferRecord, error) {
	var sequence uint64
	if err := binary.Read(r, binary.LittleEndian, &sequence); err != nil {
		return 0, 0, clusterTransferRecord{}, err
	}
	var payloadLen uint32
	if err := binary.Read(r, binary.LittleEndian, &payloadLen); err != nil {
		return 0, 0, clusterTransferRecord{}, err
	}
	if payloadLen == 0 || payloadLen > clusterTransferMaxFramePayload {
		return 0, 0, clusterTransferRecord{}, errors.New("cluster transfer frame length is invalid")
	}
	var expectedChecksum uint32
	if err := binary.Read(r, binary.LittleEndian, &expectedChecksum); err != nil {
		return 0, 0, clusterTransferRecord{}, err
	}
	payload := make([]byte, int(payloadLen))
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, 0, clusterTransferRecord{}, err
	}
	actualChecksum := crc32.Checksum(payload, clusterTransferCRC32Table)
	if actualChecksum != expectedChecksum {
		return 0, 0, clusterTransferRecord{}, errors.New("cluster transfer frame checksum mismatch")
	}
	var record clusterTransferRecord
	if err := gob.NewDecoder(bytes.NewReader(payload)).Decode(&record); err != nil {
		return 0, 0, clusterTransferRecord{}, err
	}
	return sequence, actualChecksum, record, nil
}

func streamClusterSlotRangeChunk(db *storage.DB, w io.Writer, slotCount int, startSlot int, endSlot int, nowMs int64, cursorDB int, cursorKey []byte, limit int) (clusterSlotTransferStats, error) {
	return streamClusterSlotRangeChunkWithIdentity(db, w, slotCount, startSlot, endSlot, nowMs, cursorDB, cursorKey, limit, clusterTransferIdentity{})
}

func streamClusterSlotRangeChunkWithIdentity(db *storage.DB, w io.Writer, slotCount int, startSlot int, endSlot int, nowMs int64, cursorDB int, cursorKey []byte, limit int, identity clusterTransferIdentity) (clusterSlotTransferStats, error) {
	return streamClusterSlotRangeChunkWithIdentityAndPolicy(db, w, slotCount, startSlot, endSlot, nowMs, cursorDB, cursorKey, clusterTransferChunkPolicy{MaxKeys: limit}, identity)
}

func streamClusterSlotRangeChunkWithIdentityAndPolicy(db *storage.DB, w io.Writer, slotCount int, startSlot int, endSlot int, nowMs int64, cursorDB int, cursorKey []byte, policy clusterTransferChunkPolicy, identity clusterTransferIdentity) (clusterSlotTransferStats, error) {
	buffered := bufio.NewWriter(w)
	counter := &countingWriter{w: buffered}
	header := clusterSlotStreamHeader{Version: clusterTransferProtocolVersion, SlotCount: slotCount, StartSlot: startSlot, EndSlot: endSlot}
	if err := writeClusterSlotStreamHeader(counter, header); err != nil {
		return clusterSlotTransferStats{}, err
	}
	frameWriter := &clusterTransferFrameWriter{w: counter}
	if err := frameWriter.writeRecord(clusterTransferManifestRecord(identity)); err != nil {
		return clusterSlotTransferStats{}, err
	}
	stats := clusterSlotTransferStats{}
	keysWritten := 0
	if cursorDB < redisDatabaseMin || cursorDB > redisDatabaseMax {
		cursorDB = redisDatabaseMin
		cursorKey = nil
	}

	err := db.View(func(tx *storage.Tx) error {
		for dbIndex := cursorDB; dbIndex <= redisDatabaseMax; dbIndex++ {
			ns := redisNamespaceForDB(dbIndex, slotCount)
			currentLimit := 0
			if policy.MaxKeys > 0 {
				currentLimit = policy.MaxKeys - int(stats.EntriesTransferred)
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
				keyType, typeErr := redisKeyTypeTx(tx, ns, key, nowMs)
				if typeErr != nil {
					return typeErr
				}
				estimatedBytes, estimateErr := estimateRedisKeyLogicalBytesTx(tx, ns, key, keyType, nowMs)
				if estimateErr != nil {
					return estimateErr
				}
				projectedBytes := counter.n + int64(estimatedBytes) + 4096
				if keysWritten > 0 && ((policy.TargetBytes > 0 && projectedBytes > policy.TargetBytes) || (policy.MaxBytes > 0 && projectedBytes > policy.MaxBytes)) {
					return errRedisSlotIterationStopBefore
				}
				if writeErr := writeClusterTransferKeyTx(tx, frameWriter, ns, dbIndex, key, nowMs); writeErr != nil {
					return writeErr
				}
				keysWritten++
				if policy.MaxBytes > 0 && counter.n > policy.MaxBytes && keysWritten == 1 {
					stats.OversizedLogicalKey = true
				}
				if (policy.TargetBytes > 0 && counter.n >= policy.TargetBytes) || (policy.MaxRecords > 0 && frameWriter.sequence >= policy.MaxRecords) {
					return errRedisSlotIterationStopAfter
				}
				return nil
			})
			if err != nil {
				return err
			}
			if yielded > 0 {
				stats.NextCursorDB = dbIndex
				stats.NextCursorKey = cloneBytes(lastIndexKey)
			}
			stats.EntriesTransferred += yielded
			if policy.MaxKeys > 0 && int(stats.EntriesTransferred) >= policy.MaxKeys {
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
		return clusterSlotTransferStats{}, err
	}
	if err = frameWriter.writeRecord(clusterTransferRecord{Type: clusterTransferRecordStreamEnd}); err != nil {
		return clusterSlotTransferStats{}, err
	}
	if err = buffered.Flush(); err != nil {
		return clusterSlotTransferStats{}, err
	}
	stats.BytesTransferred = uint64(counter.n)
	stats.RecordsTransferred = frameWriter.sequence
	return stats, nil
}

// streamClusterDirtyKeyBatch resolves compact dirty-key markers to their final
// state while the source slot range is write-fenced. Missing or expired keys
// are represented as tombstones, and live keys use the same bounded v2 record
// stream as the base copy.
func streamClusterDirtyKeyBatch(db *storage.DB, w io.Writer, slotCount int, startSlot int, endSlot int, nowMs int64, batch clusterDeltaBatch) (clusterSlotTransferStats, error) {
	return streamClusterDirtyKeyBatchWithIdentity(db, w, slotCount, startSlot, endSlot, nowMs, batch, clusterTransferIdentity{})
}

func streamClusterDirtyKeyBatchWithIdentity(db *storage.DB, w io.Writer, slotCount int, startSlot int, endSlot int, nowMs int64, batch clusterDeltaBatch, identity clusterTransferIdentity) (clusterSlotTransferStats, error) {
	buffered := bufio.NewWriter(w)
	counter := &countingWriter{w: buffered}
	header := clusterSlotStreamHeader{Version: clusterTransferProtocolVersion, SlotCount: slotCount, StartSlot: startSlot, EndSlot: endSlot}
	if err := writeClusterSlotStreamHeader(counter, header); err != nil {
		return clusterSlotTransferStats{}, err
	}
	frameWriter := &clusterTransferFrameWriter{w: counter}
	if err := frameWriter.writeRecord(clusterTransferManifestRecord(identity)); err != nil {
		return clusterSlotTransferStats{}, err
	}
	stats := clusterSlotTransferStats{EOF: true}
	err := db.View(func(tx *storage.Tx) error {
		for _, mutation := range batch.Mutations {
			if mutation.DBIndex < redisDatabaseMin || mutation.DBIndex > redisDatabaseMax || len(mutation.Key) == 0 {
				return errors.New("cluster dirty-key marker is invalid")
			}
			slot := ClusterKeySlot(mutation.Key, slotCount)
			if slot < startSlot || slot > endSlot {
				return errors.New("cluster dirty-key marker is outside the moving slot range")
			}
			ns := redisNamespaceForDB(mutation.DBIndex, slotCount)
			keyType, err := redisKeyTypeTx(tx, ns, mutation.Key, nowMs)
			if err != nil {
				return err
			}
			if keyType == redisKeyTypeNone {
				if err = frameWriter.writeRecord(clusterTransferRecord{
					Type:    clusterTransferRecordKeyDelete,
					DBIndex: mutation.DBIndex,
					Key:     cloneBytes(mutation.Key),
				}); err != nil {
					return err
				}
			} else if err = writeClusterTransferKeyTx(tx, frameWriter, ns, mutation.DBIndex, mutation.Key, nowMs); err != nil {
				return err
			}
			stats.EntriesTransferred++
		}
		return nil
	})
	if err != nil {
		return clusterSlotTransferStats{}, err
	}
	if err = frameWriter.writeRecord(clusterTransferRecord{Type: clusterTransferRecordStreamEnd}); err != nil {
		return clusterSlotTransferStats{}, err
	}
	if err = buffered.Flush(); err != nil {
		return clusterSlotTransferStats{}, err
	}
	stats.BytesTransferred = uint64(counter.n)
	stats.RecordsTransferred = frameWriter.sequence
	return stats, nil
}

func countClusterSlotRangeKeys(db *storage.DB, slotCount int, startSlot int, endSlot int, nowMs int64) (uint64, error) {
	var count uint64
	err := db.View(func(tx *storage.Tx) error {
		for dbIndex := redisDatabaseMin; dbIndex <= redisDatabaseMax; dbIndex++ {
			ns := redisNamespaceForDB(dbIndex, slotCount)
			yielded, _, _, err := forEachRedisKeyBySlotRangeFromIndexKeyTx(tx, ns, startSlot, endSlot, nowMs, nil, 0, func([]byte) error {
				return nil
			})
			if err != nil {
				return err
			}
			count += yielded
		}
		return nil
	})
	return count, err
}

func clusterTransferManifestRecord(identity clusterTransferIdentity) clusterTransferRecord {
	return clusterTransferRecord{
		Type:              clusterTransferRecordManifest,
		ClusterID:         identity.ClusterID,
		MoveID:            identity.MoveID,
		SourceNodeID:      identity.SourceNodeID,
		DestinationNodeID: identity.DestinationNodeID,
		Epoch:             identity.Epoch,
	}
}

func writeClusterTransferKeyTx(tx *storage.Tx, writer *clusterTransferFrameWriter, ns redisNamespace, dbIndex int, key []byte, nowMs int64) error {
	keyType, err := redisKeyTypeTx(tx, ns, key, nowMs)
	if err != nil || keyType == redisKeyTypeNone {
		return err
	}
	expireAtMs := int64(-1)
	if value, found, loadErr := loadRedisExpireAtMsTx(tx, ns, key); loadErr != nil {
		return loadErr
	} else if found {
		expireAtMs = value
	}
	begin := clusterTransferRecord{Type: clusterTransferRecordKeyBegin, DBIndex: dbIndex, Key: cloneBytes(key), RedisType: keyType, ExpireAtMs: expireAtMs}

	var buckets []struct {
		kind   string
		bucket *redisObjectBucket
	}
	switch keyType {
	case redisKeyTypeString:
		bucket, err := tx.GetBucket(ns.stringBucket)
		if err != nil {
			return err
		}
		buckets = append(buckets, struct {
			kind   string
			bucket *redisObjectBucket
		}{clusterTransferBucketString, newRedisObjectBucket(bucket, 0, false)})
	case redisKeyTypeList:
		meta, found, err := loadRedisListMetaForReadTx(tx, ns, key, nowMs)
		if err != nil || !found {
			return err
		}
		begin.Meta1, begin.Meta2, begin.Meta3 = meta.Length, meta.HeadSegID, meta.TailSegID
		bucket, err := openRedisObjectBucketTx(tx, ns.listDataBucket, redisListBucketName(ns, meta.ID), meta.ID, meta.StorageFormat, false)
		if err != nil {
			return err
		}
		buckets = append(buckets, struct {
			kind   string
			bucket *redisObjectBucket
		}{clusterTransferBucketListData, bucket})
	case redisKeyTypeHash:
		meta, found, err := loadRedisHashMetaForReadTx(tx, ns, key, nowMs)
		if err != nil || !found {
			return err
		}
		begin.Meta1 = meta.FieldCount
		bucket, err := openRedisObjectBucketTx(tx, ns.hashDataBucket, redisHashBucketName(ns, meta.ID), meta.ID, meta.StorageFormat, false)
		if err != nil {
			return err
		}
		buckets = append(buckets, struct {
			kind   string
			bucket *redisObjectBucket
		}{clusterTransferBucketHashData, bucket})
	case redisKeyTypeZSet:
		memberBucket, scoreBucket, meta, found, err := getRedisZSetBucketsTx(tx, ns, key, nowMs)
		if err != nil || !found {
			return err
		}
		begin.Meta1 = meta.Cardinality
		buckets = append(buckets,
			struct {
				kind   string
				bucket *redisObjectBucket
			}{clusterTransferBucketZSetMember, memberBucket},
			struct {
				kind   string
				bucket *redisObjectBucket
			}{clusterTransferBucketZSetScore, scoreBucket},
		)
	case redisKeyTypeBloom:
		meta, found, err := loadRedisBloomMetaForReadTx(tx, ns, key, nowMs)
		if err != nil || !found {
			return err
		}
		begin.Meta1, begin.Meta2, begin.Meta3 = meta.InitialCapacity, meta.ErrorRateBits, meta.Expansion
		begin.Meta4, begin.Meta5 = meta.Flags, meta.SubFilterCount
		bucket, err := getRedisBloomDataBucketTx(tx, ns, meta)
		if err != nil {
			return err
		}
		buckets = append(buckets, struct {
			kind   string
			bucket *redisObjectBucket
		}{clusterTransferBucketBloomData, bucket})
	case redisKeyTypeTopK:
		meta, found, err := loadRedisTopKMetaForReadTx(tx, ns, key, nowMs)
		if err != nil || !found {
			return err
		}
		begin.Meta1, begin.Meta2, begin.Meta3 = meta.K, meta.Width, meta.Depth
		begin.Meta4, begin.Meta5 = meta.HeapSize, meta.DecayBits
		bucket, err := getRedisTopKDataBucketTx(tx, ns, meta)
		if err != nil {
			return err
		}
		buckets = append(buckets, struct {
			kind   string
			bucket *redisObjectBucket
		}{clusterTransferBucketTopKData, bucket})
	default:
		return fmt.Errorf("unsupported redis type %q in cluster transfer", keyType)
	}

	if err = writer.writeRecord(begin); err != nil {
		return err
	}
	for _, item := range buckets {
		if keyType == redisKeyTypeString {
			if err = writeClusterTransferBucketEntryTx(item.bucket, writer, begin, item.kind, key); err != nil {
				return err
			}
			continue
		}
		cursor := item.bucket.Cursor()
		for entryKey, _ := cursor.First(); entryKey != nil; entryKey, _ = cursor.Next() {
			if err = writeClusterTransferBucketEntryTx(item.bucket, writer, begin, item.kind, entryKey); err != nil {
				return err
			}
		}
		if err = cursor.Err(); err != nil {
			return err
		}
	}
	end := begin
	end.Type = clusterTransferRecordKeyEnd
	return writer.writeRecord(end)
}

func writeClusterTransferBucketEntryTx(bucket *redisObjectBucket, writer *clusterTransferFrameWriter, keyRecord clusterTransferRecord, bucketKind string, entryKey []byte) error {
	valueLen, found, err := bucket.ValueLen(entryKey)
	if err != nil {
		return err
	}
	if !found {
		return errors.New("cluster transfer bucket entry disappeared")
	}
	entry := clusterTransferRecord{
		Type:       clusterTransferRecordBucketEntryBegin,
		DBIndex:    keyRecord.DBIndex,
		Key:        cloneBytes(keyRecord.Key),
		RedisType:  keyRecord.RedisType,
		ExpireAtMs: keyRecord.ExpireAtMs,
		BucketKind: bucketKind,
		EntryKey:   cloneBytes(entryKey),
		ValueLen:   uint64(valueLen),
	}
	if err = writer.writeRecord(entry); err != nil {
		return err
	}
	valueWriter := &clusterTransferValueWriter{writer: writer, record: entry}
	written, found, err := bucket.WriteValueTo(entryKey, valueWriter)
	if err != nil {
		return err
	}
	if !found || written != int64(valueLen) || valueWriter.offset != uint64(valueLen) {
		return errors.New("cluster transfer bucket entry length changed")
	}
	entry.Type = clusterTransferRecordBucketEntryEnd
	entry.Offset = valueWriter.offset
	return writer.writeRecord(entry)
}

type clusterTransferValueWriter struct {
	writer *clusterTransferFrameWriter
	record clusterTransferRecord
	offset uint64
}

func (w *clusterTransferValueWriter) Write(p []byte) (int, error) {
	written := 0
	for len(p) > 0 {
		chunkSize := min(len(p), clusterTransferValueChunkSize)
		record := w.record
		record.Type = clusterTransferRecordValueChunk
		record.Offset = w.offset
		record.Data = cloneBytes(p[:chunkSize])
		if err := w.writer.writeRecord(record); err != nil {
			return written, err
		}
		w.offset += uint64(chunkSize)
		written += chunkSize
		p = p[chunkSize:]
	}
	return written, nil
}

func importClusterSlotStream(db *storage.DB, r io.Reader, nowMs int64) (clusterSlotTransferStats, error) {
	return importClusterSlotStreamWithReceiptAndIdentity(db, r, nowMs, clusterImportReceiptContext{}, clusterTransferIdentity{})
}

func importClusterSlotStreamWithReceipt(db *storage.DB, r io.Reader, nowMs int64, receiptContext clusterImportReceiptContext) (clusterSlotTransferStats, error) {
	return importClusterSlotStreamWithReceiptAndIdentity(db, r, nowMs, receiptContext, clusterTransferIdentity{})
}

func importClusterSlotStreamWithReceiptAndIdentity(db *storage.DB, r io.Reader, nowMs int64, receiptContext clusterImportReceiptContext, expectedIdentity clusterTransferIdentity) (clusterSlotTransferStats, error) {
	buffered := bufio.NewReader(r)
	counter := &countingReader{r: buffered}
	header, err := readClusterTransferHeader(counter)
	if err != nil {
		return clusterSlotTransferStats{}, err
	}
	if header.SlotCount <= 0 || header.StartSlot < 0 || header.EndSlot < header.StartSlot || header.EndSlot >= header.SlotCount {
		return clusterSlotTransferStats{}, errors.New("cluster transfer header slot range is invalid")
	}
	stats := clusterSlotTransferStats{}
	expectedSequence := uint64(1)
	frames := make([]clusterTransferDecodedFrame, 0, 128)
	batchBytes := 0
	flush := func() error {
		if len(frames) == 0 {
			return nil
		}
		applied, err := applyClusterTransferRecords(db, receiptContext, frames, header.SlotCount, header.StartSlot, header.EndSlot, nowMs)
		if err != nil {
			return err
		}
		for index, frame := range frames {
			if applied[index] {
				stats.RecordsTransferred++
			}
			if frame.record.Type == clusterTransferRecordKeyEnd && applied[index] {
				stats.EntriesTransferred++
			}
		}
		frames = frames[:0]
		batchBytes = 0
		return nil
	}
	for {
		sequence, checksum, record, err := readClusterTransferRecord(counter)
		if err != nil {
			return clusterSlotTransferStats{}, err
		}
		if sequence != expectedSequence {
			return clusterSlotTransferStats{}, fmt.Errorf("cluster transfer sequence %d arrived, expected %d", sequence, expectedSequence)
		}
		if sequence == 1 {
			if record.Type != clusterTransferRecordManifest {
				return clusterSlotTransferStats{}, errors.New("cluster transfer manifest must be the first frame")
			}
			if err = validateClusterTransferManifest(record, expectedIdentity); err != nil {
				return clusterSlotTransferStats{}, err
			}
		} else if record.Type == clusterTransferRecordManifest {
			return clusterSlotTransferStats{}, errors.New("cluster transfer manifest may only appear once")
		}
		expectedSequence++
		frames = append(frames, clusterTransferDecodedFrame{sequence: sequence, checksum: checksum, record: record})
		batchBytes += len(record.Data) + len(record.Key) + len(record.EntryKey) + 128
		if len(frames) >= 128 || batchBytes >= 8*1024*1024 || record.Type == clusterTransferRecordStreamEnd {
			if err = flush(); err != nil {
				return clusterSlotTransferStats{}, err
			}
		}
		if record.Type == clusterTransferRecordStreamEnd {
			stats.BytesTransferred = uint64(counter.n)
			stats.EOF = true
			return stats, nil
		}
	}
}

func validateClusterTransferManifest(record clusterTransferRecord, expected clusterTransferIdentity) error {
	if len(record.ClusterID) > 256 || len(record.MoveID) > 256 || len(record.SourceNodeID) > 256 || len(record.DestinationNodeID) > 256 {
		return errors.New("cluster transfer identity is too large")
	}
	if expected.ClusterID != "" && record.ClusterID != expected.ClusterID {
		return errors.New("cluster transfer cluster id does not match")
	}
	if expected.MoveID != "" && record.MoveID != expected.MoveID {
		return errors.New("cluster transfer move id does not match")
	}
	if expected.SourceNodeID != "" && record.SourceNodeID != expected.SourceNodeID {
		return errors.New("cluster transfer source node does not match")
	}
	if expected.DestinationNodeID != "" && record.DestinationNodeID != expected.DestinationNodeID {
		return errors.New("cluster transfer destination node does not match")
	}
	if expected.ClusterID != "" && record.Epoch != expected.Epoch {
		return errors.New("cluster transfer epoch is stale")
	}
	return nil
}

func readClusterTransferHeader(r io.Reader) (clusterSlotStreamHeader, error) {
	var magic [8]byte
	if _, err := io.ReadFull(r, magic[:]); err != nil {
		return clusterSlotStreamHeader{}, err
	}
	if magic != clusterSlotStreamMagic {
		return clusterSlotStreamHeader{}, errors.New("invalid cluster transfer magic")
	}
	var version uint16
	if err := binary.Read(r, binary.LittleEndian, &version); err != nil {
		return clusterSlotStreamHeader{}, err
	}
	if version != clusterTransferProtocolVersion {
		return clusterSlotStreamHeader{}, fmt.Errorf("unsupported cluster transfer version %d", version)
	}
	var slotCount, startSlot, endSlot uint32
	if err := binary.Read(r, binary.LittleEndian, &slotCount); err != nil {
		return clusterSlotStreamHeader{}, err
	}
	if err := binary.Read(r, binary.LittleEndian, &startSlot); err != nil {
		return clusterSlotStreamHeader{}, err
	}
	if err := binary.Read(r, binary.LittleEndian, &endSlot); err != nil {
		return clusterSlotStreamHeader{}, err
	}
	return clusterSlotStreamHeader{Version: version, SlotCount: int(slotCount), StartSlot: int(startSlot), EndSlot: int(endSlot)}, nil
}

func applyClusterTransferRecords(db *storage.DB, receiptContext clusterImportReceiptContext, frames []clusterTransferDecodedFrame, slotCount int, startSlot int, endSlot int, nowMs int64) ([]bool, error) {
	applied := make([]bool, len(frames))
	err := db.Update(func(tx *storage.Tx) error {
		var receiptBucket *storage.Bucket
		manifest := clusterImportManifest{SchemaVersion: 1}
		if receiptContext.JobID != "" {
			var err error
			receiptBucket, err = getOrMigrateClusterImportReceiptBucketTx(tx, receiptContext)
			if err != nil {
				return err
			}
			manifest, err = loadClusterImportManifest(receiptBucket, receiptContext)
			if err != nil {
				return err
			}
		}
		for index, frame := range frames {
			sequence := frame.sequence
			checksum := frame.checksum
			record := frame.record
			if record.Type != clusterTransferRecordManifest && record.Type != clusterTransferRecordStreamEnd {
				if record.DBIndex < redisDatabaseMin || record.DBIndex > redisDatabaseMax || len(record.Key) == 0 || len(record.Key) >= storage.MaxKeySize {
					return errors.New("cluster transfer key metadata is invalid")
				}
				slot := ClusterKeySlot(record.Key, slotCount)
				if slot < startSlot || slot > endSlot {
					return errors.New("cluster transfer key is outside the declared slot range")
				}
			}
			if receiptBucket != nil {
				receiptKey := clusterImportReceiptKey(receiptContext, sequence)
				if raw, found := receiptBucket.Get(receiptKey); found {
					if len(raw) != 4 || binary.BigEndian.Uint32(raw) != checksum {
						return errors.New("cluster transfer retry checksum does not match committed frame")
					}
					if record.Type == clusterTransferRecordStreamEnd && !manifest.Complete {
						manifest.Complete = true
						manifest.UpdatedAtMs = nowMs
						if err := saveClusterImportManifest(receiptBucket, receiptContext, manifest); err != nil {
							return err
						}
					}
					continue
				}
				if sequence != manifest.LastSequence+1 {
					return fmt.Errorf("cluster transfer frame %d is out of order after %d", sequence, manifest.LastSequence)
				}
			}

			if err := applyClusterTransferRecordTx(tx, receiptContext, record, slotCount, nowMs); err != nil {
				return err
			}
			applied[index] = true
			if receiptBucket != nil {
				checksumBytes := make([]byte, 4)
				binary.BigEndian.PutUint32(checksumBytes, checksum)
				if err := receiptBucket.Put(clusterImportReceiptKey(receiptContext, sequence), checksumBytes); err != nil {
					return err
				}
				manifest.LastSequence = sequence
				manifest.FramesApplied++
				manifest.UpdatedAtMs = nowMs
				if record.Type == clusterTransferRecordKeyEnd || record.Type == clusterTransferRecordKeyDelete {
					manifest.KeysCompleted++
				}
				if record.Type == clusterTransferRecordStreamEnd {
					manifest.Complete = true
				}
				if err := saveClusterImportManifest(receiptBucket, receiptContext, manifest); err != nil {
					return err
				}
			}
		}
		return nil
	})
	return applied, err
}

func applyClusterTransferRecordTx(tx *storage.Tx, receiptContext clusterImportReceiptContext, record clusterTransferRecord, slotCount int, nowMs int64) error {
	switch record.Type {
	case clusterTransferRecordManifest:
		return nil
	case clusterTransferRecordKeyBegin:
		return beginClusterTransferKeyTx(tx, receiptContext, record, slotCount)
	case clusterTransferRecordBucketEntryBegin:
		return beginClusterTransferEntryTx(tx, receiptContext, record)
	case clusterTransferRecordValueChunk:
		return appendClusterTransferValueChunkTx(tx, receiptContext, record)
	case clusterTransferRecordBucketEntryEnd:
		return finishClusterTransferEntryTx(tx, receiptContext, record, slotCount)
	case clusterTransferRecordKeyEnd:
		return finishClusterTransferKeyTx(tx, record, slotCount, nowMs)
	case clusterTransferRecordKeyDelete:
		return deleteClusterTransferKeyTx(tx, record, slotCount)
	case clusterTransferRecordStreamEnd:
		return nil
	default:
		return fmt.Errorf("unsupported cluster transfer record type %d", record.Type)
	}
}

func deleteClusterTransferKeyTx(tx *storage.Tx, record clusterTransferRecord, slotCount int) error {
	ns := redisNamespaceForDB(record.DBIndex, slotCount)
	if state, found, err := loadClusterImportKeyStateTx(tx, record.DBIndex, record.Key); err != nil {
		return err
	} else if found {
		if err = cleanupClusterImportKeyStateTx(tx, state, slotCount); err != nil {
			return err
		}
	}
	rawType, err := redisRawKeyTypeTx(tx, ns, record.Key)
	if err != nil {
		return err
	}
	if _, err = deleteRedisKeyByRawTypeTx(tx, ns, record.Key, rawType); err != nil {
		return err
	}
	return deleteRedisExpireAtMsTx(tx, ns, record.Key)
}

func beginClusterTransferKeyTx(tx *storage.Tx, receiptContext clusterImportReceiptContext, record clusterTransferRecord, slotCount int) error {
	if record.DBIndex < redisDatabaseMin || record.DBIndex > redisDatabaseMax || len(record.Key) == 0 {
		return errors.New("cluster transfer key metadata is invalid")
	}
	ns := redisNamespaceForDB(record.DBIndex, slotCount)
	if prior, found, err := loadClusterImportKeyStateTx(tx, record.DBIndex, record.Key); err != nil {
		return err
	} else if found {
		if err = cleanupClusterImportKeyStateTx(tx, prior, slotCount); err != nil {
			return err
		}
	}
	rawType, err := redisRawKeyTypeTx(tx, ns, record.Key)
	if err != nil {
		return err
	}
	if _, err = deleteRedisKeyByRawTypeTx(tx, ns, record.Key, rawType); err != nil {
		return err
	}
	if err = deleteRedisExpireAtMsTx(tx, ns, record.Key); err != nil {
		return err
	}
	state := clusterImportKeyState{
		SchemaVersion:  clusterImportKeyStateSchemaVersion,
		Begin:          record,
		ReceiptContext: receiptContext,
	}
	switch record.RedisType {
	case redisKeyTypeString:
		// The value remains in its move-scoped stage bucket until KeyEnd.
	case redisKeyTypeList:
		id, err := nextRedisListIDTx(tx, ns)
		if err != nil {
			return err
		}
		meta := &redisListMeta{ID: id, Length: record.Meta1, HeadSegID: record.Meta2, TailSegID: record.Meta3, StorageFormat: redisKeyStorageShared}
		if _, err = ensureRedisListDataBucketTx(tx, ns, meta); err != nil {
			return err
		}
		state.AllocatedID = id
	case redisKeyTypeHash:
		id, err := nextRedisHashIDTx(tx, ns)
		if err != nil {
			return err
		}
		meta := &redisHashMeta{ID: id, FieldCount: record.Meta1, StorageFormat: redisKeyStorageShared}
		if _, err = ensureRedisHashDataBucketTx(tx, ns, meta); err != nil {
			return err
		}
		state.AllocatedID = id
	case redisKeyTypeZSet:
		id, err := nextRedisZSetIDTx(tx, ns)
		if err != nil {
			return err
		}
		meta := &redisZSetMeta{ID: id, Cardinality: record.Meta1, StorageFormat: redisKeyStorageShared}
		if _, err = ensureRedisZSetMemberBucketTx(tx, ns, meta); err != nil {
			return err
		}
		if _, err = ensureRedisZSetScoreBucketTx(tx, ns, meta); err != nil {
			return err
		}
		state.AllocatedID = id
	case redisKeyTypeBloom:
		id, err := nextRedisBloomIDTx(tx, ns)
		if err != nil {
			return err
		}
		meta := &redisBloomMeta{ID: id, InitialCapacity: record.Meta1, ErrorRateBits: record.Meta2, Expansion: record.Meta3, Flags: record.Meta4, SubFilterCount: record.Meta5, StorageFormat: redisKeyStorageShared}
		if _, err = ensureRedisBloomDataBucketTx(tx, ns, meta); err != nil {
			return err
		}
		state.AllocatedID = id
	case redisKeyTypeTopK:
		id, err := nextRedisTopKIDTx(tx, ns)
		if err != nil {
			return err
		}
		meta := &redisTopKMeta{ID: id, K: record.Meta1, Width: record.Meta2, Depth: record.Meta3, HeapSize: record.Meta4, DecayBits: record.Meta5, StorageFormat: redisKeyStorageShared}
		if _, err = ensureRedisTopKDataBucketTx(tx, ns, meta); err != nil {
			return err
		}
		state.AllocatedID = id
	default:
		return fmt.Errorf("unsupported cluster transfer redis type %q", record.RedisType)
	}
	return saveClusterImportKeyStateTx(tx, state)
}

func beginClusterTransferEntryTx(tx *storage.Tx, receiptContext clusterImportReceiptContext, record clusterTransferRecord) error {
	if record.ValueLen >= storage.OneGigabyte || len(record.EntryKey) >= storage.MaxKeySize {
		return errors.New("cluster transfer entry size is invalid")
	}
	stageName := clusterImportStageBucketName(receiptContext, record)
	if err := tx.DeleteBucket(stageName); err != nil && !errors.Is(err, storage.ErrBucketNotFound) {
		return err
	}
	bucket, err := tx.CreateBucket(stageName)
	if err != nil {
		return err
	}
	return bucket.Put([]byte{0}, encodeClusterImportStageMeta(clusterImportStageMeta{ValueLen: record.ValueLen}))
}

func appendClusterTransferValueChunkTx(tx *storage.Tx, receiptContext clusterImportReceiptContext, record clusterTransferRecord) error {
	if len(record.Data) == 0 || len(record.Data) > clusterTransferValueChunkSize {
		return errors.New("cluster transfer value chunk size is invalid")
	}
	bucket, err := tx.GetBucket(clusterImportStageBucketName(receiptContext, record))
	if err != nil {
		return err
	}
	rawMeta, found := bucket.Get([]byte{0})
	if !found {
		return errors.New("cluster transfer stage metadata is missing")
	}
	meta, err := decodeClusterImportStageMeta(rawMeta)
	if err != nil {
		return err
	}
	if record.Offset != meta.NextOffset || record.Offset+uint64(len(record.Data)) > meta.ValueLen {
		return errors.New("cluster transfer value chunk offset is invalid")
	}
	chunkKey := make([]byte, 9)
	chunkKey[0] = 1
	binary.BigEndian.PutUint64(chunkKey[1:], record.Offset)
	if err = bucket.Put(chunkKey, record.Data); err != nil {
		return err
	}
	meta.NextOffset += uint64(len(record.Data))
	return bucket.Put([]byte{0}, encodeClusterImportStageMeta(meta))
}

func finishClusterTransferEntryTx(tx *storage.Tx, receiptContext clusterImportReceiptContext, record clusterTransferRecord, slotCount int) error {
	stageName := clusterImportStageBucketName(receiptContext, record)
	stage, err := tx.GetBucket(stageName)
	if err != nil {
		return err
	}
	rawMeta, found := stage.Get([]byte{0})
	if !found {
		return errors.New("cluster transfer stage metadata is missing")
	}
	meta, err := decodeClusterImportStageMeta(rawMeta)
	if err != nil {
		return err
	}
	if meta.ValueLen != record.ValueLen || meta.NextOffset != record.ValueLen || record.Offset != record.ValueLen {
		return errors.New("cluster transfer staged value is incomplete")
	}
	if record.BucketKind == clusterTransferBucketString {
		// Publishing a string before KeyEnd would expose a partial logical key.
		return nil
	}
	target, err := resolveClusterTransferTargetBucketTx(tx, record, slotCount)
	if err != nil {
		return err
	}
	reader := newClusterImportStageReader(stage)
	if err = target.PutReader(record.EntryKey, reader, int64(record.ValueLen)); err != nil {
		return err
	}
	if reader.read != record.ValueLen {
		return errors.New("cluster transfer staged value length mismatch")
	}
	return tx.DeleteBucket(stageName)
}

func finishClusterTransferKeyTx(tx *storage.Tx, record clusterTransferRecord, slotCount int, nowMs int64) error {
	state, found, err := loadClusterImportKeyStateTx(tx, record.DBIndex, record.Key)
	if err != nil {
		return err
	}
	if !found {
		return errors.New("cluster transfer key state is missing")
	}
	if state.Begin.RedisType != record.RedisType || state.Begin.ExpireAtMs != record.ExpireAtMs {
		return errors.New("cluster transfer key end does not match key begin")
	}
	ns := redisNamespaceForDB(state.Begin.DBIndex, slotCount)
	switch state.Begin.RedisType {
	case redisKeyTypeString:
		entry := state.Begin
		entry.BucketKind = clusterTransferBucketString
		entry.EntryKey = cloneBytes(state.Begin.Key)
		stageName := clusterImportStageBucketName(state.ReceiptContext, entry)
		stage, stageErr := tx.GetBucket(stageName)
		if stageErr != nil {
			return stageErr
		}
		rawMeta, metaFound := stage.Get([]byte{0})
		if !metaFound {
			return errors.New("cluster transfer string stage metadata is missing")
		}
		meta, decodeErr := decodeClusterImportStageMeta(rawMeta)
		if decodeErr != nil {
			return decodeErr
		}
		if meta.NextOffset != meta.ValueLen {
			return errors.New("cluster transfer string stage is incomplete")
		}
		bucket, bucketErr := tx.CreateBucketIfNotExists(ns.stringBucket)
		if bucketErr != nil {
			return bucketErr
		}
		reader := newClusterImportStageReader(stage)
		if bucketErr = bucket.PutReader(state.Begin.Key, reader, int64(meta.ValueLen)); bucketErr != nil {
			return bucketErr
		}
		if reader.read != meta.ValueLen {
			return errors.New("cluster transfer staged string length mismatch")
		}
		if bucketErr = saveRedisKeyMetaForTypeTx(tx, ns, state.Begin.Key, redisKeyTypeString, 0); bucketErr != nil {
			return bucketErr
		}
		if bucketErr = tx.DeleteBucket(stageName); bucketErr != nil {
			return bucketErr
		}
	case redisKeyTypeList:
		err = saveRedisListMetaTx(tx, ns, state.Begin.Key, &redisListMeta{ID: state.AllocatedID, Length: state.Begin.Meta1, HeadSegID: state.Begin.Meta2, TailSegID: state.Begin.Meta3, StorageFormat: redisKeyStorageShared})
	case redisKeyTypeHash:
		err = saveRedisHashMetaTx(tx, ns, state.Begin.Key, &redisHashMeta{ID: state.AllocatedID, FieldCount: state.Begin.Meta1, StorageFormat: redisKeyStorageShared})
	case redisKeyTypeZSet:
		err = saveRedisZSetMetaTx(tx, ns, state.Begin.Key, &redisZSetMeta{ID: state.AllocatedID, Cardinality: state.Begin.Meta1, StorageFormat: redisKeyStorageShared})
	case redisKeyTypeBloom:
		err = saveRedisBloomMetaTx(tx, ns, state.Begin.Key, &redisBloomMeta{ID: state.AllocatedID, InitialCapacity: state.Begin.Meta1, ErrorRateBits: state.Begin.Meta2, Expansion: state.Begin.Meta3, Flags: state.Begin.Meta4, SubFilterCount: state.Begin.Meta5, StorageFormat: redisKeyStorageShared})
	case redisKeyTypeTopK:
		err = saveRedisTopKMetaTx(tx, ns, state.Begin.Key, &redisTopKMeta{ID: state.AllocatedID, K: state.Begin.Meta1, Width: state.Begin.Meta2, Depth: state.Begin.Meta3, HeapSize: state.Begin.Meta4, DecayBits: state.Begin.Meta5, StorageFormat: redisKeyStorageShared})
	default:
		return fmt.Errorf("unsupported cluster transfer redis type %q", state.Begin.RedisType)
	}
	if err != nil {
		return err
	}
	if err := ensureRedisSlotIndexEntryTx(tx, ns, record.Key); err != nil {
		return err
	}
	if state.Begin.ExpireAtMs < 0 {
		err = deleteRedisExpireAtMsTx(tx, ns, record.Key)
	} else if state.Begin.ExpireAtMs <= nowMs {
		rawType, err := redisRawKeyTypeTx(tx, ns, record.Key)
		if err != nil {
			return err
		}
		_, err = deleteRedisKeyByRawTypeTx(tx, ns, record.Key, rawType)
	} else {
		_, err = redisExpireKeyAtTx(tx, ns, record.Key, state.Begin.ExpireAtMs, nowMs)
	}
	if err != nil {
		return err
	}
	return deleteClusterImportKeyStateTx(tx, record.DBIndex, record.Key)
}

func resolveClusterTransferTargetBucketTx(tx *storage.Tx, record clusterTransferRecord, slotCount int) (*redisObjectBucket, error) {
	ns := redisNamespaceForDB(record.DBIndex, slotCount)
	state, found, err := loadClusterImportKeyStateTx(tx, record.DBIndex, record.Key)
	if err != nil {
		return nil, err
	}
	if !found || state.Begin.RedisType != record.RedisType {
		return nil, errors.New("cluster transfer key state is missing")
	}
	switch record.BucketKind {
	case clusterTransferBucketString:
		return nil, errors.New("cluster transfer string is published only at key end")
	case clusterTransferBucketListData:
		meta := &redisListMeta{ID: state.AllocatedID, Length: state.Begin.Meta1, HeadSegID: state.Begin.Meta2, TailSegID: state.Begin.Meta3, StorageFormat: redisKeyStorageShared}
		return ensureRedisListDataBucketTx(tx, ns, meta)
	case clusterTransferBucketHashData:
		meta := &redisHashMeta{ID: state.AllocatedID, FieldCount: state.Begin.Meta1, StorageFormat: redisKeyStorageShared}
		return ensureRedisHashDataBucketTx(tx, ns, meta)
	case clusterTransferBucketZSetMember:
		meta := &redisZSetMeta{ID: state.AllocatedID, Cardinality: state.Begin.Meta1, StorageFormat: redisKeyStorageShared}
		return ensureRedisZSetMemberBucketTx(tx, ns, meta)
	case clusterTransferBucketZSetScore:
		meta := &redisZSetMeta{ID: state.AllocatedID, Cardinality: state.Begin.Meta1, StorageFormat: redisKeyStorageShared}
		return ensureRedisZSetScoreBucketTx(tx, ns, meta)
	case clusterTransferBucketBloomData:
		meta := &redisBloomMeta{ID: state.AllocatedID, InitialCapacity: state.Begin.Meta1, ErrorRateBits: state.Begin.Meta2, Expansion: state.Begin.Meta3, Flags: state.Begin.Meta4, SubFilterCount: state.Begin.Meta5, StorageFormat: redisKeyStorageShared}
		return ensureRedisBloomDataBucketTx(tx, ns, meta)
	case clusterTransferBucketTopKData:
		meta := &redisTopKMeta{ID: state.AllocatedID, K: state.Begin.Meta1, Width: state.Begin.Meta2, Depth: state.Begin.Meta3, HeapSize: state.Begin.Meta4, DecayBits: state.Begin.Meta5, StorageFormat: redisKeyStorageShared}
		return ensureRedisTopKDataBucketTx(tx, ns, meta)
	default:
		return nil, fmt.Errorf("unsupported cluster transfer bucket kind %q", record.BucketKind)
	}
}

func clusterImportKeyStateKey(dbIndex int, key []byte) []byte {
	digest := sha256.New()
	var dbBuf [8]byte
	binary.BigEndian.PutUint64(dbBuf[:], uint64(dbIndex))
	_, _ = digest.Write(dbBuf[:])
	_, _ = digest.Write(key)
	return digest.Sum(nil)
}

func saveClusterImportKeyStateTx(tx *storage.Tx, state clusterImportKeyState) error {
	state.SchemaVersion = clusterImportKeyStateSchemaVersion
	var encoded bytes.Buffer
	if err := gob.NewEncoder(&encoded).Encode(state); err != nil {
		return err
	}
	bucket, err := tx.CreateBucketIfNotExists([]byte(clusterImportKeyStateBucketNameConst))
	if err != nil {
		return err
	}
	return bucket.Put(clusterImportKeyStateKey(state.Begin.DBIndex, state.Begin.Key), encoded.Bytes())
}

func loadClusterImportKeyStateTx(tx *storage.Tx, dbIndex int, key []byte) (clusterImportKeyState, bool, error) {
	bucket, err := tx.GetBucket([]byte(clusterImportKeyStateBucketNameConst))
	if errors.Is(err, storage.ErrBucketNotFound) {
		return clusterImportKeyState{}, false, nil
	}
	if err != nil {
		return clusterImportKeyState{}, false, err
	}
	raw, found := bucket.Get(clusterImportKeyStateKey(dbIndex, key))
	if !found {
		return clusterImportKeyState{}, false, nil
	}
	var state clusterImportKeyState
	if err = gob.NewDecoder(bytes.NewReader(raw)).Decode(&state); err != nil {
		return clusterImportKeyState{}, false, err
	}
	if state.SchemaVersion != clusterImportKeyStateSchemaVersion || state.Begin.DBIndex != dbIndex || !bytes.Equal(state.Begin.Key, key) {
		return clusterImportKeyState{}, false, errors.New("cluster import key state is corrupted")
	}
	return state, true, nil
}

func deleteClusterImportKeyStateTx(tx *storage.Tx, dbIndex int, key []byte) error {
	bucket, err := tx.GetBucket([]byte(clusterImportKeyStateBucketNameConst))
	if errors.Is(err, storage.ErrBucketNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	err = bucket.Remove(clusterImportKeyStateKey(dbIndex, key))
	if errors.Is(err, storage.ErrNodeNotFound) {
		return nil
	}
	return err
}

func cleanupClusterImportKeyStateTx(tx *storage.Tx, state clusterImportKeyState, slotCount int) error {
	ns := redisNamespaceForDB(state.Begin.DBIndex, slotCount)
	deleteBucket := func(name []byte) error {
		err := tx.DeleteBucket(name)
		if errors.Is(err, storage.ErrBucketNotFound) {
			return nil
		}
		return err
	}
	deleteSharedObject := func(sharedName, legacyName []byte) error {
		store, err := openRedisObjectBucketTx(tx, sharedName, legacyName, state.AllocatedID, redisKeyStorageShared, false)
		if errors.Is(err, storage.ErrBucketNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		return deleteRedisObjectBucketTx(tx, store, legacyName)
	}
	var err error
	switch state.Begin.RedisType {
	case redisKeyTypeString:
		entry := state.Begin
		entry.BucketKind = clusterTransferBucketString
		entry.EntryKey = cloneBytes(state.Begin.Key)
		err = deleteBucket(clusterImportStageBucketName(state.ReceiptContext, entry))
	case redisKeyTypeList:
		err = deleteSharedObject(ns.listDataBucket, redisListBucketName(ns, state.AllocatedID))
	case redisKeyTypeHash:
		err = deleteSharedObject(ns.hashDataBucket, redisHashBucketName(ns, state.AllocatedID))
	case redisKeyTypeZSet:
		if err = deleteSharedObject(ns.zsetMemberBucket, redisZSetMemberBucketName(ns, state.AllocatedID)); err == nil {
			err = deleteSharedObject(ns.zsetScoreBucket, redisZSetScoreBucketName(ns, state.AllocatedID))
		}
	case redisKeyTypeBloom:
		err = deleteSharedObject(ns.bloomDataBucket, redisBloomDataBucketName(ns, state.AllocatedID))
	case redisKeyTypeTopK:
		err = deleteSharedObject(ns.topkDataBucket, redisTopKDataBucketName(ns, state.AllocatedID))
	default:
		err = fmt.Errorf("unsupported staged cluster transfer redis type %q", state.Begin.RedisType)
	}
	if err != nil {
		return err
	}
	return deleteClusterImportKeyStateTx(tx, state.Begin.DBIndex, state.Begin.Key)
}

func clusterImportReceiptBase(receiptContext clusterImportReceiptContext) [16]byte {
	digest := sha256.Sum256([]byte(fmt.Sprintf("%s:%d", receiptContext.JobID, receiptContext.ChunkIndex)))
	var base [16]byte
	copy(base[:], digest[:16])
	return base
}

func clusterImportReceiptBucketName(receiptContext clusterImportReceiptContext) []byte {
	base := clusterImportReceiptBase(receiptContext)
	return []byte(fmt.Sprintf("%s%x", clusterImportReceiptBucketPrefix, base[:]))
}

func getOrMigrateClusterImportReceiptBucketTx(tx *storage.Tx, receiptContext clusterImportReceiptContext) (*storage.Bucket, error) {
	bucket, err := tx.GetBucket(clusterImportReceiptBucketName(receiptContext))
	if err == nil {
		return bucket, nil
	}
	if !errors.Is(err, storage.ErrBucketNotFound) {
		return nil, err
	}
	bucket, err = tx.CreateBucket(clusterImportReceiptBucketName(receiptContext))
	if err != nil {
		return nil, err
	}
	legacy, legacyErr := tx.GetBucket([]byte(clusterImportReceiptsBucketNameConst))
	if errors.Is(legacyErr, storage.ErrBucketNotFound) {
		return bucket, nil
	}
	if legacyErr != nil {
		return nil, legacyErr
	}
	manifestKey := clusterImportManifestKey(receiptContext)
	if raw, found := legacy.Get(manifestKey); found {
		if err = bucket.Put(manifestKey, raw); err != nil {
			return nil, err
		}
	}
	prefix := clusterImportReceiptKey(receiptContext, 0)[:17]
	cursor := legacy.Cursor()
	for key, value := cursor.Seek(prefix); key != nil && bytes.HasPrefix(key, prefix); key, value = cursor.Next() {
		if err = bucket.Put(key, value); err != nil {
			return nil, err
		}
	}
	return bucket, nil
}

func clusterImportReceiptKey(receiptContext clusterImportReceiptContext, sequence uint64) []byte {
	base := clusterImportReceiptBase(receiptContext)
	key := make([]byte, 25)
	key[0] = 'r'
	copy(key[1:17], base[:])
	binary.BigEndian.PutUint64(key[17:], sequence)
	return key
}

func clusterImportManifestKey(receiptContext clusterImportReceiptContext) []byte {
	base := clusterImportReceiptBase(receiptContext)
	key := make([]byte, 17)
	key[0] = 'm'
	copy(key[1:], base[:])
	return key
}

func loadClusterImportManifest(bucket *storage.Bucket, receiptContext clusterImportReceiptContext) (clusterImportManifest, error) {
	raw, found := bucket.Get(clusterImportManifestKey(receiptContext))
	if !found {
		return clusterImportManifest{SchemaVersion: 1}, nil
	}
	if len(raw) == 8 {
		return clusterImportManifest{SchemaVersion: 1, LastSequence: binary.BigEndian.Uint64(raw)}, nil
	}
	if len(raw) != 34 || raw[0] != 1 {
		return clusterImportManifest{}, errors.New("cluster import manifest is corrupted")
	}
	return clusterImportManifest{
		SchemaVersion: 1,
		LastSequence:  binary.BigEndian.Uint64(raw[1:9]),
		FramesApplied: binary.BigEndian.Uint64(raw[9:17]),
		KeysCompleted: binary.BigEndian.Uint64(raw[17:25]),
		Complete:      raw[25] == 1,
		UpdatedAtMs:   int64(binary.BigEndian.Uint64(raw[26:34])),
	}, nil
}

func saveClusterImportManifest(bucket *storage.Bucket, receiptContext clusterImportReceiptContext, manifest clusterImportManifest) error {
	raw := make([]byte, 34)
	raw[0] = 1
	binary.BigEndian.PutUint64(raw[1:9], manifest.LastSequence)
	binary.BigEndian.PutUint64(raw[9:17], manifest.FramesApplied)
	binary.BigEndian.PutUint64(raw[17:25], manifest.KeysCompleted)
	if manifest.Complete {
		raw[25] = 1
	}
	binary.BigEndian.PutUint64(raw[26:34], uint64(manifest.UpdatedAtMs))
	return bucket.Put(clusterImportManifestKey(receiptContext), raw)
}

func loadClusterImportManifestFromDB(db *storage.DB, receiptContext clusterImportReceiptContext) (clusterImportManifest, error) {
	var manifest clusterImportManifest
	err := db.View(func(tx *storage.Tx) error {
		bucket, err := tx.GetBucket(clusterImportReceiptBucketName(receiptContext))
		if errors.Is(err, storage.ErrBucketNotFound) {
			bucket, err = tx.GetBucket([]byte(clusterImportReceiptsBucketNameConst))
		}
		if errors.Is(err, storage.ErrBucketNotFound) {
			manifest = clusterImportManifest{SchemaVersion: 1}
			return nil
		}
		if err != nil {
			return err
		}
		manifest, err = loadClusterImportManifest(bucket, receiptContext)
		return err
	})
	return manifest, err
}

func deleteClusterImportJobState(db *storage.DB, jobID string, lastChunk uint64, slotCount int) error {
	if jobID == "" || lastChunk == 0 {
		return nil
	}
	if slotCount <= 0 {
		return errors.New("cluster import cleanup slot count is invalid")
	}
	for {
		removed := 0
		err := db.Update(func(tx *storage.Tx) error {
			bucket, err := tx.GetBucket([]byte(clusterImportKeyStateBucketNameConst))
			if errors.Is(err, storage.ErrBucketNotFound) {
				return nil
			}
			if err != nil {
				return err
			}
			states := make([]clusterImportKeyState, 0, 128)
			cursor := bucket.Cursor()
			for key, raw := cursor.First(); key != nil && len(states) < cap(states); key, raw = cursor.Next() {
				var state clusterImportKeyState
				if decodeErr := gob.NewDecoder(bytes.NewReader(raw)).Decode(&state); decodeErr != nil {
					return decodeErr
				}
				if state.SchemaVersion != clusterImportKeyStateSchemaVersion {
					return errors.New("cluster import key state has an unsupported schema")
				}
				if state.ReceiptContext.JobID == jobID {
					states = append(states, state)
				}
			}
			for _, state := range states {
				if err = cleanupClusterImportKeyStateTx(tx, state, slotCount); err != nil {
					return err
				}
				removed++
			}
			return nil
		})
		if err != nil {
			return err
		}
		if removed == 0 {
			break
		}
	}
	const receiptBucketsPerTransaction = 256
	for first := uint64(1); first <= lastChunk; first += receiptBucketsPerTransaction {
		last := min(lastChunk, first+receiptBucketsPerTransaction-1)
		if err := db.Update(func(tx *storage.Tx) error {
			for chunk := first; chunk <= last; chunk++ {
				err := tx.DeleteBucket(clusterImportReceiptBucketName(clusterImportReceiptContext{JobID: jobID, ChunkIndex: chunk}))
				if err != nil && !errors.Is(err, storage.ErrBucketNotFound) {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

func countClusterImportStagingKeys(db *storage.DB) (uint64, error) {
	if db == nil {
		return 0, nil
	}
	var count uint64
	err := db.View(func(tx *storage.Tx) error {
		bucket, err := tx.GetBucket([]byte(clusterImportKeyStateBucketNameConst))
		if errors.Is(err, storage.ErrBucketNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		count = bucket.ItemCount()
		return nil
	})
	return count, err
}

func clusterImportStageBucketName(receiptContext clusterImportReceiptContext, record clusterTransferRecord) []byte {
	digest := sha256.New()
	_, _ = fmt.Fprintf(digest, "%s:%d:%d:%s:%s:", receiptContext.JobID, receiptContext.ChunkIndex, record.DBIndex, record.BucketKind, record.RedisType)
	_, _ = digest.Write(record.Key)
	_, _ = digest.Write([]byte{0})
	_, _ = digest.Write(record.EntryKey)
	return []byte(fmt.Sprintf("%s%x", clusterImportStageBucketPrefix, digest.Sum(nil)[:16]))
}

func encodeClusterImportStageMeta(meta clusterImportStageMeta) []byte {
	raw := make([]byte, 16)
	binary.BigEndian.PutUint64(raw[0:8], meta.ValueLen)
	binary.BigEndian.PutUint64(raw[8:16], meta.NextOffset)
	return raw
}

func decodeClusterImportStageMeta(raw []byte) (clusterImportStageMeta, error) {
	if len(raw) != 16 {
		return clusterImportStageMeta{}, errors.New("cluster transfer stage metadata is corrupted")
	}
	return clusterImportStageMeta{ValueLen: binary.BigEndian.Uint64(raw[0:8]), NextOffset: binary.BigEndian.Uint64(raw[8:16])}, nil
}

type clusterImportStageReader struct {
	cursor  *storage.Cursor
	current []byte
	offset  int
	started bool
	read    uint64
}

func newClusterImportStageReader(bucket *storage.Bucket) *clusterImportStageReader {
	return &clusterImportStageReader{cursor: bucket.Cursor()}
}

func (r *clusterImportStageReader) Read(p []byte) (int, error) {
	for len(r.current) == r.offset {
		var key, value []byte
		if !r.started {
			key, value = r.cursor.Seek([]byte{1})
			r.started = true
		} else {
			key, value = r.cursor.Next()
		}
		if key == nil || len(key) == 0 || key[0] != 1 {
			return 0, io.EOF
		}
		r.current = value
		r.offset = 0
	}
	n := copy(p, r.current[r.offset:])
	r.offset += n
	r.read += uint64(n)
	return n, nil
}
