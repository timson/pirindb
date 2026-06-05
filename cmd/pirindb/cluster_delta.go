package main

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"

	"github.com/timson/pirindb/storage"
)

const (
	clusterDeltaMetaBucketNameConst = "__pirin_cluster_delta_meta__"
	clusterDeltaBatchEntryLimit     = 128
)

type clusterDeltaMutation struct {
	DBIndex    int
	Key        []byte
	Deleted    bool
	Type       string
	ExpireAtMs int64

	StringValue []byte
	ListValues  [][]byte
	HashPairs   []clusterSnapshotHashPair
	ZSetPairs   []clusterSnapshotZSetPair
	Bloom       *clusterSnapshotBloom
	TopK        *clusterSnapshotTopK
}

type clusterDeltaBatch struct {
	MoveID    string
	AfterSeq  uint64
	LastSeq   uint64
	Mutations []clusterDeltaMutation
}

func clusterDeltaMetaBucketName() []byte {
	return []byte(clusterDeltaMetaBucketNameConst)
}

func clusterDeltaLogBucketName(moveID string) []byte {
	return []byte("__pirin_cluster_delta_log__:" + moveID)
}

func encodeClusterDeltaSeq(seq uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, seq)
	return buf
}

func decodeClusterDeltaSeq(buf []byte) (uint64, error) {
	if len(buf) != 8 {
		return 0, errors.New("corrupted cluster delta sequence key")
	}
	return binary.BigEndian.Uint64(buf), nil
}

func encodeClusterDeltaMutation(mutation clusterDeltaMutation) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(mutation); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeClusterDeltaMutation(raw []byte) (clusterDeltaMutation, error) {
	var mutation clusterDeltaMutation
	err := gob.NewDecoder(bytes.NewReader(raw)).Decode(&mutation)
	return mutation, err
}

func encodeClusterDeltaBatch(batch clusterDeltaBatch) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(batch); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeClusterDeltaBatch(raw []byte) (clusterDeltaBatch, error) {
	var batch clusterDeltaBatch
	err := gob.NewDecoder(bytes.NewReader(raw)).Decode(&batch)
	return batch, err
}

func nextClusterDeltaSeqTx(tx *storage.Tx, moveID string) (uint64, error) {
	metaBucket, err := tx.CreateBucketIfNotExists(clusterDeltaMetaBucketName())
	if err != nil {
		return 0, err
	}
	raw, found := metaBucket.Get([]byte(moveID))
	nextSeq := uint64(1)
	if found {
		nextSeq, err = decodeClusterDeltaSeq(raw)
		if err != nil {
			return 0, err
		}
	}
	if err = metaBucket.Put([]byte(moveID), encodeClusterDeltaSeq(nextSeq+1)); err != nil {
		return 0, err
	}
	return nextSeq, nil
}

func appendClusterDeltaMutationsTx(tx *storage.Tx, moveID string, mutations []clusterDeltaMutation) error {
	if len(mutations) == 0 {
		return nil
	}
	logBucket, err := tx.CreateBucketIfNotExists(clusterDeltaLogBucketName(moveID))
	if err != nil {
		return err
	}
	for _, mutation := range mutations {
		seq, err := nextClusterDeltaSeqTx(tx, moveID)
		if err != nil {
			return err
		}
		encoded, err := encodeClusterDeltaMutation(mutation)
		if err != nil {
			return err
		}
		if err = logBucket.Put(encodeClusterDeltaSeq(seq), encoded); err != nil {
			return err
		}
	}
	return nil
}

func buildClusterDeltaMutationTx(tx *storage.Tx, ns redisNamespace, dbIndex int, key []byte, nowMs int64) (clusterDeltaMutation, error) {
	keyType, err := redisKeyTypeTx(tx, ns, key, nowMs)
	if err != nil {
		return clusterDeltaMutation{}, err
	}
	mutation := clusterDeltaMutation{
		DBIndex:    dbIndex,
		Key:        cloneBytes(key),
		Deleted:    keyType == redisKeyTypeNone,
		Type:       keyType,
		ExpireAtMs: -1,
	}
	if keyType == redisKeyTypeNone {
		return mutation, nil
	}
	expireAtMs, foundTTL, err := loadRedisExpireAtMsTx(tx, ns, key)
	if err != nil {
		return clusterDeltaMutation{}, err
	}
	if foundTTL {
		mutation.ExpireAtMs = expireAtMs
	}
	switch keyType {
	case redisKeyTypeString:
		bucket, err := tx.GetBucket(ns.stringBucket)
		if err != nil {
			return clusterDeltaMutation{}, err
		}
		value, found := bucket.Get(key)
		if !found {
			mutation.Deleted = true
			mutation.Type = redisKeyTypeNone
			return mutation, nil
		}
		mutation.StringValue = cloneBytes(value)
	case redisKeyTypeList:
		meta, found, err := loadRedisListMetaForReadTx(tx, ns, key, nowMs)
		if err != nil {
			return clusterDeltaMutation{}, err
		}
		if !found {
			mutation.Deleted = true
			mutation.Type = redisKeyTypeNone
			return mutation, nil
		}
		values, _, err := readRedisListValuesTx(tx, ns, meta)
		if err != nil {
			return clusterDeltaMutation{}, err
		}
		mutation.ListValues = make([][]byte, 0, len(values))
		for _, value := range values {
			mutation.ListValues = append(mutation.ListValues, cloneBytes(value))
		}
	case redisKeyTypeHash:
		bucket, _, found, err := getRedisHashBucketTx(tx, ns, key, nowMs)
		if err != nil {
			return clusterDeltaMutation{}, err
		}
		if !found {
			mutation.Deleted = true
			mutation.Type = redisKeyTypeNone
			return mutation, nil
		}
		pairs := make([]clusterSnapshotHashPair, 0)
		if err = bucket.ForEach(func(field, value []byte) error {
			pairs = append(pairs, clusterSnapshotHashPair{
				Field: cloneBytes(field),
				Value: cloneBytes(value),
			})
			return nil
		}); err != nil {
			return clusterDeltaMutation{}, err
		}
		mutation.HashPairs = pairs
	case redisKeyTypeZSet:
		memberBucket, _, _, found, err := getRedisZSetBucketsTx(tx, ns, key, nowMs)
		if err != nil {
			return clusterDeltaMutation{}, err
		}
		if !found {
			mutation.Deleted = true
			mutation.Type = redisKeyTypeNone
			return mutation, nil
		}
		pairs := make([]clusterSnapshotZSetPair, 0)
		if err = memberBucket.ForEach(func(member, rawScore []byte) error {
			score, decodeErr := decodeRedisZSetSortableScore(rawScore)
			if decodeErr != nil {
				return decodeErr
			}
			pairs = append(pairs, clusterSnapshotZSetPair{
				Score:  score,
				Member: cloneBytes(member),
			})
			return nil
		}); err != nil {
			return clusterDeltaMutation{}, err
		}
		mutation.ZSetPairs = pairs
	case redisKeyTypeBloom:
		meta, found, err := loadRedisBloomMetaForReadTx(tx, ns, key, nowMs)
		if err != nil {
			return clusterDeltaMutation{}, err
		}
		if !found {
			mutation.Deleted = true
			mutation.Type = redisKeyTypeNone
			return mutation, nil
		}
		dataBucket, err := getRedisBloomDataBucketTx(tx, ns, meta)
		if err != nil {
			return clusterDeltaMutation{}, err
		}
		dataEntries, err := dumpBucketEntries(dataBucket)
		if err != nil {
			return clusterDeltaMutation{}, err
		}
		mutation.Bloom = &clusterSnapshotBloom{
			InitialCapacity: meta.InitialCapacity,
			ErrorRateBits:   meta.ErrorRateBits,
			Expansion:       meta.Expansion,
			Flags:           meta.Flags,
			SubFilterCount:  meta.SubFilterCount,
			Data:            dataEntries,
		}
	case redisKeyTypeTopK:
		meta, found, err := loadRedisTopKMetaForReadTx(tx, ns, key, nowMs)
		if err != nil {
			return clusterDeltaMutation{}, err
		}
		if !found {
			mutation.Deleted = true
			mutation.Type = redisKeyTypeNone
			return mutation, nil
		}
		dataBucket, err := getRedisTopKDataBucketTx(tx, ns, meta)
		if err != nil {
			return clusterDeltaMutation{}, err
		}
		dataEntries, err := dumpBucketEntries(dataBucket)
		if err != nil {
			return clusterDeltaMutation{}, err
		}
		mutation.TopK = &clusterSnapshotTopK{
			K:         meta.K,
			Width:     meta.Width,
			Depth:     meta.Depth,
			HeapSize:  meta.HeapSize,
			DecayBits: meta.DecayBits,
			Data:      dataEntries,
		}
	default:
		return clusterDeltaMutation{}, errors.New("unsupported redis type in cluster delta capture")
	}
	return mutation, nil
}

func captureClusterDeltaMutationsForCommandTx(tx *storage.Tx, selectedDB int, args [][]byte, move *clusterPendingMove, slotCount int, nowMs int64) error {
	if tx == nil || move == nil || move.ID == "" {
		return nil
	}
	keys, err := redisCommandKeys(args)
	if err != nil {
		return err
	}
	if len(keys) == 0 {
		return nil
	}
	ns := redisNamespaceForDB(selectedDB, slotCount)
	mutations := make([]clusterDeltaMutation, 0, len(keys))
	seen := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		if ClusterKeySlot(key, slotCount) < move.StartSlot || ClusterKeySlot(key, slotCount) > move.EndSlot {
			continue
		}
		keyStr := string(key)
		if _, exists := seen[keyStr]; exists {
			continue
		}
		seen[keyStr] = struct{}{}
		mutation, err := buildClusterDeltaMutationTx(tx, ns, selectedDB, key, nowMs)
		if err != nil {
			return err
		}
		mutations = append(mutations, mutation)
	}
	return appendClusterDeltaMutationsTx(tx, move.ID, mutations)
}

func loadClusterDeltaBatch(db *storage.DB, moveID string, afterSeq uint64, limit int) (clusterDeltaBatch, error) {
	batch := clusterDeltaBatch{
		MoveID:   moveID,
		AfterSeq: afterSeq,
	}
	err := db.View(func(tx *storage.Tx) error {
		bucket, err := tx.GetBucket(clusterDeltaLogBucketName(moveID))
		if errors.Is(err, storage.ErrBucketNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		cursor := bucket.Cursor()
		startKey := encodeClusterDeltaSeq(afterSeq + 1)
		for key, raw := cursor.Seek(startKey); key != nil; key, raw = cursor.Next() {
			seq, err := decodeClusterDeltaSeq(key)
			if err != nil {
				return err
			}
			mutation, err := decodeClusterDeltaMutation(raw)
			if err != nil {
				return err
			}
			batch.Mutations = append(batch.Mutations, mutation)
			batch.LastSeq = seq
			if limit > 0 && len(batch.Mutations) >= limit {
				break
			}
		}
		return nil
	})
	return batch, err
}

func applyClusterDeltaMutationTx(tx *storage.Tx, mutation clusterDeltaMutation, slotCount int, nowMs int64) error {
	ns := redisNamespaceForDB(mutation.DBIndex, slotCount)
	rawType, err := redisRawKeyTypeTx(tx, ns, mutation.Key)
	if err != nil {
		return err
	}
	if _, err = deleteRedisKeyByRawTypeTx(tx, ns, mutation.Key, rawType); err != nil {
		return err
	}
	if err = deleteRedisExpireAtMsTx(tx, ns, mutation.Key); err != nil {
		return err
	}
	if mutation.Deleted || mutation.Type == redisKeyTypeNone {
		return nil
	}
	switch mutation.Type {
	case redisKeyTypeString:
		if err = putRedisBytesTx(tx, ns, mutation.Key, mutation.StringValue, nowMs); err != nil {
			return err
		}
	case redisKeyTypeList:
		if _, err = redisPushTx(tx, ns, mutation.Key, mutation.ListValues, false, nowMs); err != nil {
			return err
		}
	case redisKeyTypeHash:
		pairs := make([]hashFieldValuePair, 0, len(mutation.HashPairs))
		for _, pair := range mutation.HashPairs {
			pairs = append(pairs, hashFieldValuePair{
				field: cloneBytes(pair.Field),
				value: cloneBytes(pair.Value),
			})
		}
		if _, err = redisHashSetTx(tx, ns, mutation.Key, pairs, nowMs); err != nil {
			return err
		}
	case redisKeyTypeZSet:
		pairs := make([]redisZSetScoreMemberPair, 0, len(mutation.ZSetPairs))
		for _, pair := range mutation.ZSetPairs {
			pairs = append(pairs, redisZSetScoreMemberPair{
				score:  pair.Score,
				member: cloneBytes(pair.Member),
			})
		}
		if _, err = redisZSetAddTx(tx, ns, mutation.Key, pairs, nowMs); err != nil {
			return err
		}
	case redisKeyTypeBloom:
		if mutation.Bloom == nil {
			return errors.New("corrupted bloom delta mutation")
		}
		id, err := nextRedisBloomIDTx(tx, ns)
		if err != nil {
			return err
		}
		meta := &redisBloomMeta{
			ID:              id,
			InitialCapacity: mutation.Bloom.InitialCapacity,
			ErrorRateBits:   mutation.Bloom.ErrorRateBits,
			Expansion:       mutation.Bloom.Expansion,
			Flags:           mutation.Bloom.Flags,
			SubFilterCount:  mutation.Bloom.SubFilterCount,
		}
		bucket, err := ensureRedisBloomDataBucketTx(tx, ns, meta)
		if err != nil {
			return err
		}
		for _, item := range mutation.Bloom.Data {
			if err = bucket.Put(item.Key, item.Value); err != nil {
				return err
			}
		}
		if err = saveRedisBloomMetaTx(tx, ns, mutation.Key, meta); err != nil {
			return err
		}
	case redisKeyTypeTopK:
		if mutation.TopK == nil {
			return errors.New("corrupted topk delta mutation")
		}
		id, err := nextRedisTopKIDTx(tx, ns)
		if err != nil {
			return err
		}
		meta := &redisTopKMeta{
			ID:        id,
			K:         mutation.TopK.K,
			Width:     mutation.TopK.Width,
			Depth:     mutation.TopK.Depth,
			HeapSize:  mutation.TopK.HeapSize,
			DecayBits: mutation.TopK.DecayBits,
		}
		bucket, err := ensureRedisTopKDataBucketTx(tx, ns, meta)
		if err != nil {
			return err
		}
		for _, item := range mutation.TopK.Data {
			if err = bucket.Put(item.Key, item.Value); err != nil {
				return err
			}
		}
		if err = saveRedisTopKMetaTx(tx, ns, mutation.Key, meta); err != nil {
			return err
		}
	default:
		return errors.New("unsupported redis type in cluster delta apply")
	}
	if mutation.ExpireAtMs >= 0 {
		_, err = redisExpireKeyAtTx(tx, ns, mutation.Key, mutation.ExpireAtMs, nowMs)
		return err
	}
	return nil
}

func applyClusterDeltaBatch(db *storage.DB, slotCount int, batch clusterDeltaBatch, nowMs int64) error {
	return db.Update(func(tx *storage.Tx) error {
		for _, mutation := range batch.Mutations {
			if err := applyClusterDeltaMutationTx(tx, mutation, slotCount, nowMs); err != nil {
				return err
			}
		}
		return nil
	})
}

func deleteClusterDeltaMoveData(db *storage.DB, moveID string) error {
	if moveID == "" {
		return nil
	}
	return db.Update(func(tx *storage.Tx) error {
		if err := tx.DeleteBucket(clusterDeltaLogBucketName(moveID)); err != nil && !errors.Is(err, storage.ErrBucketNotFound) {
			return err
		}
		metaBucket, err := tx.GetBucket(clusterDeltaMetaBucketName())
		if errors.Is(err, storage.ErrBucketNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		err = metaBucket.Remove([]byte(moveID))
		if errors.Is(err, storage.ErrNodeNotFound) {
			return nil
		}
		return err
	})
}
