package main

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"

	"github.com/timson/pirindb/storage"
)

const (
	clusterDeltaMetaBucketNameConst = "__pirin_cluster_delta_meta__"
	clusterDeltaBatchEntryLimit     = 128
	clusterDeltaSchemaVersion       = 1
)

// clusterDeltaMutation is intentionally a dirty-key marker, not a post-image.
// Keeping Deleted in the persisted shape lets older logs decode, but catch-up
// always reads the final key state after writes for the slot range are fenced.
type clusterDeltaMutation struct {
	SchemaVersion uint8
	DBIndex       int
	Key           []byte
	Deleted       bool
}

type clusterDeltaBatch struct {
	MoveID      string
	AfterSeq    uint64
	LastSeq     uint64
	RecordCount int
	Mutations   []clusterDeltaMutation
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
	if err := gob.NewDecoder(bytes.NewReader(raw)).Decode(&mutation); err != nil {
		return clusterDeltaMutation{}, err
	}
	if mutation.SchemaVersion == 0 {
		// Version zero is the original post-image format. Gob ignores its removed
		// value fields, so it migrates safely to a compact dirty-key marker.
		mutation.SchemaVersion = clusterDeltaSchemaVersion
	}
	if mutation.SchemaVersion != clusterDeltaSchemaVersion {
		return clusterDeltaMutation{}, fmt.Errorf("unsupported cluster dirty-key schema version %d", mutation.SchemaVersion)
	}
	if mutation.DBIndex < redisDatabaseMin || mutation.DBIndex > redisDatabaseMax || len(mutation.Key) == 0 || len(mutation.Key) >= storage.MaxKeySize {
		return clusterDeltaMutation{}, errors.New("cluster dirty-key marker is invalid")
	}
	mutation.Key = cloneBytes(mutation.Key)
	return mutation, nil
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
		mutation.SchemaVersion = clusterDeltaSchemaVersion
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

func captureClusterDeltaMutationsForCommandTx(tx *storage.Tx, selectedDB int, args [][]byte, move *clusterPendingMove, slotCount int, nowMs int64) error {
	_ = nowMs
	if tx == nil || move == nil || move.ID == "" {
		return nil
	}
	keys, err := redisCommandKeys(args)
	if err != nil {
		return err
	}
	mutations := make([]clusterDeltaMutation, 0, len(keys))
	seen := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		slot := ClusterKeySlot(key, slotCount)
		if slot < move.StartSlot || slot > move.EndSlot {
			continue
		}
		keyID := string(key)
		if _, exists := seen[keyID]; exists {
			continue
		}
		seen[keyID] = struct{}{}
		mutations = append(mutations, clusterDeltaMutation{
			SchemaVersion: clusterDeltaSchemaVersion,
			DBIndex:       selectedDB,
			Key:           cloneBytes(key),
		})
	}
	return appendClusterDeltaMutationsTx(tx, move.ID, mutations)
}

// loadClusterDeltaBatch advances through a bounded number of log records and
// coalesces repeated writes to the same key. The source is write-fenced during
// catch-up, so each returned marker can be resolved to one final state.
func loadClusterDeltaBatch(db *storage.DB, moveID string, afterSeq uint64, limit int) (clusterDeltaBatch, error) {
	batch := clusterDeltaBatch{MoveID: moveID, AfterSeq: afterSeq}
	if limit <= 0 {
		limit = clusterDeltaBatchEntryLimit
	}
	maxRecords := limit * 8
	err := db.View(func(tx *storage.Tx) error {
		bucket, err := tx.GetBucket(clusterDeltaLogBucketName(moveID))
		if errors.Is(err, storage.ErrBucketNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		seen := make(map[string]int, limit)
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
			identity := fmt.Sprintf("%d\x00%s", mutation.DBIndex, mutation.Key)
			if index, exists := seen[identity]; exists {
				batch.Mutations[index] = mutation
			} else {
				seen[identity] = len(batch.Mutations)
				batch.Mutations = append(batch.Mutations, mutation)
			}
			batch.LastSeq = seq
			batch.RecordCount++
			if len(batch.Mutations) >= limit || batch.RecordCount >= maxRecords {
				break
			}
		}
		return nil
	})
	return batch, err
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

func clusterDeltaLastSequence(db *storage.DB, moveID string) (uint64, error) {
	var last uint64
	err := db.View(func(tx *storage.Tx) error {
		bucket, err := tx.GetBucket(clusterDeltaMetaBucketName())
		if errors.Is(err, storage.ErrBucketNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		raw, found := bucket.Get([]byte(moveID))
		if !found {
			return nil
		}
		next, err := decodeClusterDeltaSeq(raw)
		if err != nil {
			return err
		}
		if next > 0 {
			last = next - 1
		}
		return nil
	})
	return last, err
}
