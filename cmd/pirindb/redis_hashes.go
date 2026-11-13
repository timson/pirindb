package main

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/timson/pirindb/storage"
)

var (
	redisHashNextIDKey = []byte("next_id")
)

const redisHashMetaSize = 16

const (
	redisKeyTypeNone   = ""
	redisKeyTypeString = "string"
	redisKeyTypeList   = "list"
	redisKeyTypeHash   = "hash"
	redisKeyTypeZSet   = "zset"
	redisKeyTypeBloom  = "bf"
	redisKeyTypeTopK   = "topk"
)

type redisHashMeta struct {
	ID            uint64
	FieldCount    uint64
	StorageFormat byte
}

type hashFieldValuePair struct {
	field []byte
	value []byte
}

func (meta *redisHashMeta) serialize() []byte {
	buf := make([]byte, redisHashMetaSize)
	binary.BigEndian.PutUint64(buf[0:8], meta.ID)
	binary.BigEndian.PutUint64(buf[8:16], meta.FieldCount)
	return buf
}

func deserializeRedisHashMeta(buf []byte) (*redisHashMeta, error) {
	if len(buf) != redisHashMetaSize {
		return nil, errors.New("corrupted redis hash metadata")
	}
	return &redisHashMeta{
		ID:            binary.BigEndian.Uint64(buf[0:8]),
		FieldCount:    binary.BigEndian.Uint64(buf[8:16]),
		StorageFormat: redisKeyStorageLegacy,
	}, nil
}

func redisHashBucketName(ns redisNamespace, id uint64) []byte {
	name := make([]byte, len(ns.hashDataPrefix)+8)
	copy(name, ns.hashDataPrefix)
	binary.BigEndian.PutUint64(name[len(ns.hashDataPrefix):], id)
	return name
}

func loadRedisHashMetaTx(tx *storage.Tx, ns redisNamespace, key []byte) (*redisHashMeta, bool, error) {
	bucket, err := tx.GetBucket(ns.hashMetaBucket)
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

	meta, err := deserializeRedisHashMeta(raw)
	if err != nil {
		return nil, false, err
	}
	meta.StorageFormat, err = redisObjectStorageFormatTx(tx, ns, key, redisKeyTypeHash, meta.ID)
	if err != nil {
		return nil, false, err
	}
	return meta, true, nil
}

func saveRedisHashMetaTx(tx *storage.Tx, ns redisNamespace, key []byte, meta *redisHashMeta) error {
	bucket, err := tx.CreateBucketIfNotExists(ns.hashMetaBucket)
	if err != nil {
		return err
	}
	if err = bucket.Put(key, meta.serialize()); err != nil {
		return err
	}
	if err = saveRedisKeyMetaForTypeWithFormatTx(tx, ns, key, redisKeyTypeHash, meta.ID, meta.StorageFormat); err != nil {
		return err
	}
	return ensureRedisSlotIndexEntryTx(tx, ns, key)
}

func deleteRedisHashMetaTx(tx *storage.Tx, ns redisNamespace, key []byte) error {
	bucket, err := tx.GetBucket(ns.hashMetaBucket)
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
	if err = deleteRedisKeyMetaTx(tx, ns, key); err != nil {
		return err
	}
	return deleteRedisSlotIndexEntryTx(tx, ns, key)
}

func nextRedisHashIDTx(tx *storage.Tx, ns redisNamespace) (uint64, error) {
	bucket, err := tx.CreateBucketIfNotExists(ns.hashSysBucket)
	if err != nil {
		return 0, err
	}

	raw, found := bucket.Get(redisHashNextIDKey)
	if !found {
		initial := make([]byte, 8)
		binary.BigEndian.PutUint64(initial, 2)
		if err = bucket.Put(redisHashNextIDKey, initial); err != nil {
			return 0, err
		}
		return 1, nil
	}

	if len(raw) != 8 {
		return 0, errors.New("corrupted redis hash id counter")
	}

	id := binary.BigEndian.Uint64(raw)
	next := make([]byte, 8)
	binary.BigEndian.PutUint64(next, id+1)
	if err = bucket.Put(redisHashNextIDKey, next); err != nil {
		return 0, err
	}
	return id, nil
}

func ensureRedisHashDataBucketTx(tx *storage.Tx, ns redisNamespace, meta *redisHashMeta) (*redisObjectBucket, error) {
	return openRedisObjectBucketTx(tx, ns.hashDataBucket, redisHashBucketName(ns, meta.ID), meta.ID, meta.StorageFormat, true)
}

func deleteRedisHashTx(tx *storage.Tx, ns redisNamespace, key []byte, meta *redisHashMeta) error {
	store, err := openRedisObjectBucketTx(tx, ns.hashDataBucket, redisHashBucketName(ns, meta.ID), meta.ID, meta.StorageFormat, false)
	if err != nil && !errors.Is(err, storage.ErrBucketNotFound) {
		return err
	}
	if err == nil {
		if err = deleteRedisObjectBucketTx(tx, store, redisHashBucketName(ns, meta.ID)); err != nil {
			return err
		}
	}
	if err = deleteRedisHashMetaTx(tx, ns, key); err != nil {
		return err
	}
	return deleteRedisExpireAtMsTx(tx, ns, key)
}

func deleteRedisHashIfExistsTx(tx *storage.Tx, ns redisNamespace, key []byte) (bool, error) {
	meta, found, err := loadRedisHashMetaTx(tx, ns, key)
	if err != nil || !found {
		return false, err
	}
	return true, deleteRedisHashTx(tx, ns, key, meta)
}

func loadRedisHashMetaForReadTx(tx *storage.Tx, ns redisNamespace, key []byte, nowMs int64) (*redisHashMeta, bool, error) {
	meta, found, err := loadRedisHashMetaTx(tx, ns, key)
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
	if keyType == redisKeyTypeString || keyType == redisKeyTypeList || keyType == redisKeyTypeZSet || keyType == redisKeyTypeBloom || keyType == redisKeyTypeTopK {
		return nil, false, errRedisWrongType
	}
	return nil, false, nil
}

func initRedisHashMetaTx(tx *storage.Tx, ns redisNamespace, key []byte, nowMs int64) (*redisHashMeta, error) {
	keyType, err := redisKeyTypeTx(tx, ns, key, nowMs)
	if err != nil {
		return nil, err
	}
	if keyType == redisKeyTypeString || keyType == redisKeyTypeList || keyType == redisKeyTypeZSet || keyType == redisKeyTypeBloom || keyType == redisKeyTypeTopK {
		return nil, errRedisWrongType
	}

	id, err := nextRedisHashIDTx(tx, ns)
	if err != nil {
		return nil, err
	}

	meta := &redisHashMeta{ID: id, StorageFormat: redisKeyStorageShared}
	if _, err = ensureRedisHashDataBucketTx(tx, ns, meta); err != nil {
		return nil, err
	}
	return meta, nil
}

func getRedisHashBucketTx(tx *storage.Tx, ns redisNamespace, key []byte, nowMs int64) (*redisObjectBucket, *redisHashMeta, bool, error) {
	meta, found, err := loadRedisHashMetaForReadTx(tx, ns, key, nowMs)
	if err != nil || !found {
		return nil, meta, found, err
	}
	bucket, err := openRedisObjectBucketTx(tx, ns.hashDataBucket, redisHashBucketName(ns, meta.ID), meta.ID, meta.StorageFormat, false)
	if err != nil {
		return nil, nil, false, err
	}
	return bucket, meta, true, nil
}

func redisHashSetTx(tx *storage.Tx, ns redisNamespace, key []byte, pairs []hashFieldValuePair, nowMs int64) (int64, error) {
	if _, err := purgeExpiredRedisKeyTx(tx, ns, key, nowMs); err != nil {
		return 0, err
	}

	meta, found, err := loadRedisHashMetaTx(tx, ns, key)
	if err != nil {
		return 0, err
	}
	if !found {
		meta, err = initRedisHashMetaTx(tx, ns, key, nowMs)
		if err != nil {
			return 0, err
		}
	}

	bucket, err := ensureRedisHashDataBucketTx(tx, ns, meta)
	if err != nil {
		return 0, err
	}

	var added int64
	changed := false
	for _, pair := range pairs {
		currentValue, exists := bucket.Get(pair.field)
		if !exists {
			added++
			meta.FieldCount++
		} else if bytes.Equal(currentValue, pair.value) {
			continue
		}
		if err = bucket.Put(pair.field, pair.value); err != nil {
			return 0, err
		}
		changed = true
	}

	if !changed {
		return added, nil
	}
	if err = saveRedisHashMetaTx(tx, ns, key, meta); err != nil {
		return 0, err
	}
	return added, nil
}

func redisHashGetTx(tx *storage.Tx, ns redisNamespace, key []byte, field []byte, nowMs int64) ([]byte, bool, error) {
	bucket, _, found, err := getRedisHashBucketTx(tx, ns, key, nowMs)
	if err != nil || !found {
		return nil, false, err
	}
	value, exists := bucket.Get(field)
	if !exists {
		return nil, false, nil
	}
	return cloneBytes(value), true, nil
}

func redisHashDelTx(tx *storage.Tx, ns redisNamespace, key []byte, fields [][]byte, nowMs int64) (int64, error) {
	if _, err := purgeExpiredRedisKeyTx(tx, ns, key, nowMs); err != nil {
		return 0, err
	}

	bucket, meta, found, err := getRedisHashBucketTx(tx, ns, key, nowMs)
	if err != nil || !found {
		return 0, err
	}

	var deleted int64
	for _, field := range fields {
		err = bucket.Remove(field)
		if errors.Is(err, storage.ErrNodeNotFound) {
			continue
		}
		if err != nil {
			return 0, err
		}
		deleted++
		if meta.FieldCount > 0 {
			meta.FieldCount--
		}
	}

	if deleted == 0 {
		return 0, nil
	}
	if meta.FieldCount == 0 {
		return deleted, deleteRedisHashTx(tx, ns, key, meta)
	}
	if err = saveRedisHashMetaTx(tx, ns, key, meta); err != nil {
		return 0, err
	}
	return deleted, nil
}

func redisHashExistsTx(tx *storage.Tx, ns redisNamespace, key []byte, field []byte, nowMs int64) (bool, error) {
	bucket, _, found, err := getRedisHashBucketTx(tx, ns, key, nowMs)
	if err != nil || !found {
		return false, err
	}
	_, exists := bucket.Get(field)
	return exists, nil
}

func redisHashLenTx(tx *storage.Tx, ns redisNamespace, key []byte, nowMs int64) (int64, error) {
	meta, found, err := loadRedisHashMetaForReadTx(tx, ns, key, nowMs)
	if err != nil || !found {
		return 0, err
	}
	return int64(meta.FieldCount), nil
}

func redisHashKeysTx(tx *storage.Tx, ns redisNamespace, key []byte, nowMs int64) ([][]byte, error) {
	bucket, _, found, err := getRedisHashBucketTx(tx, ns, key, nowMs)
	if err != nil || !found {
		return nil, err
	}

	keys := make([][]byte, 0)
	cursor := bucket.Cursor()
	for field, _ := cursor.First(); field != nil; field, _ = cursor.Next() {
		keys = append(keys, cloneBytes(field))
	}
	return keys, nil
}

func redisHashValuesTx(tx *storage.Tx, ns redisNamespace, key []byte, nowMs int64) ([][]byte, error) {
	bucket, _, found, err := getRedisHashBucketTx(tx, ns, key, nowMs)
	if err != nil || !found {
		return nil, err
	}

	values := make([][]byte, 0)
	cursor := bucket.Cursor()
	for _, value := cursor.First(); value != nil; _, value = cursor.Next() {
		values = append(values, cloneBytes(value))
	}
	return values, nil
}

func redisHashGetAllTx(tx *storage.Tx, ns redisNamespace, key []byte, nowMs int64) ([][]byte, error) {
	bucket, _, found, err := getRedisHashBucketTx(tx, ns, key, nowMs)
	if err != nil || !found {
		return nil, err
	}

	values := make([][]byte, 0)
	cursor := bucket.Cursor()
	for field, value := cursor.First(); field != nil; field, value = cursor.Next() {
		values = append(values, cloneBytes(field), cloneBytes(value))
	}
	return values, nil
}
