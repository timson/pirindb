package main

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/timson/pirindb/storage"
)

const (
	redisKeyMetaSchemaVersion = 1
	redisKeyMetaSize          = 24

	redisKeyStorageLegacy = 1

	redisKeyDirectoryPhaseBuilding = 1
	redisKeyDirectoryPhaseActive   = 2
)

const (
	redisKeyMetaTypeString byte = iota + 1
	redisKeyMetaTypeList
	redisKeyMetaTypeHash
	redisKeyMetaTypeZSet
	redisKeyMetaTypeBloom
	redisKeyMetaTypeTopK
)

var redisKeyDirectoryStateKey = []byte("state")

type redisKeyMeta struct {
	Type          byte
	StorageFormat byte
	Flags         byte
	ObjectID      uint64
	ExpireAtMs    int64
	Generation    uint32
}

type redisKeyDirectoryState struct {
	Phase       byte
	BucketIndex byte
	Cursor      []byte
}

func redisKeyMetaTypeCode(keyType string) (byte, error) {
	switch keyType {
	case redisKeyTypeString:
		return redisKeyMetaTypeString, nil
	case redisKeyTypeList:
		return redisKeyMetaTypeList, nil
	case redisKeyTypeHash:
		return redisKeyMetaTypeHash, nil
	case redisKeyTypeZSet:
		return redisKeyMetaTypeZSet, nil
	case redisKeyTypeBloom:
		return redisKeyMetaTypeBloom, nil
	case redisKeyTypeTopK:
		return redisKeyMetaTypeTopK, nil
	default:
		return 0, fmt.Errorf("unsupported redis key type %q", keyType)
	}
}

func redisKeyTypeFromMetaCode(code byte) (string, error) {
	switch code {
	case redisKeyMetaTypeString:
		return redisKeyTypeString, nil
	case redisKeyMetaTypeList:
		return redisKeyTypeList, nil
	case redisKeyMetaTypeHash:
		return redisKeyTypeHash, nil
	case redisKeyMetaTypeZSet:
		return redisKeyTypeZSet, nil
	case redisKeyMetaTypeBloom:
		return redisKeyTypeBloom, nil
	case redisKeyMetaTypeTopK:
		return redisKeyTypeTopK, nil
	default:
		return redisKeyTypeNone, errors.New("corrupted redis key metadata type")
	}
}

func (meta redisKeyMeta) serialize() []byte {
	buf := make([]byte, redisKeyMetaSize)
	buf[0] = redisKeyMetaSchemaVersion
	buf[1] = meta.Type
	buf[2] = meta.StorageFormat
	buf[3] = meta.Flags
	binary.BigEndian.PutUint64(buf[4:12], meta.ObjectID)
	binary.BigEndian.PutUint64(buf[12:20], uint64(meta.ExpireAtMs))
	binary.BigEndian.PutUint32(buf[20:24], meta.Generation)
	return buf
}

func deserializeRedisKeyMeta(buf []byte) (redisKeyMeta, error) {
	if len(buf) != redisKeyMetaSize || buf[0] != redisKeyMetaSchemaVersion {
		return redisKeyMeta{}, errors.New("corrupted redis key metadata")
	}
	meta := redisKeyMeta{
		Type:          buf[1],
		StorageFormat: buf[2],
		Flags:         buf[3],
		ObjectID:      binary.BigEndian.Uint64(buf[4:12]),
		ExpireAtMs:    int64(binary.BigEndian.Uint64(buf[12:20])),
		Generation:    binary.BigEndian.Uint32(buf[20:24]),
	}
	if _, err := redisKeyTypeFromMetaCode(meta.Type); err != nil {
		return redisKeyMeta{}, err
	}
	if meta.StorageFormat == 0 {
		return redisKeyMeta{}, errors.New("corrupted redis key storage format")
	}
	return meta, nil
}

func loadRedisKeyMetaTx(tx *storage.Tx, ns redisNamespace, key []byte) (redisKeyMeta, bool, error) {
	bucket, err := tx.GetBucket(ns.keyMetaBucket)
	if errors.Is(err, storage.ErrBucketNotFound) {
		return redisKeyMeta{}, false, nil
	}
	if err != nil {
		return redisKeyMeta{}, false, err
	}
	raw, found := bucket.Get(key)
	if !found {
		return redisKeyMeta{}, false, nil
	}
	meta, err := deserializeRedisKeyMeta(raw)
	return meta, err == nil, err
}

func putRedisKeyMetaTx(tx *storage.Tx, ns redisNamespace, key []byte, meta redisKeyMeta) error {
	bucket, err := tx.CreateBucketIfNotExists(ns.keyMetaBucket)
	if err != nil {
		return err
	}
	return bucket.Put(key, meta.serialize())
}

func saveRedisKeyMetaForTypeTx(tx *storage.Tx, ns redisNamespace, key []byte, keyType string, objectID uint64) error {
	return saveRedisKeyMetaForTypeWithFormatTx(tx, ns, key, keyType, objectID, redisKeyStorageLegacy)
}

func saveRedisKeyMetaForTypeWithFormatTx(tx *storage.Tx, ns redisNamespace, key []byte, keyType string, objectID uint64, storageFormat byte) error {
	typeCode, err := redisKeyMetaTypeCode(keyType)
	if err != nil {
		return err
	}
	existing, found, err := loadRedisKeyMetaTx(tx, ns, key)
	if err != nil {
		return err
	}
	if found {
		existingType, typeErr := redisKeyTypeFromMetaCode(existing.Type)
		if typeErr != nil {
			return typeErr
		}
		if existingType != keyType {
			return fmt.Errorf("redis key metadata type conflict: have %s, writing %s", existingType, keyType)
		}
		if existing.StorageFormat != storageFormat {
			return fmt.Errorf("redis key metadata storage format conflict: have %d, writing %d", existing.StorageFormat, storageFormat)
		}
		if existing.ObjectID == objectID {
			return nil
		}
		existing.ObjectID = objectID
		return putRedisKeyMetaTx(tx, ns, key, existing)
	}
	expireAtMs := int64(-1)
	if value, hasTTL, loadErr := loadRedisExpireAtMsTx(tx, ns, key); loadErr != nil {
		return loadErr
	} else if hasTTL {
		expireAtMs = value
	}
	return putRedisKeyMetaTx(tx, ns, key, redisKeyMeta{
		Type:          typeCode,
		StorageFormat: storageFormat,
		ObjectID:      objectID,
		ExpireAtMs:    expireAtMs,
	})
}

func deleteRedisKeyMetaTx(tx *storage.Tx, ns redisNamespace, key []byte) error {
	bucket, err := tx.GetBucket(ns.keyMetaBucket)
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
	return err
}

func updateRedisKeyMetaExpiryTx(tx *storage.Tx, ns redisNamespace, key []byte, expireAtMs int64) error {
	meta, found, err := loadRedisKeyMetaTx(tx, ns, key)
	if err != nil || !found {
		return err
	}
	if meta.ExpireAtMs == expireAtMs {
		return nil
	}
	meta.ExpireAtMs = expireAtMs
	return putRedisKeyMetaTx(tx, ns, key, meta)
}

func redisObjectStorageFormatTx(tx *storage.Tx, ns redisNamespace, key []byte, keyType string, objectID uint64) (byte, error) {
	meta, found, err := loadRedisKeyMetaTx(tx, ns, key)
	if err != nil || !found {
		return redisKeyStorageLegacy, err
	}
	storedType, err := redisKeyTypeFromMetaCode(meta.Type)
	if err != nil {
		return 0, err
	}
	if storedType != keyType || meta.ObjectID != objectID {
		return 0, errors.New("redis key metadata does not match object metadata")
	}
	return meta.StorageFormat, nil
}

func serializeRedisKeyDirectoryState(state redisKeyDirectoryState) []byte {
	buf := make([]byte, 4+len(state.Cursor))
	buf[0] = redisKeyMetaSchemaVersion
	buf[1] = state.Phase
	buf[2] = state.BucketIndex
	copy(buf[4:], state.Cursor)
	return buf
}

func deserializeRedisKeyDirectoryState(buf []byte) (redisKeyDirectoryState, error) {
	if len(buf) < 4 || buf[0] != redisKeyMetaSchemaVersion {
		return redisKeyDirectoryState{}, errors.New("corrupted redis key directory state")
	}
	if buf[1] != redisKeyDirectoryPhaseBuilding && buf[1] != redisKeyDirectoryPhaseActive {
		return redisKeyDirectoryState{}, errors.New("corrupted redis key directory phase")
	}
	return redisKeyDirectoryState{Phase: buf[1], BucketIndex: buf[2], Cursor: cloneBytes(buf[4:])}, nil
}

func loadRedisKeyDirectoryStateTx(tx *storage.Tx, ns redisNamespace) (redisKeyDirectoryState, bool, error) {
	bucket, err := tx.GetBucket(ns.keyMetaSysBucket)
	if errors.Is(err, storage.ErrBucketNotFound) {
		return redisKeyDirectoryState{}, false, nil
	}
	if err != nil {
		return redisKeyDirectoryState{}, false, err
	}
	raw, found := bucket.Get(redisKeyDirectoryStateKey)
	if !found {
		return redisKeyDirectoryState{}, false, nil
	}
	state, err := deserializeRedisKeyDirectoryState(raw)
	return state, err == nil, err
}

func saveRedisKeyDirectoryStateTx(tx *storage.Tx, ns redisNamespace, state redisKeyDirectoryState) error {
	bucket, err := tx.CreateBucketIfNotExists(ns.keyMetaSysBucket)
	if err != nil {
		return err
	}
	return bucket.Put(redisKeyDirectoryStateKey, serializeRedisKeyDirectoryState(state))
}

func redisKeyDirectoryActiveTx(tx *storage.Tx, ns redisNamespace) (bool, error) {
	state, found, err := loadRedisKeyDirectoryStateTx(tx, ns)
	return found && state.Phase == redisKeyDirectoryPhaseActive, err
}
