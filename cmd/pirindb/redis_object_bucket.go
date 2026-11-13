package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"

	"github.com/timson/pirindb/storage"
)

const redisKeyStorageShared = 2

type redisObjectBucket struct {
	bucket *storage.Bucket
	prefix []byte
}

type redisObjectCursor struct {
	cursor *storage.Cursor
	prefix []byte
	upper  []byte
}

func redisObjectPrefix(objectID uint64) []byte {
	prefix := make([]byte, 8)
	binary.BigEndian.PutUint64(prefix, objectID)
	return prefix
}

func newRedisObjectBucket(bucket *storage.Bucket, objectID uint64, shared bool) *redisObjectBucket {
	store := &redisObjectBucket{bucket: bucket}
	if shared {
		store.prefix = redisObjectPrefix(objectID)
	}
	return store
}

func openRedisObjectBucketTx(tx *storage.Tx, sharedBucketName, legacyBucketName []byte, objectID uint64, storageFormat byte, create bool) (*redisObjectBucket, error) {
	if storageFormat == 0 {
		storageFormat = redisKeyStorageLegacy
	}
	if storageFormat == redisKeyStorageLegacy {
		var bucket *storage.Bucket
		var err error
		if create {
			bucket, err = tx.CreateBucketIfNotExists(legacyBucketName)
		} else {
			bucket, err = tx.GetBucket(legacyBucketName)
		}
		if err != nil {
			return nil, err
		}
		return newRedisObjectBucket(bucket, objectID, false), nil
	}
	if storageFormat != redisKeyStorageShared {
		return nil, errors.New("unsupported redis object storage format")
	}
	var bucket *storage.Bucket
	var err error
	if create {
		bucket, err = tx.CreateBucketIfNotExists(sharedBucketName)
	} else {
		bucket, err = tx.GetBucket(sharedBucketName)
	}
	if err != nil {
		return nil, err
	}
	return newRedisObjectBucket(bucket, objectID, true), nil
}

func (store *redisObjectBucket) entryKey(key []byte) []byte {
	if len(store.prefix) == 0 {
		return key
	}
	result := make([]byte, len(store.prefix)+len(key))
	copy(result, store.prefix)
	copy(result[len(store.prefix):], key)
	return result
}

func (store *redisObjectBucket) Get(key []byte) ([]byte, bool) {
	return store.bucket.Get(store.entryKey(key))
}

func (store *redisObjectBucket) ValueLen(key []byte) (int, bool, error) {
	return store.bucket.ValueLen(store.entryKey(key))
}

func (store *redisObjectBucket) Put(key, value []byte) error {
	return store.bucket.Put(store.entryKey(key), value)
}

func (store *redisObjectBucket) PutReader(key []byte, reader io.Reader, valueLen int64) error {
	return store.bucket.PutReader(store.entryKey(key), reader, valueLen)
}

func (store *redisObjectBucket) WriteValueTo(key []byte, writer io.Writer) (int64, bool, error) {
	return store.bucket.WriteValueTo(store.entryKey(key), writer)
}

func (store *redisObjectBucket) Remove(key []byte) error {
	return store.bucket.Remove(store.entryKey(key))
}

func (store *redisObjectBucket) NextSequence() (uint64, error) {
	return store.bucket.NextSequence()
}

func (store *redisObjectBucket) Cursor() *redisObjectCursor {
	cursor := &redisObjectCursor{cursor: store.bucket.Cursor(), prefix: cloneBytes(store.prefix)}
	if len(store.prefix) > 0 {
		cursor.upper = prefixUpperBound(store.prefix)
	}
	return cursor
}

func (store *redisObjectBucket) ForEach(fn func(k, v []byte) error) error {
	cursor := store.Cursor()
	for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
		if err := fn(key, value); err != nil {
			return err
		}
	}
	return cursor.Err()
}

func countRedisObjectEntries(store *redisObjectBucket) int {
	count, _ := countRedisObjectEntriesChecked(store)
	return count
}

func countRedisObjectEntriesChecked(store *redisObjectBucket) (int, error) {
	if store == nil {
		return 0, nil
	}
	count := 0
	cursor := store.Cursor()
	for key, _ := cursor.First(); key != nil; key, _ = cursor.Next() {
		count++
	}
	return count, cursor.Err()
}

func (cursor *redisObjectCursor) within(key []byte) bool {
	if key == nil {
		return false
	}
	if len(cursor.prefix) == 0 {
		return true
	}
	if cursor.upper != nil && bytes.Compare(key, cursor.upper) >= 0 {
		return false
	}
	return bytes.HasPrefix(key, cursor.prefix)
}

func (cursor *redisObjectCursor) strip(key []byte, value []byte) ([]byte, []byte) {
	if !cursor.within(key) {
		return nil, nil
	}
	return key[len(cursor.prefix):], value
}

func (cursor *redisObjectCursor) First() ([]byte, []byte) {
	if len(cursor.prefix) == 0 {
		return cursor.cursor.First()
	}
	key, value := cursor.cursor.Seek(cursor.prefix)
	return cursor.strip(key, value)
}

func (cursor *redisObjectCursor) Last() ([]byte, []byte) {
	if len(cursor.prefix) == 0 {
		return cursor.cursor.Last()
	}
	if cursor.upper == nil {
		key, value := cursor.cursor.Last()
		return cursor.strip(key, value)
	}
	key, _ := cursor.cursor.Seek(cursor.upper)
	if key == nil {
		key, value := cursor.cursor.Last()
		return cursor.strip(key, value)
	}
	key, value := cursor.cursor.Prev()
	return cursor.strip(key, value)
}

func (cursor *redisObjectCursor) Seek(entryKey []byte) ([]byte, []byte) {
	key, value := cursor.cursor.Seek(append(cloneBytes(cursor.prefix), entryKey...))
	return cursor.strip(key, value)
}

func (cursor *redisObjectCursor) Next() ([]byte, []byte) {
	key, value := cursor.cursor.Next()
	return cursor.strip(key, value)
}

func (cursor *redisObjectCursor) Prev() ([]byte, []byte) {
	key, value := cursor.cursor.Prev()
	return cursor.strip(key, value)
}

func (cursor *redisObjectCursor) Err() error {
	return cursor.cursor.Err()
}

func deleteRedisObjectBucketTx(tx *storage.Tx, store *redisObjectBucket, legacyBucketName []byte) error {
	if len(store.prefix) == 0 {
		err := tx.DeleteBucket(legacyBucketName)
		if errors.Is(err, storage.ErrBucketNotFound) {
			return nil
		}
		return err
	}
	cursor := store.Cursor()
	keys := make([][]byte, 0)
	for key, _ := cursor.First(); key != nil; key, _ = cursor.Next() {
		keys = append(keys, cloneBytes(key))
	}
	if err := cursor.Err(); err != nil {
		return err
	}
	for _, key := range keys {
		if err := store.Remove(key); err != nil && !errors.Is(err, storage.ErrNodeNotFound) {
			return err
		}
	}
	return nil
}
