package main

import (
	"bytes"
	"errors"
	"math"
	"math/big"
	"strconv"

	"github.com/timson/pirindb/storage"
)

const (
	defaultRedisScanCount = 10
)

type keyMatcher func(key []byte) bool

type keyValuePair struct {
	key   []byte
	value []byte
}

type redisSetOptions struct {
	onlyIfMissing bool
	onlyIfExists  bool
	returnOld     bool
	hasExpire     bool
	expireAtMs    int64
}

var errRedisIntegerOutOfRange = errors.New("value is not an integer or out of range")

func Status(db *storage.DB) *storage.DBStat {
	return db.Stat()
}

func cloneBytes(src []byte) []byte {
	if src == nil {
		return nil
	}
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

func parseRedisInt64(raw []byte) (int64, error) {
	value, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil {
		return 0, errRedisIntegerOutOfRange
	}
	return value, nil
}

func addRedisInt64(base, delta int64) (int64, error) {
	if delta > 0 && base > math.MaxInt64-delta {
		return 0, errRedisIntegerOutOfRange
	}
	if delta < 0 && base < math.MinInt64-delta {
		return 0, errRedisIntegerOutOfRange
	}
	return base + delta, nil
}

func subtractRedisInt64(base, delta int64) (int64, error) {
	if delta == math.MinInt64 {
		if base >= 0 {
			return 0, errRedisIntegerOutOfRange
		}
		return math.MaxInt64 + base + 1, nil
	}
	return addRedisInt64(base, -delta)
}

func putMainBytesTx(tx *storage.Tx, key, value []byte) error {
	bucket, err := tx.CreateBucketIfNotExists(DBBucket)
	if err != nil {
		return err
	}
	return bucket.Put(key, value)
}

func putManyMainBytesTx(tx *storage.Tx, pairs []keyValuePair) error {
	bucket, err := tx.CreateBucketIfNotExists(DBBucket)
	if err != nil {
		return err
	}
	for _, pair := range pairs {
		if err = bucket.Put(pair.key, pair.value); err != nil {
			return err
		}
	}
	return nil
}

func PutBytes(db *storage.DB, key, value []byte) error {
	return db.Update(func(tx *storage.Tx) error {
		return putMainBytesTx(tx, key, value)
	})
}

func Put(db *storage.DB, key string, value string) error {
	return PutBytes(db, []byte(key), []byte(value))
}

func PutManyBytes(db *storage.DB, pairs []keyValuePair) error {
	return db.Update(func(tx *storage.Tx) error {
		return putManyMainBytesTx(tx, pairs)
	})
}

func DeleteBytes(db *storage.DB, key []byte) (bool, error) {
	deletedN, err := DeleteManyBytes(db, [][]byte{key})
	return deletedN == 1, err
}

func Delete(db *storage.DB, key string) bool {
	deleted, err := DeleteBytes(db, []byte(key))
	return err == nil && deleted
}

func deleteManyMainBytesTx(tx *storage.Tx, keys [][]byte) (int64, error) {
	bucket, err := tx.GetBucket(DBBucket)
	if errors.Is(err, storage.ErrBucketNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	var deleted int64
	for _, key := range keys {
		err = bucket.Remove(key)
		if errors.Is(err, storage.ErrNodeNotFound) {
			continue
		}
		if err != nil {
			return 0, err
		}
		deleted++
	}

	return deleted, nil
}

func DeleteManyBytes(db *storage.DB, keys [][]byte) (int64, error) {
	var deleted int64
	err := db.Update(func(tx *storage.Tx) error {
		var innerErr error
		deleted, innerErr = deleteManyMainBytesTx(tx, keys)
		return innerErr
	})
	if err != nil {
		return 0, err
	}
	return deleted, nil
}

func getMainBytesTx(tx *storage.Tx, key []byte) ([]byte, bool, error) {
	bucket, err := tx.GetBucket(DBBucket)
	if errors.Is(err, storage.ErrBucketNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}

	value, isFound := bucket.Get(key)
	if !isFound {
		return nil, false, nil
	}
	return cloneBytes(value), true, nil
}

func GetBytes(db *storage.DB, key []byte) ([]byte, bool, error) {
	var result []byte
	var found bool
	err := db.View(func(tx *storage.Tx) error {
		var innerErr error
		result, found, innerErr = getMainBytesTx(tx, key)
		return innerErr
	})
	if err != nil {
		return nil, false, err
	}
	return result, found, nil
}

func Get(db *storage.DB, key string) (string, bool) {
	value, found, err := GetBytes(db, []byte(key))
	if err != nil || !found {
		return "", false
	}
	return string(value), true
}

func getManyMainBytesTx(tx *storage.Tx, keys [][]byte) ([][]byte, []bool, error) {
	values := make([][]byte, len(keys))
	found := make([]bool, len(keys))
	for i, key := range keys {
		value, isFound, err := getMainBytesTx(tx, key)
		if err != nil {
			return nil, nil, err
		}
		if isFound {
			values[i] = value
			found[i] = true
		}
	}
	return values, found, nil
}

func GetManyBytes(db *storage.DB, keys [][]byte) ([][]byte, []bool, error) {
	var (
		values [][]byte
		found  []bool
	)
	err := db.View(func(tx *storage.Tx) error {
		var innerErr error
		values, found, innerErr = getManyMainBytesTx(tx, keys)
		return innerErr
	})
	if err != nil {
		return nil, nil, err
	}
	return values, found, nil
}

func listMainKeysTx(tx *storage.Tx, matches keyMatcher) ([][]byte, error) {
	bucket, err := tx.GetBucket(DBBucket)
	if errors.Is(err, storage.ErrBucketNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	keys := make([][]byte, 0)
	cursor := bucket.Cursor()
	for key, _ := cursor.First(); key != nil; key, _ = cursor.Next() {
		if matches != nil && !matches(key) {
			continue
		}
		keys = append(keys, cloneBytes(key))
	}

	return keys, nil
}

func ListKeys(db *storage.DB, matches keyMatcher) ([][]byte, error) {
	var keys [][]byte
	err := db.View(func(tx *storage.Tx) error {
		var innerErr error
		keys, innerErr = listMainKeysTx(tx, matches)
		return innerErr
	})
	if err != nil {
		return nil, err
	}
	return keys, nil
}

func encodeScanCursor(key []byte) string {
	if len(key) == 0 {
		return "0"
	}

	// Prefix with a non-zero sentinel so arbitrary key bytes round-trip through
	// big.Int decimal encoding without losing leading zero bytes.
	payload := make([]byte, 1+len(key))
	payload[0] = 1
	copy(payload[1:], key)
	return new(big.Int).SetBytes(payload).String()
}

func decodeScanCursor(cursor string) ([]byte, error) {
	if cursor == "" || cursor == "0" {
		return nil, nil
	}

	value, ok := new(big.Int).SetString(cursor, 10)
	if !ok || value.Sign() <= 0 {
		return nil, errors.New("invalid scan cursor")
	}
	payload := value.Bytes()
	if len(payload) == 0 || payload[0] != 1 {
		return nil, errors.New("invalid scan cursor")
	}
	return cloneBytes(payload[1:]), nil
}

func nextScanSeekKey(lastKey []byte) []byte {
	if len(lastKey) == 0 {
		return nil
	}
	seekKey := make([]byte, len(lastKey)+1)
	copy(seekKey, lastKey)
	return seekKey
}

func prefixUpperBound(prefix []byte) []byte {
	if len(prefix) == 0 {
		return nil
	}
	upperBound := cloneBytes(prefix)
	for i := len(upperBound) - 1; i >= 0; i-- {
		if upperBound[i] == 0xFF {
			continue
		}
		upperBound[i]++
		return upperBound[:i+1]
	}
	return nil
}

func keyWithinPrefixRange(key, prefix, upperBound []byte) bool {
	if len(prefix) == 0 {
		return true
	}
	if upperBound != nil {
		return bytes.Compare(key, prefix) >= 0 && bytes.Compare(key, upperBound) < 0
	}
	return bytes.HasPrefix(key, prefix)
}

func scanMainKeysTx(tx *storage.Tx, cursorToken string, matches keyMatcher, count int) (string, [][]byte, error) {
	return scanBucketKeysTx(tx, DBBucket, cursorToken, matches, count)
}

func ScanKeys(db *storage.DB, cursorToken string, matches keyMatcher, count int) (string, [][]byte, error) {
	var (
		nextCursor string
		keys       [][]byte
	)
	err := db.View(func(tx *storage.Tx) error {
		var innerErr error
		nextCursor, keys, innerErr = scanMainKeysTx(tx, cursorToken, matches, count)
		return innerErr
	})
	if err != nil {
		return "", nil, err
	}

	return nextCursor, keys, nil
}

func scanBucketKeysTx(tx *storage.Tx, bucketName []byte, cursorToken string, matches keyMatcher, count int) (string, [][]byte, error) {
	if count <= 0 {
		count = defaultRedisScanCount
	}

	startAfter, err := decodeScanCursor(cursorToken)
	if err != nil {
		return "", nil, err
	}

	bucket, err := tx.GetBucket(bucketName)
	if errors.Is(err, storage.ErrBucketNotFound) {
		return "0", nil, nil
	}
	if err != nil {
		return "", nil, err
	}

	nextCursor := "0"
	keys := make([][]byte, 0, count)
	cursor := bucket.Cursor()

	var key []byte
	if len(startAfter) == 0 {
		key, _ = cursor.First()
	} else {
		key, _ = cursor.Seek(nextScanSeekKey(startAfter))
		for key != nil && bytes.Compare(key, startAfter) <= 0 {
			key, _ = cursor.Next()
		}
	}

	for ; key != nil; key, _ = cursor.Next() {
		currentKey := cloneBytes(key)
		if matches == nil || matches(currentKey) {
			keys = append(keys, currentKey)
			if len(keys) >= count {
				nextKey, _ := cursor.Next()
				if nextKey != nil {
					nextCursor = encodeScanCursor(currentKey)
				}
				break
			}
		}
	}

	return nextCursor, keys, nil
}

func putRedisBytesTx(tx *storage.Tx, ns redisNamespace, key, value []byte, nowMs int64) error {
	if _, err := purgeExpiredRedisKeyTx(tx, ns, key, nowMs); err != nil {
		return err
	}
	if _, err := deleteRedisListIfExistsTx(tx, ns, key); err != nil {
		return err
	}
	if _, err := deleteRedisHashIfExistsTx(tx, ns, key); err != nil {
		return err
	}
	if _, err := deleteRedisBloomIfExistsTx(tx, ns, key); err != nil {
		return err
	}
	if _, err := deleteRedisTopKIfExistsTx(tx, ns, key); err != nil {
		return err
	}
	if _, err := deleteRedisZSetIfExistsTx(tx, ns, key); err != nil {
		return err
	}
	bucket, err := tx.CreateBucketIfNotExists(ns.stringBucket)
	if err != nil {
		return err
	}
	if err = bucket.Put(key, value); err != nil {
		return err
	}
	if err = ensureRedisSlotIndexEntryTx(tx, ns, key); err != nil {
		return err
	}
	return deleteRedisExpireAtMsTx(tx, ns, key)
}

func putManyRedisBytesTx(tx *storage.Tx, ns redisNamespace, pairs []keyValuePair, nowMs int64) error {
	bucket, err := tx.CreateBucketIfNotExists(ns.stringBucket)
	if err != nil {
		return err
	}
	for _, pair := range pairs {
		if _, err = purgeExpiredRedisKeyTx(tx, ns, pair.key, nowMs); err != nil {
			return err
		}
		if _, err = deleteRedisListIfExistsTx(tx, ns, pair.key); err != nil {
			return err
		}
		if _, err = deleteRedisHashIfExistsTx(tx, ns, pair.key); err != nil {
			return err
		}
		if _, err = deleteRedisBloomIfExistsTx(tx, ns, pair.key); err != nil {
			return err
		}
		if _, err = deleteRedisTopKIfExistsTx(tx, ns, pair.key); err != nil {
			return err
		}
		if _, err = deleteRedisZSetIfExistsTx(tx, ns, pair.key); err != nil {
			return err
		}
		if err = bucket.Put(pair.key, pair.value); err != nil {
			return err
		}
		if err = ensureRedisSlotIndexEntryTx(tx, ns, pair.key); err != nil {
			return err
		}
		if err = deleteRedisExpireAtMsTx(tx, ns, pair.key); err != nil {
			return err
		}
	}
	return nil
}

func PutRedisBytes(db *storage.DB, ns redisNamespace, key, value []byte, nowMs int64) error {
	return db.Update(func(tx *storage.Tx) error {
		return putRedisBytesTx(tx, ns, key, value, nowMs)
	})
}

func PutManyRedisBytes(db *storage.DB, ns redisNamespace, pairs []keyValuePair, nowMs int64) error {
	return db.Update(func(tx *storage.Tx) error {
		return putManyRedisBytesTx(tx, ns, pairs, nowMs)
	})
}

func setRedisBytesTx(tx *storage.Tx, ns redisNamespace, key, value []byte, opts redisSetOptions, nowMs int64) ([]byte, bool, bool, error) {
	if _, err := purgeExpiredRedisKeyTx(tx, ns, key, nowMs); err != nil {
		return nil, false, false, err
	}

	keyType, err := redisRawKeyTypeTx(tx, ns, key)
	if err != nil {
		return nil, false, false, err
	}
	existed := keyType != redisKeyTypeNone

	var currentValue []byte
	var foundCurrentValue bool
	if opts.returnOld {
		switch keyType {
		case redisKeyTypeNone:
		case redisKeyTypeString:
			bucket, err := tx.GetBucket(ns.stringBucket)
			if errors.Is(err, storage.ErrBucketNotFound) {
				break
			}
			if err != nil {
				return nil, false, false, err
			}
			rawValue, found := bucket.Get(key)
			if found {
				currentValue = cloneBytes(rawValue)
				foundCurrentValue = true
			}
		default:
			return nil, false, false, errRedisWrongType
		}
	}

	if opts.onlyIfMissing && existed {
		return currentValue, foundCurrentValue, false, nil
	}
	if opts.onlyIfExists && !existed {
		return currentValue, foundCurrentValue, false, nil
	}

	if _, err = deleteRedisListIfExistsTx(tx, ns, key); err != nil {
		return nil, false, false, err
	}
	if _, err = deleteRedisHashIfExistsTx(tx, ns, key); err != nil {
		return nil, false, false, err
	}
	if _, err = deleteRedisBloomIfExistsTx(tx, ns, key); err != nil {
		return nil, false, false, err
	}
	if _, err = deleteRedisTopKIfExistsTx(tx, ns, key); err != nil {
		return nil, false, false, err
	}
	if _, err = deleteRedisZSetIfExistsTx(tx, ns, key); err != nil {
		return nil, false, false, err
	}

	bucket, err := tx.CreateBucketIfNotExists(ns.stringBucket)
	if err != nil {
		return nil, false, false, err
	}
	if err = bucket.Put(key, value); err != nil {
		return nil, false, false, err
	}
	if err = ensureRedisSlotIndexEntryTx(tx, ns, key); err != nil {
		return nil, false, false, err
	}
	if err = deleteRedisExpireAtMsTx(tx, ns, key); err != nil {
		return nil, false, false, err
	}
	if opts.hasExpire {
		if err = setRedisExpireAtMsTx(tx, ns, key, nowMs+opts.expireAtMs); err != nil {
			return nil, false, false, err
		}
	}
	return currentValue, foundCurrentValue, true, nil
}

func SetRedisBytes(db *storage.DB, ns redisNamespace, key, value []byte, opts redisSetOptions, nowMs int64) ([]byte, bool, bool, error) {
	var (
		currentValue []byte
		found        bool
		applied      bool
	)
	err := db.Update(func(tx *storage.Tx) error {
		var innerErr error
		currentValue, found, applied, innerErr = setRedisBytesTx(tx, ns, key, value, opts, nowMs)
		return innerErr
	})
	if err != nil {
		return nil, false, false, err
	}
	return currentValue, found, applied, nil
}

func getSetRedisBytesTx(tx *storage.Tx, ns redisNamespace, key, value []byte, nowMs int64) ([]byte, bool, error) {
	currentValue, found, _, err := setRedisBytesTx(tx, ns, key, value, redisSetOptions{returnOld: true}, nowMs)
	return currentValue, found, err
}

func GetSetRedisBytes(db *storage.DB, ns redisNamespace, key, value []byte, nowMs int64) ([]byte, bool, error) {
	var (
		currentValue []byte
		found        bool
	)
	err := db.Update(func(tx *storage.Tx) error {
		var innerErr error
		currentValue, found, innerErr = getSetRedisBytesTx(tx, ns, key, value, nowMs)
		return innerErr
	})
	if err != nil {
		return nil, false, err
	}
	return currentValue, found, nil
}

func applyRedisIntDeltaTx(tx *storage.Tx, ns redisNamespace, key []byte, delta int64, subtract bool, nowMs int64) (int64, error) {
	if _, err := purgeExpiredRedisKeyTx(tx, ns, key, nowMs); err != nil {
		return 0, err
	}

	keyType, err := redisRawKeyTypeTx(tx, ns, key)
	if err != nil {
		return 0, err
	}

	var current int64
	switch keyType {
	case redisKeyTypeNone:
	case redisKeyTypeString:
		bucket, err := tx.GetBucket(ns.stringBucket)
		if errors.Is(err, storage.ErrBucketNotFound) {
			break
		}
		if err != nil {
			return 0, err
		}
		rawValue, found := bucket.Get(key)
		if !found {
			break
		}
		current, err = parseRedisInt64(rawValue)
		if err != nil {
			return 0, err
		}
	default:
		return 0, errRedisWrongType
	}

	var next int64
	if subtract {
		next, err = subtractRedisInt64(current, delta)
	} else {
		next, err = addRedisInt64(current, delta)
	}
	if err != nil {
		return 0, err
	}

	bucket, err := tx.CreateBucketIfNotExists(ns.stringBucket)
	if err != nil {
		return 0, err
	}
	if err = bucket.Put(key, []byte(strconv.FormatInt(next, 10))); err != nil {
		return 0, err
	}
	if err = ensureRedisSlotIndexEntryTx(tx, ns, key); err != nil {
		return 0, err
	}
	return next, nil
}

func ApplyRedisIntDelta(db *storage.DB, ns redisNamespace, key []byte, delta int64, subtract bool, nowMs int64) (int64, error) {
	var updated int64
	err := db.Update(func(tx *storage.Tx) error {
		var innerErr error
		updated, innerErr = applyRedisIntDeltaTx(tx, ns, key, delta, subtract, nowMs)
		return innerErr
	})
	if err != nil {
		return 0, err
	}
	return updated, nil
}

func deleteManyRedisBytesTx(tx *storage.Tx, ns redisNamespace, keys [][]byte, nowMs int64) (int64, error) {
	var deleted int64
	for _, key := range keys {
		if _, err := purgeExpiredRedisKeyTx(tx, ns, key, nowMs); err != nil {
			return 0, err
		}
		keyType, err := redisRawKeyTypeTx(tx, ns, key)
		if err != nil {
			return 0, err
		}
		removed, err := deleteRedisKeyByRawTypeTx(tx, ns, key, keyType)
		if err != nil {
			return 0, err
		}
		if removed {
			deleted++
		}
	}

	return deleted, nil
}

func DeleteManyRedisBytes(db *storage.DB, ns redisNamespace, keys [][]byte, nowMs int64) (int64, error) {
	var deleted int64
	err := db.Update(func(tx *storage.Tx) error {
		var innerErr error
		deleted, innerErr = deleteManyRedisBytesTx(tx, ns, keys, nowMs)
		return innerErr
	})
	if err != nil {
		return 0, err
	}
	return deleted, nil
}

func countExistingRedisKeysTx(tx *storage.Tx, ns redisNamespace, keys [][]byte, nowMs int64) (int64, error) {
	var existing int64
	for _, key := range keys {
		keyType, err := redisKeyTypeTx(tx, ns, key, nowMs)
		if err != nil {
			return 0, err
		}
		if keyType != redisKeyTypeNone {
			existing++
		}
	}
	return existing, nil
}

func CountExistingRedisKeys(db *storage.DB, ns redisNamespace, keys [][]byte, nowMs int64) (int64, error) {
	var existing int64
	err := db.View(func(tx *storage.Tx) error {
		var innerErr error
		existing, innerErr = countExistingRedisKeysTx(tx, ns, keys, nowMs)
		return innerErr
	})
	if err != nil {
		return 0, err
	}
	return existing, nil
}

func getRedisBytesTx(tx *storage.Tx, ns redisNamespace, key []byte, nowMs int64) ([]byte, bool, error) {
	keyType, err := redisKeyTypeTx(tx, ns, key, nowMs)
	if err != nil {
		return nil, false, err
	}
	switch keyType {
	case redisKeyTypeNone:
		return nil, false, nil
	case redisKeyTypeString:
	default:
		return nil, false, errRedisWrongType
	}

	bucket, err := tx.GetBucket(ns.stringBucket)
	if errors.Is(err, storage.ErrBucketNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}

	value, isFound := bucket.Get(key)
	if !isFound {
		return nil, false, nil
	}
	return cloneBytes(value), true, nil
}

func GetRedisBytes(db *storage.DB, ns redisNamespace, key []byte, nowMs int64) ([]byte, bool, error) {
	var result []byte
	var found bool
	err := db.View(func(tx *storage.Tx) error {
		var innerErr error
		result, found, innerErr = getRedisBytesTx(tx, ns, key, nowMs)
		return innerErr
	})
	if err != nil {
		return nil, false, err
	}
	return result, found, nil
}

func getManyRedisBytesTx(tx *storage.Tx, ns redisNamespace, keys [][]byte, nowMs int64) ([][]byte, []bool, error) {
	values := make([][]byte, len(keys))
	found := make([]bool, len(keys))
	for i, key := range keys {
		value, isFound, err := getRedisBytesTx(tx, ns, key, nowMs)
		if err != nil {
			return nil, nil, err
		}
		if isFound {
			values[i] = value
			found[i] = true
		}
	}
	return values, found, nil
}

func GetManyRedisBytes(db *storage.DB, ns redisNamespace, keys [][]byte, nowMs int64) ([][]byte, []bool, error) {
	var (
		values [][]byte
		found  []bool
	)
	err := db.View(func(tx *storage.Tx) error {
		var innerErr error
		values, found, innerErr = getManyRedisBytesTx(tx, ns, keys, nowMs)
		return innerErr
	})
	if err != nil {
		return nil, nil, err
	}
	return values, found, nil
}

func listRedisKeysTx(tx *storage.Tx, ns redisNamespace, matches keyMatcher, nowMs int64) ([][]byte, error) {
	return collectRedisKeysTx(tx, ns, matches, nowMs)
}

func ListRedisKeys(db *storage.DB, ns redisNamespace, matches keyMatcher, nowMs int64) ([][]byte, error) {
	var keys [][]byte
	err := db.View(func(tx *storage.Tx) error {
		var innerErr error
		keys, innerErr = listRedisKeysTx(tx, ns, matches, nowMs)
		return innerErr
	})
	if err != nil {
		return nil, err
	}
	return keys, nil
}

type redisBucketKeyIterator struct {
	cursor *storage.Cursor
	key    []byte
}

func redisKeyBucketNames(ns redisNamespace) [][]byte {
	return [][]byte{
		ns.stringBucket,
		ns.listMetaBucket,
		ns.hashMetaBucket,
		ns.bloomMetaBucket,
		ns.topkMetaBucket,
		ns.zsetMetaBucket,
	}
}

func newRedisBucketKeyIterator(tx *storage.Tx, bucketName, seekKey []byte) (*redisBucketKeyIterator, error) {
	bucket, err := tx.GetBucket(bucketName)
	if errors.Is(err, storage.ErrBucketNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	cursor := bucket.Cursor()
	var key []byte
	if len(seekKey) == 0 {
		key, _ = cursor.First()
	} else {
		key, _ = cursor.Seek(seekKey)
	}
	if key == nil {
		return nil, nil
	}
	return &redisBucketKeyIterator{
		cursor: cursor,
		key:    cloneBytes(key),
	}, nil
}

func (it *redisBucketKeyIterator) advance() {
	if it == nil || it.cursor == nil {
		return
	}
	key, _ := it.cursor.Next()
	it.key = cloneBytes(key)
}

func iterateRedisKeysTx(tx *storage.Tx, ns redisNamespace, startAfter, prefix []byte, matches keyMatcher, count int, nowMs int64) (string, [][]byte, error) {
	upperBound := prefixUpperBound(prefix)
	seekKey := []byte(nil)
	switch {
	case len(prefix) == 0 && len(startAfter) == 0:
	case len(prefix) == 0:
		seekKey = nextScanSeekKey(startAfter)
	case len(startAfter) == 0 || bytes.Compare(startAfter, prefix) < 0:
		seekKey = cloneBytes(prefix)
	case upperBound != nil && bytes.Compare(startAfter, upperBound) >= 0:
		return "0", nil, nil
	default:
		seekKey = nextScanSeekKey(startAfter)
	}

	iterators := make([]*redisBucketKeyIterator, 0, len(redisKeyBucketNames(ns)))
	for _, bucketName := range redisKeyBucketNames(ns) {
		iterator, iteratorErr := newRedisBucketKeyIterator(tx, bucketName, seekKey)
		if iteratorErr != nil {
			return "", nil, iteratorErr
		}
		for iterator != nil && iterator.key != nil && len(startAfter) > 0 && bytes.Compare(iterator.key, startAfter) <= 0 {
			iterator.advance()
		}
		if iterator != nil && iterator.key == nil {
			iterator = nil
		}
		if iterator != nil && keyWithinPrefixRange(iterator.key, prefix, upperBound) {
			iterators = append(iterators, iterator)
		}
	}
	if len(iterators) == 0 {
		return "0", nil, nil
	}

	nextCursor := "0"
	keys := make([][]byte, 0, count)
	for len(iterators) > 0 {
		key := iterators[0].key
		for _, iterator := range iterators[1:] {
			if bytes.Compare(iterator.key, key) < 0 {
				key = iterator.key
			}
		}

		for i := 0; i < len(iterators); {
			if bytes.Equal(iterators[i].key, key) {
				iterators[i].advance()
				if iterators[i].key == nil || !keyWithinPrefixRange(iterators[i].key, prefix, upperBound) {
					iterators = append(iterators[:i], iterators[i+1:]...)
					continue
				}
			}
			i++
		}

		expired, expiredErr := redisKeyExpiredTx(tx, ns, key, nowMs)
		if expiredErr != nil {
			return "", nil, expiredErr
		}
		if expired {
			continue
		}
		if matches != nil && !matches(key) {
			continue
		}

		keys = append(keys, cloneBytes(key))
		if count > 0 && len(keys) >= count {
			if len(iterators) > 0 {
				nextCursor = encodeScanCursor(key)
			}
			break
		}
	}
	return nextCursor, keys, nil
}

func scanRedisKeysTx(tx *storage.Tx, ns redisNamespace, cursorToken string, matches keyMatcher, count int, nowMs int64) (string, [][]byte, error) {
	if count <= 0 {
		count = defaultRedisScanCount
	}

	startAfter, err := decodeScanCursor(cursorToken)
	if err != nil {
		return "", nil, err
	}

	return iterateRedisKeysTx(tx, ns, startAfter, nil, matches, count, nowMs)
}

func ScanRedisKeys(db *storage.DB, ns redisNamespace, cursorToken string, matches keyMatcher, count int, nowMs int64) (string, [][]byte, error) {
	var (
		nextCursor string
		keys       [][]byte
	)
	err := db.View(func(tx *storage.Tx) error {
		var innerErr error
		nextCursor, keys, innerErr = scanRedisKeysTx(tx, ns, cursorToken, matches, count, nowMs)
		return innerErr
	})
	if err != nil {
		return "", nil, err
	}

	return nextCursor, keys, nil
}

func flushAllRedisTx(tx *storage.Tx) error {
	buckets := tx.Buckets()
	toDelete := make([][]byte, 0, len(buckets))
	for _, bucketName := range buckets {
		if !isRedisReservedBucketName(bucketName) {
			continue
		}
		toDelete = append(toDelete, cloneBytes(bucketName))
	}

	for _, bucketName := range toDelete {
		if err := tx.DeleteBucket(bucketName); err != nil && !errors.Is(err, storage.ErrBucketNotFound) {
			return err
		}
	}
	return nil
}

func FlushAllRedis(db *storage.DB) error {
	return db.Update(func(tx *storage.Tx) error {
		return flushAllRedisTx(tx)
	})
}

func isRedisDBBucketName(ns redisNamespace, bucketName []byte) bool {
	switch {
	case bytes.Equal(bucketName, ns.stringBucket),
		bytes.Equal(bucketName, ns.listMetaBucket),
		bytes.Equal(bucketName, ns.listSysBucket),
		bytes.Equal(bucketName, ns.hashMetaBucket),
		bytes.Equal(bucketName, ns.hashSysBucket),
		bytes.Equal(bucketName, ns.bloomMetaBucket),
		bytes.Equal(bucketName, ns.bloomSysBucket),
		bytes.Equal(bucketName, ns.topkMetaBucket),
		bytes.Equal(bucketName, ns.topkSysBucket),
		bytes.Equal(bucketName, ns.zsetMetaBucket),
		bytes.Equal(bucketName, ns.zsetSysBucket),
		bytes.Equal(bucketName, ns.slotIndexBucket),
		bytes.Equal(bucketName, ns.expireMetaBucket),
		bytes.Equal(bucketName, ns.expireIndexBucket):
		return true
	case bytes.HasPrefix(bucketName, ns.listDataPrefix), bytes.HasPrefix(bucketName, ns.hashDataPrefix), bytes.HasPrefix(bucketName, ns.bloomDataPrefix),
		bytes.HasPrefix(bucketName, ns.topkDataPrefix), bytes.HasPrefix(bucketName, ns.zsetMemberPrefix), bytes.HasPrefix(bucketName, ns.zsetScorePrefix):
		return true
	default:
		return false
	}
}

func flushRedisDBTx(tx *storage.Tx, ns redisNamespace) error {
	buckets := tx.Buckets()
	toDelete := make([][]byte, 0, len(buckets))
	for _, bucketName := range buckets {
		if !isRedisDBBucketName(ns, bucketName) {
			continue
		}
		toDelete = append(toDelete, cloneBytes(bucketName))
	}

	for _, bucketName := range toDelete {
		if err := tx.DeleteBucket(bucketName); err != nil && !errors.Is(err, storage.ErrBucketNotFound) {
			return err
		}
	}
	return nil
}

func FlushRedisDB(db *storage.DB, ns redisNamespace) error {
	return db.Update(func(tx *storage.Tx) error {
		return flushRedisDBTx(tx, ns)
	})
}

func countRedisDBKeysFastTx(tx *storage.Tx, ns redisNamespace) (int64, error) {
	var total uint64
	for _, bucketName := range redisKeyBucketNames(ns) {
		bucket, bucketErr := tx.GetBucket(bucketName)
		if errors.Is(bucketErr, storage.ErrBucketNotFound) {
			continue
		}
		if bucketErr != nil {
			return 0, bucketErr
		}
		total += bucket.ItemCount()
	}
	return int64(total), nil
}

func countRedisDBKeysTx(tx *storage.Tx, ns redisNamespace, nowMs int64) (int64, error) {
	return countRedisDBKeysFastTx(tx, ns)
}

func CountRedisDBKeys(db *storage.DB, ns redisNamespace, nowMs int64) (int64, error) {
	var count int64
	err := db.View(func(tx *storage.Tx) error {
		var innerErr error
		count, innerErr = countRedisDBKeysTx(tx, ns, nowMs)
		return innerErr
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func renameRedisKeyTx(tx *storage.Tx, ns redisNamespace, source, destination []byte, onlyIfDestinationMissing bool, nowMs int64) (bool, string, error) {
	if _, err := purgeExpiredRedisKeyTx(tx, ns, source, nowMs); err != nil {
		return false, redisKeyTypeNone, err
	}
	if _, err := purgeExpiredRedisKeyTx(tx, ns, destination, nowMs); err != nil {
		return false, redisKeyTypeNone, err
	}

	sourceType, err := redisRawKeyTypeTx(tx, ns, source)
	if err != nil {
		return false, redisKeyTypeNone, err
	}
	if sourceType == redisKeyTypeNone {
		return false, redisKeyTypeNone, errRedisNoSuchKey
	}
	if bytes.Equal(source, destination) {
		if onlyIfDestinationMissing {
			return false, sourceType, nil
		}
		return true, sourceType, nil
	}

	destinationType, err := redisRawKeyTypeTx(tx, ns, destination)
	if err != nil {
		return false, redisKeyTypeNone, err
	}
	if onlyIfDestinationMissing && destinationType != redisKeyTypeNone {
		return false, sourceType, nil
	}
	if destinationType != redisKeyTypeNone {
		if _, err = deleteRedisKeyByRawTypeTx(tx, ns, destination, destinationType); err != nil {
			return false, redisKeyTypeNone, err
		}
	}

	expireAtMs, hasTTL, err := loadRedisExpireAtMsTx(tx, ns, source)
	if err != nil {
		return false, redisKeyTypeNone, err
	}

	switch sourceType {
	case redisKeyTypeString:
		bucket, err := tx.GetBucket(ns.stringBucket)
		if err != nil {
			return false, redisKeyTypeNone, err
		}
		value, found := bucket.Get(source)
		if !found {
			return false, redisKeyTypeNone, errRedisNoSuchKey
		}
		if err = bucket.Put(destination, cloneBytes(value)); err != nil {
			return false, redisKeyTypeNone, err
		}
		if err = ensureRedisSlotIndexEntryTx(tx, ns, destination); err != nil {
			return false, redisKeyTypeNone, err
		}
		if err = bucket.Remove(source); err != nil && !errors.Is(err, storage.ErrNodeNotFound) {
			return false, redisKeyTypeNone, err
		}
		if err = deleteRedisSlotIndexEntryTx(tx, ns, source); err != nil {
			return false, redisKeyTypeNone, err
		}
	case redisKeyTypeList:
		meta, found, err := loadRedisListMetaTx(tx, ns, source)
		if err != nil {
			return false, redisKeyTypeNone, err
		}
		if !found {
			return false, redisKeyTypeNone, errRedisNoSuchKey
		}
		if err = saveRedisListMetaTx(tx, ns, destination, meta); err != nil {
			return false, redisKeyTypeNone, err
		}
		if err = deleteRedisListMetaTx(tx, ns, source); err != nil {
			return false, redisKeyTypeNone, err
		}
	case redisKeyTypeHash:
		meta, found, err := loadRedisHashMetaTx(tx, ns, source)
		if err != nil {
			return false, redisKeyTypeNone, err
		}
		if !found {
			return false, redisKeyTypeNone, errRedisNoSuchKey
		}
		if err = saveRedisHashMetaTx(tx, ns, destination, meta); err != nil {
			return false, redisKeyTypeNone, err
		}
		if err = deleteRedisHashMetaTx(tx, ns, source); err != nil {
			return false, redisKeyTypeNone, err
		}
	case redisKeyTypeBloom:
		meta, found, err := loadRedisBloomMetaTx(tx, ns, source)
		if err != nil {
			return false, redisKeyTypeNone, err
		}
		if !found {
			return false, redisKeyTypeNone, errRedisNoSuchKey
		}
		if err = saveRedisBloomMetaTx(tx, ns, destination, meta); err != nil {
			return false, redisKeyTypeNone, err
		}
		if err = deleteRedisBloomMetaTx(tx, ns, source); err != nil {
			return false, redisKeyTypeNone, err
		}
	case redisKeyTypeTopK:
		meta, found, err := loadRedisTopKMetaTx(tx, ns, source)
		if err != nil {
			return false, redisKeyTypeNone, err
		}
		if !found {
			return false, redisKeyTypeNone, errRedisNoSuchKey
		}
		if err = saveRedisTopKMetaTx(tx, ns, destination, meta); err != nil {
			return false, redisKeyTypeNone, err
		}
		if err = deleteRedisTopKMetaTx(tx, ns, source); err != nil {
			return false, redisKeyTypeNone, err
		}
	case redisKeyTypeZSet:
		meta, found, err := loadRedisZSetMetaTx(tx, ns, source)
		if err != nil {
			return false, redisKeyTypeNone, err
		}
		if !found {
			return false, redisKeyTypeNone, errRedisNoSuchKey
		}
		if err = saveRedisZSetMetaTx(tx, ns, destination, meta); err != nil {
			return false, redisKeyTypeNone, err
		}
		if err = deleteRedisZSetMetaTx(tx, ns, source); err != nil {
			return false, redisKeyTypeNone, err
		}
	default:
		return false, redisKeyTypeNone, errors.New("unsupported redis key type")
	}

	if err = deleteRedisExpireAtMsTx(tx, ns, source); err != nil {
		return false, redisKeyTypeNone, err
	}
	if hasTTL {
		if err = setRedisExpireAtMsTx(tx, ns, destination, expireAtMs); err != nil {
			return false, redisKeyTypeNone, err
		}
	}
	return true, sourceType, nil
}

func RenameRedisKey(db *storage.DB, ns redisNamespace, source, destination []byte, onlyIfDestinationMissing bool, nowMs int64) (bool, string, error) {
	var (
		renamed    bool
		sourceType string
	)
	err := db.Update(func(tx *storage.Tx) error {
		var innerErr error
		renamed, sourceType, innerErr = renameRedisKeyTx(tx, ns, source, destination, onlyIfDestinationMissing, nowMs)
		return innerErr
	})
	if err != nil {
		return false, redisKeyTypeNone, err
	}
	return renamed, sourceType, nil
}

func collectRedisKeysTx(tx *storage.Tx, ns redisNamespace, matches keyMatcher, nowMs int64) ([][]byte, error) {
	_, keys, err := iterateRedisKeysTx(tx, ns, nil, nil, matches, 0, nowMs)
	return keys, err
}

func collectRedisKeysByPrefixTx(tx *storage.Tx, ns redisNamespace, prefix []byte, nowMs int64) ([][]byte, error) {
	_, keys, err := iterateRedisKeysTx(tx, ns, nil, prefix, nil, 0, nowMs)
	return keys, err
}

func listRedisKeysByPrefixTx(tx *storage.Tx, ns redisNamespace, prefix []byte, nowMs int64) ([][]byte, error) {
	return collectRedisKeysByPrefixTx(tx, ns, prefix, nowMs)
}

func ListRedisKeysByPrefix(db *storage.DB, ns redisNamespace, prefix []byte, nowMs int64) ([][]byte, error) {
	var keys [][]byte
	err := db.View(func(tx *storage.Tx) error {
		var innerErr error
		keys, innerErr = listRedisKeysByPrefixTx(tx, ns, prefix, nowMs)
		return innerErr
	})
	if err != nil {
		return nil, err
	}
	return keys, nil
}

func scanRedisKeysByPrefixTx(tx *storage.Tx, ns redisNamespace, cursorToken string, prefix []byte, count int, nowMs int64) (string, [][]byte, error) {
	if count <= 0 {
		count = defaultRedisScanCount
	}

	startAfter, err := decodeScanCursor(cursorToken)
	if err != nil {
		return "", nil, err
	}

	return iterateRedisKeysTx(tx, ns, startAfter, prefix, nil, count, nowMs)
}

func ScanRedisKeysByPrefix(db *storage.DB, ns redisNamespace, cursorToken string, prefix []byte, count int, nowMs int64) (string, [][]byte, error) {
	var (
		nextCursor string
		keys       [][]byte
	)
	err := db.View(func(tx *storage.Tx) error {
		var innerErr error
		nextCursor, keys, innerErr = scanRedisKeysByPrefixTx(tx, ns, cursorToken, prefix, count, nowMs)
		return innerErr
	})
	if err != nil {
		return "", nil, err
	}
	return nextCursor, keys, nil
}

func collectBucketKeysTx(tx *storage.Tx, bucketName []byte, matches keyMatcher, keySet map[string][]byte) error {
	bucket, err := tx.GetBucket(bucketName)
	if errors.Is(err, storage.ErrBucketNotFound) {
		return nil
	}
	if err != nil {
		return err
	}

	cursor := bucket.Cursor()
	for key, _ := cursor.First(); key != nil; key, _ = cursor.Next() {
		if matches != nil && !matches(key) {
			continue
		}
		keySet[string(key)] = cloneBytes(key)
	}
	return nil
}
