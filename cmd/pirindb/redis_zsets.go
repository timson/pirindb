package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/timson/pirindb/storage"
)

var (
	redisZSetNextIDKey     = []byte("next_id")
	errRedisInvalidFloat   = errors.New("value is not a valid float")
	errRedisInvalidRange   = errors.New("min or max is not a float")
	errRedisInvalidLexItem = errors.New("min or max not valid string range item")
)

const (
	redisZSetMetaSize        = 16
	redisZSetSortableSize    = 8
	redisZSetIndexSepSize    = 1
	redisZSetIndexPrefixSize = redisZSetSortableSize + redisZSetIndexSepSize
)

type redisZSetMeta struct {
	ID          uint64
	Cardinality uint64
}

type redisZSetScoreMemberPair struct {
	score  float64
	member []byte
}

type redisZSetScoreBound struct {
	score     float64
	exclusive bool
}

type redisZSetLexBound struct {
	negativeInf bool
	positiveInf bool
	value       []byte
	exclusive   bool
}

type redisZSetRangeOptions struct {
	offset int
	count  int
}

func (meta *redisZSetMeta) serialize() []byte {
	buf := make([]byte, redisZSetMetaSize)
	binary.BigEndian.PutUint64(buf[0:8], meta.ID)
	binary.BigEndian.PutUint64(buf[8:16], meta.Cardinality)
	return buf
}

func deserializeRedisZSetMeta(buf []byte) (*redisZSetMeta, error) {
	if len(buf) != redisZSetMetaSize {
		return nil, errors.New("corrupted redis zset metadata")
	}
	return &redisZSetMeta{
		ID:          binary.BigEndian.Uint64(buf[0:8]),
		Cardinality: binary.BigEndian.Uint64(buf[8:16]),
	}, nil
}

func redisZSetMemberBucketName(ns redisNamespace, id uint64) []byte {
	name := make([]byte, len(ns.zsetMemberPrefix)+8)
	copy(name, ns.zsetMemberPrefix)
	binary.BigEndian.PutUint64(name[len(ns.zsetMemberPrefix):], id)
	return name
}

func redisZSetScoreBucketName(ns redisNamespace, id uint64) []byte {
	name := make([]byte, len(ns.zsetScorePrefix)+8)
	copy(name, ns.zsetScorePrefix)
	binary.BigEndian.PutUint64(name[len(ns.zsetScorePrefix):], id)
	return name
}

func normalizeRedisZSetScore(score float64) float64 {
	if score == 0 {
		return 0
	}
	return score
}

func parseRedisZSetScore(raw []byte, errMessage error) (float64, error) {
	score, err := strconv.ParseFloat(string(raw), 64)
	if err != nil || math.IsNaN(score) {
		return 0, errMessage
	}
	return normalizeRedisZSetScore(score), nil
}

func encodeRedisZSetSortableScore(score float64) []byte {
	score = normalizeRedisZSetScore(score)
	bits := math.Float64bits(score)
	if bits&(1<<63) != 0 {
		bits = ^bits
	} else {
		bits ^= 1 << 63
	}
	buf := make([]byte, redisZSetSortableSize)
	binary.BigEndian.PutUint64(buf, bits)
	return buf
}

func decodeRedisZSetSortableScore(buf []byte) (float64, error) {
	if len(buf) != redisZSetSortableSize {
		return 0, errors.New("corrupted redis zset score")
	}
	bits := binary.BigEndian.Uint64(buf)
	if bits&(1<<63) != 0 {
		bits ^= 1 << 63
	} else {
		bits = ^bits
	}
	return normalizeRedisZSetScore(math.Float64frombits(bits)), nil
}

func formatRedisZSetScore(score float64) string {
	switch {
	case math.IsInf(score, 1):
		return "inf"
	case math.IsInf(score, -1):
		return "-inf"
	default:
		return strconv.FormatFloat(normalizeRedisZSetScore(score), 'g', -1, 64)
	}
}

func redisZSetScoreIndexKey(score float64, member []byte) []byte {
	return redisZSetScoreIndexKeyFromEncoded(encodeRedisZSetSortableScore(score), member)
}

func redisZSetScoreIndexKeyFromEncoded(encodedScore []byte, member []byte) []byte {
	buf := make([]byte, redisZSetIndexPrefixSize+len(member))
	copy(buf[0:redisZSetSortableSize], encodedScore)
	buf[redisZSetSortableSize] = 0
	copy(buf[redisZSetIndexPrefixSize:], member)
	return buf
}

func decodeRedisZSetScoreIndexKey(key []byte) (float64, []byte, error) {
	if len(key) < redisZSetIndexPrefixSize || key[redisZSetSortableSize] != 0 {
		return 0, nil, errors.New("corrupted redis zset score index")
	}
	score, err := decodeRedisZSetSortableScore(key[:redisZSetSortableSize])
	if err != nil {
		return 0, nil, err
	}
	return score, cloneBytes(key[redisZSetIndexPrefixSize:]), nil
}

func parseRedisZSetLimitArgs(args [][]byte, optionsStart int) (redisZSetRangeOptions, error) {
	opts := redisZSetRangeOptions{count: -1}
	if len(args) == optionsStart {
		return opts, nil
	}
	if len(args) != optionsStart+3 {
		return opts, errors.New("syntax error")
	}
	if strings.ToUpper(string(args[optionsStart])) != "LIMIT" {
		return opts, fmt.Errorf("unsupported range option '%s'", strings.ToLower(string(args[optionsStart])))
	}
	offset, err := strconv.Atoi(string(args[optionsStart+1]))
	if err != nil || offset < 0 {
		return opts, errors.New("LIMIT offset or count is not an integer or out of range")
	}
	count, err := strconv.Atoi(string(args[optionsStart+2]))
	if err != nil || count < 0 {
		return opts, errors.New("LIMIT offset or count is not an integer or out of range")
	}
	opts.offset = offset
	opts.count = count
	return opts, nil
}

func parseRedisZSetScoreBound(raw []byte) (redisZSetScoreBound, error) {
	bound := redisZSetScoreBound{}
	token := string(raw)
	if token == "" {
		return bound, errRedisInvalidRange
	}
	if token[0] == '(' {
		bound.exclusive = true
		token = token[1:]
		if token == "" {
			return bound, errRedisInvalidRange
		}
	}
	score, err := parseRedisZSetScore([]byte(token), errRedisInvalidRange)
	if err != nil {
		return redisZSetScoreBound{}, err
	}
	bound.score = score
	return bound, nil
}

func parseRedisZSetLexBound(raw []byte) (redisZSetLexBound, error) {
	switch {
	case bytes.Equal(raw, []byte("-")):
		return redisZSetLexBound{negativeInf: true}, nil
	case bytes.Equal(raw, []byte("+")):
		return redisZSetLexBound{positiveInf: true}, nil
	case len(raw) == 0:
		return redisZSetLexBound{}, errRedisInvalidLexItem
	case raw[0] == '[':
		return redisZSetLexBound{value: cloneBytes(raw[1:])}, nil
	case raw[0] == '(':
		return redisZSetLexBound{value: cloneBytes(raw[1:]), exclusive: true}, nil
	default:
		return redisZSetLexBound{}, errRedisInvalidLexItem
	}
}

func matchesRedisZSetLexLower(member []byte, bound redisZSetLexBound) bool {
	if bound.negativeInf {
		return true
	}
	if bound.positiveInf {
		return false
	}
	cmp := bytes.Compare(member, bound.value)
	if bound.exclusive {
		return cmp > 0
	}
	return cmp >= 0
}

func matchesRedisZSetLexUpper(member []byte, bound redisZSetLexBound) bool {
	if bound.positiveInf {
		return true
	}
	if bound.negativeInf {
		return false
	}
	cmp := bytes.Compare(member, bound.value)
	if bound.exclusive {
		return cmp < 0
	}
	return cmp <= 0
}

func loadRedisZSetMetaTx(tx *storage.Tx, ns redisNamespace, key []byte) (*redisZSetMeta, bool, error) {
	bucket, err := tx.GetBucket(ns.zsetMetaBucket)
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
	meta, err := deserializeRedisZSetMeta(raw)
	if err != nil {
		return nil, false, err
	}
	return meta, true, nil
}

func saveRedisZSetMetaTx(tx *storage.Tx, ns redisNamespace, key []byte, meta *redisZSetMeta) error {
	bucket, err := tx.CreateBucketIfNotExists(ns.zsetMetaBucket)
	if err != nil {
		return err
	}
	if err = bucket.Put(key, meta.serialize()); err != nil {
		return err
	}
	return ensureRedisSlotIndexEntryTx(tx, ns, key)
}

func deleteRedisZSetMetaTx(tx *storage.Tx, ns redisNamespace, key []byte) error {
	bucket, err := tx.GetBucket(ns.zsetMetaBucket)
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

func nextRedisZSetIDTx(tx *storage.Tx, ns redisNamespace) (uint64, error) {
	bucket, err := tx.CreateBucketIfNotExists(ns.zsetSysBucket)
	if err != nil {
		return 0, err
	}
	raw, found := bucket.Get(redisZSetNextIDKey)
	if !found {
		initial := make([]byte, 8)
		binary.BigEndian.PutUint64(initial, 2)
		if err = bucket.Put(redisZSetNextIDKey, initial); err != nil {
			return 0, err
		}
		return 1, nil
	}
	if len(raw) != 8 {
		return 0, errors.New("corrupted redis zset id counter")
	}
	id := binary.BigEndian.Uint64(raw)
	next := make([]byte, 8)
	binary.BigEndian.PutUint64(next, id+1)
	if err = bucket.Put(redisZSetNextIDKey, next); err != nil {
		return 0, err
	}
	return id, nil
}

func ensureRedisZSetMemberBucketTx(tx *storage.Tx, ns redisNamespace, meta *redisZSetMeta) (*storage.Bucket, error) {
	return tx.CreateBucketIfNotExists(redisZSetMemberBucketName(ns, meta.ID))
}

func ensureRedisZSetScoreBucketTx(tx *storage.Tx, ns redisNamespace, meta *redisZSetMeta) (*storage.Bucket, error) {
	return tx.CreateBucketIfNotExists(redisZSetScoreBucketName(ns, meta.ID))
}

func deleteRedisZSetTx(tx *storage.Tx, ns redisNamespace, key []byte, meta *redisZSetMeta) error {
	if err := tx.DeleteBucket(redisZSetMemberBucketName(ns, meta.ID)); err != nil && !errors.Is(err, storage.ErrBucketNotFound) {
		return err
	}
	if err := tx.DeleteBucket(redisZSetScoreBucketName(ns, meta.ID)); err != nil && !errors.Is(err, storage.ErrBucketNotFound) {
		return err
	}
	if err := deleteRedisZSetMetaTx(tx, ns, key); err != nil {
		return err
	}
	return deleteRedisExpireAtMsTx(tx, ns, key)
}

func deleteRedisZSetIfExistsTx(tx *storage.Tx, ns redisNamespace, key []byte) (bool, error) {
	meta, found, err := loadRedisZSetMetaTx(tx, ns, key)
	if err != nil || !found {
		return false, err
	}
	return true, deleteRedisZSetTx(tx, ns, key, meta)
}

func loadRedisZSetMetaForReadTx(tx *storage.Tx, ns redisNamespace, key []byte, nowMs int64) (*redisZSetMeta, bool, error) {
	meta, found, err := loadRedisZSetMetaTx(tx, ns, key)
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
	if keyType == redisKeyTypeString || keyType == redisKeyTypeList || keyType == redisKeyTypeHash || keyType == redisKeyTypeBloom || keyType == redisKeyTypeTopK {
		return nil, false, errRedisWrongType
	}
	return nil, false, nil
}

func initRedisZSetMetaTx(tx *storage.Tx, ns redisNamespace, key []byte, nowMs int64) (*redisZSetMeta, error) {
	keyType, err := redisKeyTypeTx(tx, ns, key, nowMs)
	if err != nil {
		return nil, err
	}
	if keyType == redisKeyTypeString || keyType == redisKeyTypeList || keyType == redisKeyTypeHash || keyType == redisKeyTypeBloom || keyType == redisKeyTypeTopK {
		return nil, errRedisWrongType
	}

	id, err := nextRedisZSetIDTx(tx, ns)
	if err != nil {
		return nil, err
	}
	meta := &redisZSetMeta{ID: id}
	if _, err = ensureRedisZSetMemberBucketTx(tx, ns, meta); err != nil {
		return nil, err
	}
	if _, err = ensureRedisZSetScoreBucketTx(tx, ns, meta); err != nil {
		return nil, err
	}
	return meta, nil
}

func getRedisZSetBucketsTx(tx *storage.Tx, ns redisNamespace, key []byte, nowMs int64) (*storage.Bucket, *storage.Bucket, *redisZSetMeta, bool, error) {
	meta, found, err := loadRedisZSetMetaForReadTx(tx, ns, key, nowMs)
	if err != nil || !found {
		return nil, nil, meta, found, err
	}
	memberBucket, err := tx.GetBucket(redisZSetMemberBucketName(ns, meta.ID))
	if err != nil {
		return nil, nil, nil, false, err
	}
	scoreBucket, err := tx.GetBucket(redisZSetScoreBucketName(ns, meta.ID))
	if err != nil {
		return nil, nil, nil, false, err
	}
	return memberBucket, scoreBucket, meta, true, nil
}

func redisZSetAddTx(tx *storage.Tx, ns redisNamespace, key []byte, pairs []redisZSetScoreMemberPair, nowMs int64) (int64, error) {
	if _, err := purgeExpiredRedisKeyTx(tx, ns, key, nowMs); err != nil {
		return 0, err
	}
	meta, found, err := loadRedisZSetMetaTx(tx, ns, key)
	if err != nil {
		return 0, err
	}
	if !found {
		meta, err = initRedisZSetMetaTx(tx, ns, key, nowMs)
		if err != nil {
			return 0, err
		}
	}

	memberBucket, err := ensureRedisZSetMemberBucketTx(tx, ns, meta)
	if err != nil {
		return 0, err
	}
	scoreBucket, err := ensureRedisZSetScoreBucketTx(tx, ns, meta)
	if err != nil {
		return 0, err
	}

	var added int64
	for _, pair := range pairs {
		encodedScore := encodeRedisZSetSortableScore(pair.score)
		oldEncoded, exists := memberBucket.Get(pair.member)
		if exists {
			if !bytes.Equal(oldEncoded, encodedScore) {
				err = scoreBucket.Remove(redisZSetScoreIndexKeyFromEncoded(oldEncoded, pair.member))
				if err != nil && !errors.Is(err, storage.ErrNodeNotFound) {
					return 0, err
				}
			}
		} else {
			added++
			meta.Cardinality++
		}

		if err = memberBucket.Put(pair.member, encodedScore); err != nil {
			return 0, err
		}
		if err = scoreBucket.Put(redisZSetScoreIndexKeyFromEncoded(encodedScore, pair.member), []byte{}); err != nil {
			return 0, err
		}
	}

	if err = saveRedisZSetMetaTx(tx, ns, key, meta); err != nil {
		return 0, err
	}
	return added, nil
}

func redisZSetScoreTx(tx *storage.Tx, ns redisNamespace, key []byte, member []byte, nowMs int64) (float64, bool, error) {
	memberBucket, _, _, found, err := getRedisZSetBucketsTx(tx, ns, key, nowMs)
	if err != nil || !found {
		return 0, false, err
	}
	encodedScore, exists := memberBucket.Get(member)
	if !exists {
		return 0, false, nil
	}
	score, err := decodeRedisZSetSortableScore(encodedScore)
	if err != nil {
		return 0, false, err
	}
	return score, true, nil
}

func redisZSetCardTx(tx *storage.Tx, ns redisNamespace, key []byte, nowMs int64) (int64, error) {
	meta, found, err := loadRedisZSetMetaForReadTx(tx, ns, key, nowMs)
	if err != nil || !found {
		return 0, err
	}
	return int64(meta.Cardinality), nil
}

func redisZSetRemTx(tx *storage.Tx, ns redisNamespace, key []byte, members [][]byte, nowMs int64) (int64, error) {
	if _, err := purgeExpiredRedisKeyTx(tx, ns, key, nowMs); err != nil {
		return 0, err
	}
	memberBucket, scoreBucket, meta, found, err := getRedisZSetBucketsTx(tx, ns, key, nowMs)
	if err != nil || !found {
		return 0, err
	}

	var deleted int64
	for _, member := range members {
		encodedScore, exists := memberBucket.Get(member)
		if !exists {
			continue
		}
		if err = memberBucket.Remove(member); err != nil && !errors.Is(err, storage.ErrNodeNotFound) {
			return 0, err
		}
		if err = scoreBucket.Remove(redisZSetScoreIndexKeyFromEncoded(encodedScore, member)); err != nil && !errors.Is(err, storage.ErrNodeNotFound) {
			return 0, err
		}
		deleted++
		if meta.Cardinality > 0 {
			meta.Cardinality--
		}
	}

	if deleted == 0 {
		return 0, nil
	}
	if meta.Cardinality == 0 {
		return deleted, deleteRedisZSetTx(tx, ns, key, meta)
	}
	if err = saveRedisZSetMetaTx(tx, ns, key, meta); err != nil {
		return 0, err
	}
	return deleted, nil
}

func redisZSetForwardStart(cursor *storage.Cursor, min redisZSetScoreBound) ([]byte, []byte) {
	if math.IsInf(min.score, -1) && !min.exclusive {
		return cursor.First()
	}
	prefix := encodeRedisZSetSortableScore(min.score)
	if min.exclusive {
		seekKey := append(prefix, 0xFF)
		return cursor.Seek(seekKey)
	}
	seekKey := append(prefix, 0x00)
	return cursor.Seek(seekKey)
}

func redisZSetReverseStart(cursor *storage.Cursor, max redisZSetScoreBound) ([]byte, []byte) {
	if math.IsInf(max.score, 1) && !max.exclusive {
		return cursor.Last()
	}
	prefix := encodeRedisZSetSortableScore(max.score)
	var seekKey []byte
	if max.exclusive {
		seekKey = append(prefix, 0x00)
	} else {
		seekKey = append(prefix, 0xFF)
	}
	key, _ := cursor.Seek(seekKey)
	if key == nil {
		return cursor.Last()
	}
	return cursor.Prev()
}

func redisZSetScoreAboveUpper(score float64, upper redisZSetScoreBound) bool {
	if score > upper.score {
		return true
	}
	return upper.exclusive && score == upper.score
}

func redisZSetScoreBelowLower(score float64, lower redisZSetScoreBound) bool {
	if score < lower.score {
		return true
	}
	return lower.exclusive && score == lower.score
}

func redisZSetRangeByScoreTx(tx *storage.Tx, ns redisNamespace, key []byte, min redisZSetScoreBound, max redisZSetScoreBound, reverse bool, opts redisZSetRangeOptions, nowMs int64) ([][]byte, error) {
	_, scoreBucket, _, found, err := getRedisZSetBucketsTx(tx, ns, key, nowMs)
	if err != nil || !found {
		return nil, err
	}
	if opts.count == 0 {
		return [][]byte{}, nil
	}

	cursor := scoreBucket.Cursor()
	var (
		indexKey []byte
		value    []byte
	)
	if reverse {
		indexKey, value = redisZSetReverseStart(cursor, max)
	} else {
		indexKey, value = redisZSetForwardStart(cursor, min)
	}

	results := make([][]byte, 0)
	skipped := 0
	for indexKey != nil {
		score, member, err := decodeRedisZSetScoreIndexKey(indexKey)
		if err != nil {
			return nil, err
		}
		if reverse {
			if redisZSetScoreBelowLower(score, min) {
				break
			}
			if skipped < opts.offset {
				skipped++
			} else if opts.count < 0 || len(results) < opts.count {
				results = append(results, member)
				if opts.count >= 0 && len(results) >= opts.count {
					break
				}
			}
			indexKey, value = cursor.Prev()
		} else {
			if redisZSetScoreAboveUpper(score, max) {
				break
			}
			if skipped < opts.offset {
				skipped++
			} else if opts.count < 0 || len(results) < opts.count {
				results = append(results, member)
				if opts.count >= 0 && len(results) >= opts.count {
					break
				}
			}
			indexKey, value = cursor.Next()
		}
		_ = value
	}

	return results, nil
}

func redisZSetRangeByLexTx(tx *storage.Tx, ns redisNamespace, key []byte, min redisZSetLexBound, max redisZSetLexBound, reverse bool, opts redisZSetRangeOptions, nowMs int64) ([][]byte, error) {
	_, scoreBucket, _, found, err := getRedisZSetBucketsTx(tx, ns, key, nowMs)
	if err != nil || !found {
		return nil, err
	}
	if opts.count == 0 {
		return [][]byte{}, nil
	}

	cursor := scoreBucket.Cursor()
	var (
		indexKey []byte
		value    []byte
	)
	if reverse {
		indexKey, value = cursor.Last()
	} else {
		indexKey, value = cursor.First()
	}

	results := make([][]byte, 0)
	skipped := 0
	for indexKey != nil {
		_, member, err := decodeRedisZSetScoreIndexKey(indexKey)
		if err != nil {
			return nil, err
		}
		if matchesRedisZSetLexLower(member, min) && matchesRedisZSetLexUpper(member, max) {
			if skipped < opts.offset {
				skipped++
			} else if opts.count < 0 || len(results) < opts.count {
				results = append(results, member)
				if opts.count >= 0 && len(results) >= opts.count {
					break
				}
			}
		}
		if reverse {
			indexKey, value = cursor.Prev()
		} else {
			indexKey, value = cursor.Next()
		}
		_ = value
	}

	return results, nil
}

func redisZSetRemRangeByScoreTx(tx *storage.Tx, ns redisNamespace, key []byte, min redisZSetScoreBound, max redisZSetScoreBound, nowMs int64) (int64, error) {
	members, err := redisZSetRangeByScoreTx(tx, ns, key, min, max, false, redisZSetRangeOptions{count: -1}, nowMs)
	if err != nil {
		return 0, err
	}
	return redisZSetRemTx(tx, ns, key, members, nowMs)
}

func redisZSetRemRangeByLexTx(tx *storage.Tx, ns redisNamespace, key []byte, min redisZSetLexBound, max redisZSetLexBound, nowMs int64) (int64, error) {
	members, err := redisZSetRangeByLexTx(tx, ns, key, min, max, false, redisZSetRangeOptions{count: -1}, nowMs)
	if err != nil {
		return 0, err
	}
	return redisZSetRemTx(tx, ns, key, members, nowMs)
}
