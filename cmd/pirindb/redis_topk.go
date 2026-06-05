package main

import (
	"bytes"
	"container/heap"
	"encoding/binary"
	"errors"
	"hash/crc64"
	"hash/fnv"
	"math"
	"math/bits"
	"sort"
	"strconv"
	"strings"

	"github.com/timson/pirindb/storage"
)

const (
	redisTopKMetaSize          = 48
	redisTopKCounterCellSize   = 16
	redisTopKCounterCellsBlock = redisBloomBlockSize / redisTopKCounterCellSize

	redisTopKMetaPrefix    = byte('h')
	redisTopKCounterPrefix = byte('b')

	redisTopKDefaultWidth = uint64(8)
	redisTopKDefaultDepth = uint64(7)
	redisTopKDefaultDecay = 0.9

	redisTopKMaxIncrement = uint64(100000)
)

var (
	redisTopKNextIDKey             = []byte("next_id")
	redisTopKCRC64Table            = crc64.MakeTable(crc64.ECMA)
	errRedisTopKItemExists         = errors.New("item exists")
	errRedisTopKInvalidK           = errors.New("topk must be a positive integer")
	errRedisTopKInvalidWidth       = errors.New("width must be a positive integer")
	errRedisTopKInvalidDepth       = errors.New("depth must be a positive integer")
	errRedisTopKInvalidDecay       = errors.New("invalid decay value")
	errRedisTopKInvalidIncrement   = errors.New("increment must be between 1 and 100000")
	errRedisTopKCorruptedHeapEntry = errors.New("corrupted redis topk heap entry")
)

type redisTopKMeta struct {
	ID        uint64
	K         uint64
	Width     uint64
	Depth     uint64
	HeapSize  uint64
	DecayBits uint64
}

type redisTopKHeapEntry struct {
	Count uint64
	Item  []byte
}

type redisTopKCounterCell struct {
	Fingerprint uint64
	Count       uint64
}

type redisTopKHeapEntries []redisTopKHeapEntry

func (meta *redisTopKMeta) serialize() []byte {
	buf := make([]byte, redisTopKMetaSize)
	binary.BigEndian.PutUint64(buf[0:8], meta.ID)
	binary.BigEndian.PutUint64(buf[8:16], meta.K)
	binary.BigEndian.PutUint64(buf[16:24], meta.Width)
	binary.BigEndian.PutUint64(buf[24:32], meta.Depth)
	binary.BigEndian.PutUint64(buf[32:40], meta.HeapSize)
	binary.BigEndian.PutUint64(buf[40:48], meta.DecayBits)
	return buf
}

func (meta *redisTopKMeta) Decay() float64 {
	return math.Float64frombits(meta.DecayBits)
}

func (meta *redisTopKMeta) SetDecay(decay float64) {
	meta.DecayBits = math.Float64bits(decay)
}

func deserializeRedisTopKMeta(buf []byte) (*redisTopKMeta, error) {
	if len(buf) != redisTopKMetaSize {
		return nil, errors.New("corrupted redis topk metadata")
	}
	return &redisTopKMeta{
		ID:        binary.BigEndian.Uint64(buf[0:8]),
		K:         binary.BigEndian.Uint64(buf[8:16]),
		Width:     binary.BigEndian.Uint64(buf[16:24]),
		Depth:     binary.BigEndian.Uint64(buf[24:32]),
		HeapSize:  binary.BigEndian.Uint64(buf[32:40]),
		DecayBits: binary.BigEndian.Uint64(buf[40:48]),
	}, nil
}

func (entry redisTopKHeapEntry) serialize() []byte {
	buf := make([]byte, 8+len(entry.Item))
	binary.BigEndian.PutUint64(buf[0:8], entry.Count)
	copy(buf[8:], entry.Item)
	return buf
}

func deserializeRedisTopKHeapEntry(buf []byte) (redisTopKHeapEntry, error) {
	if len(buf) < 8 {
		return redisTopKHeapEntry{}, errRedisTopKCorruptedHeapEntry
	}
	return redisTopKHeapEntry{
		Count: binary.BigEndian.Uint64(buf[0:8]),
		Item:  cloneBytes(buf[8:]),
	}, nil
}

func (entries redisTopKHeapEntries) Len() int {
	return len(entries)
}

func (entries redisTopKHeapEntries) Less(i, j int) bool {
	return redisTopKEntryWorse(entries[i], entries[j])
}

func (entries redisTopKHeapEntries) Swap(i, j int) {
	entries[i], entries[j] = entries[j], entries[i]
}

func (entries *redisTopKHeapEntries) Push(x any) {
	*entries = append(*entries, x.(redisTopKHeapEntry))
}

func (entries *redisTopKHeapEntries) Pop() any {
	old := *entries
	last := len(old) - 1
	entry := old[last]
	*entries = old[:last]
	return entry
}

func redisTopKDataBucketName(ns redisNamespace, id uint64) []byte {
	name := make([]byte, len(ns.topkDataPrefix)+8)
	copy(name, ns.topkDataPrefix)
	binary.BigEndian.PutUint64(name[len(ns.topkDataPrefix):], id)
	return name
}

func redisTopKHeapKey(index uint64) []byte {
	key := make([]byte, 9)
	key[0] = redisTopKMetaPrefix
	binary.BigEndian.PutUint64(key[1:], index)
	return key
}

func redisTopKCounterBlockKey(row uint64, blockIndex uint64) []byte {
	key := make([]byte, 17)
	key[0] = redisTopKCounterPrefix
	binary.BigEndian.PutUint64(key[1:9], row)
	binary.BigEndian.PutUint64(key[9:17], blockIndex)
	return key
}

func parseRedisTopKReserveArgs(args [][]byte) ([]byte, uint64, uint64, uint64, float64, error) {
	if len(args) != 3 && len(args) != 6 {
		return nil, 0, 0, 0, 0, errors.New("wrong number of arguments for 'topk.reserve' command")
	}

	k, err := strconv.ParseUint(string(args[2]), 10, 64)
	if err != nil || k == 0 {
		return nil, 0, 0, 0, 0, errRedisTopKInvalidK
	}

	width := redisTopKDefaultWidth
	depth := redisTopKDefaultDepth
	decay := redisTopKDefaultDecay
	if len(args) == 6 {
		width, err = strconv.ParseUint(string(args[3]), 10, 64)
		if err != nil || width == 0 {
			return nil, 0, 0, 0, 0, errRedisTopKInvalidWidth
		}
		depth, err = strconv.ParseUint(string(args[4]), 10, 64)
		if err != nil || depth == 0 {
			return nil, 0, 0, 0, 0, errRedisTopKInvalidDepth
		}
		decay, err = strconv.ParseFloat(string(args[5]), 64)
		if err != nil || math.IsNaN(decay) || math.IsInf(decay, 0) || decay <= 0 || decay >= 1 {
			return nil, 0, 0, 0, 0, errRedisTopKInvalidDecay
		}
	}

	return args[1], k, width, depth, decay, nil
}

func parseRedisTopKIncrByArgs(args [][]byte) ([]byte, [][]byte, []uint64, error) {
	if len(args) < 4 || len(args)%2 != 0 {
		return nil, nil, nil, errors.New("wrong number of arguments for 'topk.incrby' command")
	}

	items := make([][]byte, 0, (len(args)-2)/2)
	increments := make([]uint64, 0, (len(args)-2)/2)
	for i := 2; i < len(args); i += 2 {
		increment, err := strconv.ParseUint(string(args[i+1]), 10, 64)
		if err != nil || increment == 0 || increment > redisTopKMaxIncrement {
			return nil, nil, nil, errRedisTopKInvalidIncrement
		}
		items = append(items, args[i])
		increments = append(increments, increment)
	}
	return args[1], items, increments, nil
}

func parseRedisTopKListArgs(args [][]byte) ([]byte, bool, error) {
	if len(args) != 2 && len(args) != 3 {
		return nil, false, errors.New("wrong number of arguments for 'topk.list' command")
	}
	withCount := false
	if len(args) == 3 {
		if strings.ToUpper(string(args[2])) != "WITHCOUNT" {
			return nil, false, errors.New("syntax error")
		}
		withCount = true
	}
	return args[1], withCount, nil
}

func loadRedisTopKMetaTx(tx *storage.Tx, ns redisNamespace, key []byte) (*redisTopKMeta, bool, error) {
	bucket, err := tx.GetBucket(ns.topkMetaBucket)
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
	meta, err := deserializeRedisTopKMeta(raw)
	if err != nil {
		return nil, false, err
	}
	return meta, true, nil
}

func saveRedisTopKMetaTx(tx *storage.Tx, ns redisNamespace, key []byte, meta *redisTopKMeta) error {
	bucket, err := tx.CreateBucketIfNotExists(ns.topkMetaBucket)
	if err != nil {
		return err
	}
	if err = bucket.Put(key, meta.serialize()); err != nil {
		return err
	}
	return ensureRedisSlotIndexEntryTx(tx, ns, key)
}

func deleteRedisTopKMetaTx(tx *storage.Tx, ns redisNamespace, key []byte) error {
	bucket, err := tx.GetBucket(ns.topkMetaBucket)
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

func nextRedisTopKIDTx(tx *storage.Tx, ns redisNamespace) (uint64, error) {
	bucket, err := tx.CreateBucketIfNotExists(ns.topkSysBucket)
	if err != nil {
		return 0, err
	}
	raw, found := bucket.Get(redisTopKNextIDKey)
	if !found {
		initial := make([]byte, 8)
		binary.BigEndian.PutUint64(initial, 2)
		if err = bucket.Put(redisTopKNextIDKey, initial); err != nil {
			return 0, err
		}
		return 1, nil
	}
	if len(raw) != 8 {
		return 0, errors.New("corrupted redis topk id counter")
	}
	id := binary.BigEndian.Uint64(raw)
	next := make([]byte, 8)
	binary.BigEndian.PutUint64(next, id+1)
	if err = bucket.Put(redisTopKNextIDKey, next); err != nil {
		return 0, err
	}
	return id, nil
}

func ensureRedisTopKDataBucketTx(tx *storage.Tx, ns redisNamespace, meta *redisTopKMeta) (*storage.Bucket, error) {
	return tx.CreateBucketIfNotExists(redisTopKDataBucketName(ns, meta.ID))
}

func getRedisTopKDataBucketTx(tx *storage.Tx, ns redisNamespace, meta *redisTopKMeta) (*storage.Bucket, error) {
	return tx.GetBucket(redisTopKDataBucketName(ns, meta.ID))
}

func loadRedisTopKCounterBlock(bucket *storage.Bucket, row uint64, blockIndex uint64) ([]byte, error) {
	raw, found := bucket.Get(redisTopKCounterBlockKey(row, blockIndex))
	if !found {
		return make([]byte, redisBloomBlockSize), nil
	}
	if len(raw) != redisBloomBlockSize {
		return nil, errors.New("corrupted redis topk counter block")
	}
	return cloneBytes(raw), nil
}

func saveRedisTopKCounterBlock(bucket *storage.Bucket, row uint64, blockIndex uint64, block []byte) error {
	if len(block) != redisBloomBlockSize {
		return errors.New("corrupted redis topk counter block")
	}
	return bucket.Put(redisTopKCounterBlockKey(row, blockIndex), block)
}

func loadRedisTopKHeap(bucket *storage.Bucket, heapSize uint64) ([]redisTopKHeapEntry, error) {
	entries := make([]redisTopKHeapEntry, 0, heapSize)
	for i := uint64(0); i < heapSize; i++ {
		raw, found := bucket.Get(redisTopKHeapKey(i))
		if !found {
			return nil, errRedisTopKCorruptedHeapEntry
		}
		entry, err := deserializeRedisTopKHeapEntry(raw)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func saveRedisTopKHeap(bucket *storage.Bucket, previousSize uint64, entries []redisTopKHeapEntry) error {
	for i, entry := range entries {
		if err := bucket.Put(redisTopKHeapKey(uint64(i)), entry.serialize()); err != nil {
			return err
		}
	}
	for i := uint64(len(entries)); i < previousSize; i++ {
		err := bucket.Remove(redisTopKHeapKey(i))
		if err != nil && !errors.Is(err, storage.ErrNodeNotFound) {
			return err
		}
	}
	return nil
}

func redisTopKCounterCellOffset(cellIndex uint64) int {
	return int(cellIndex * redisTopKCounterCellSize)
}

func readRedisTopKCounterCell(block []byte, cellIndex uint64) redisTopKCounterCell {
	offset := redisTopKCounterCellOffset(cellIndex)
	return redisTopKCounterCell{
		Fingerprint: binary.BigEndian.Uint64(block[offset : offset+8]),
		Count:       binary.BigEndian.Uint64(block[offset+8 : offset+16]),
	}
}

func writeRedisTopKCounterCell(block []byte, cellIndex uint64, cell redisTopKCounterCell) {
	offset := redisTopKCounterCellOffset(cellIndex)
	binary.BigEndian.PutUint64(block[offset:offset+8], cell.Fingerprint)
	binary.BigEndian.PutUint64(block[offset+8:offset+16], cell.Count)
}

func redisTopKEntryWorse(left redisTopKHeapEntry, right redisTopKHeapEntry) bool {
	if left.Count != right.Count {
		return left.Count < right.Count
	}
	return bytes.Compare(left.Item, right.Item) > 0
}

func redisTopKEntryBetter(left redisTopKHeapEntry, right redisTopKHeapEntry) bool {
	if left.Count != right.Count {
		return left.Count > right.Count
	}
	return bytes.Compare(left.Item, right.Item) < 0
}

func redisTopKFingerprintAndColumns(item []byte, width uint64, depth uint64) (uint64, []uint64) {
	h1 := crc64.Checksum(item, redisTopKCRC64Table)
	hasher := fnv.New64a()
	_, _ = hasher.Write(item)
	h2 := hasher.Sum64()
	if h2 == 0 {
		h2 = 1
	}
	fingerprint := h1 ^ bits.RotateLeft64(h2, 32)
	if fingerprint == 0 {
		fingerprint = 1
	}
	columns := make([]uint64, depth)
	for row := uint64(0); row < depth; row++ {
		columns[row] = (h1 + row*h2) % width
	}
	return fingerprint, columns
}

func redisTopKRandomFloat64(randFn func() uint64) float64 {
	if randFn == nil {
		return 0
	}
	return float64(randFn()>>11) * (1.0 / (1 << 53))
}

func redisTopKShouldDecay(randFn func() uint64, decay float64, count uint64) bool {
	probability := math.Pow(decay, float64(count))
	if probability >= 1 {
		return true
	}
	if probability <= 0 {
		return false
	}
	return redisTopKRandomFloat64(randFn) < probability
}

func deleteRedisTopKTx(tx *storage.Tx, ns redisNamespace, key []byte, meta *redisTopKMeta) error {
	if err := tx.DeleteBucket(redisTopKDataBucketName(ns, meta.ID)); err != nil && !errors.Is(err, storage.ErrBucketNotFound) {
		return err
	}
	if err := deleteRedisTopKMetaTx(tx, ns, key); err != nil {
		return err
	}
	return deleteRedisExpireAtMsTx(tx, ns, key)
}

func deleteRedisTopKIfExistsTx(tx *storage.Tx, ns redisNamespace, key []byte) (bool, error) {
	meta, found, err := loadRedisTopKMetaTx(tx, ns, key)
	if err != nil || !found {
		return false, err
	}
	return true, deleteRedisTopKTx(tx, ns, key, meta)
}

func loadRedisTopKMetaForReadTx(tx *storage.Tx, ns redisNamespace, key []byte, nowMs int64) (*redisTopKMeta, bool, error) {
	meta, found, err := loadRedisTopKMetaTx(tx, ns, key)
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
	if keyType == redisKeyTypeString || keyType == redisKeyTypeList || keyType == redisKeyTypeHash || keyType == redisKeyTypeBloom || keyType == redisKeyTypeZSet {
		return nil, false, errRedisWrongType
	}
	return nil, false, nil
}

func createRedisTopKTx(tx *storage.Tx, ns redisNamespace, key []byte, k uint64, width uint64, depth uint64, decay float64, nowMs int64) (*redisTopKMeta, *storage.Bucket, error) {
	if _, err := purgeExpiredRedisKeyTx(tx, ns, key, nowMs); err != nil {
		return nil, nil, err
	}

	keyType, err := redisKeyTypeTx(tx, ns, key, nowMs)
	if err != nil {
		return nil, nil, err
	}
	if keyType != redisKeyTypeNone {
		return nil, nil, errRedisTopKItemExists
	}

	id, err := nextRedisTopKIDTx(tx, ns)
	if err != nil {
		return nil, nil, err
	}
	meta := &redisTopKMeta{
		ID:       id,
		K:        k,
		Width:    width,
		Depth:    depth,
		HeapSize: 0,
	}
	meta.SetDecay(decay)
	dataBucket, err := ensureRedisTopKDataBucketTx(tx, ns, meta)
	if err != nil {
		return nil, nil, err
	}
	if err = saveRedisTopKMetaTx(tx, ns, key, meta); err != nil {
		return nil, nil, err
	}
	if err = deleteRedisExpireAtMsTx(tx, ns, key); err != nil {
		return nil, nil, err
	}
	return meta, dataBucket, nil
}

func getRedisTopKBucketTx(tx *storage.Tx, ns redisNamespace, key []byte, nowMs int64) (*storage.Bucket, *redisTopKMeta, error) {
	meta, found, err := loadRedisTopKMetaForReadTx(tx, ns, key, nowMs)
	if err != nil {
		return nil, nil, err
	}
	if !found {
		return nil, nil, errRedisNoSuchKey
	}
	dataBucket, err := getRedisTopKDataBucketTx(tx, ns, meta)
	if err != nil {
		return nil, nil, err
	}
	return dataBucket, meta, nil
}

func redisTopKEstimateCountTx(bucket *storage.Bucket, meta *redisTopKMeta, item []byte) (uint64, error) {
	fingerprint, columns := redisTopKFingerprintAndColumns(item, meta.Width, meta.Depth)
	var minCount uint64
	found := false
	for row, column := range columns {
		blockIndex := column / uint64(redisTopKCounterCellsBlock)
		cellIndex := column % uint64(redisTopKCounterCellsBlock)
		block, err := loadRedisTopKCounterBlock(bucket, uint64(row), blockIndex)
		if err != nil {
			return 0, err
		}
		cell := readRedisTopKCounterCell(block, cellIndex)
		if cell.Fingerprint != fingerprint || cell.Count == 0 {
			return 0, nil
		}
		if !found || cell.Count < minCount {
			minCount = cell.Count
			found = true
		}
	}
	if !found {
		return 0, nil
	}
	return minCount, nil
}

func redisTopKIncrementItem(bucket *storage.Bucket, meta *redisTopKMeta, item []byte, increment uint64, randFn func() uint64) (uint64, error) {
	fingerprint, columns := redisTopKFingerprintAndColumns(item, meta.Width, meta.Depth)
	blockCache := make(map[[2]uint64][]byte)
	dirtyBlocks := make(map[[2]uint64]struct{})

	loadBlock := func(row uint64, blockIndex uint64) ([]byte, error) {
		cacheKey := [2]uint64{row, blockIndex}
		if block, ok := blockCache[cacheKey]; ok {
			return block, nil
		}
		block, err := loadRedisTopKCounterBlock(bucket, row, blockIndex)
		if err != nil {
			return nil, err
		}
		blockCache[cacheKey] = block
		return block, nil
	}

	for step := uint64(0); step < increment; step++ {
		for row, column := range columns {
			blockIndex := column / uint64(redisTopKCounterCellsBlock)
			cellIndex := column % uint64(redisTopKCounterCellsBlock)
			block, err := loadBlock(uint64(row), blockIndex)
			if err != nil {
				return 0, err
			}

			cell := readRedisTopKCounterCell(block, cellIndex)
			modified := false

			switch {
			case cell.Count == 0:
				cell.Fingerprint = fingerprint
				cell.Count = 1
				modified = true
			case cell.Fingerprint == fingerprint:
				if cell.Count < ^uint64(0) {
					cell.Count++
					modified = true
				}
			case redisTopKShouldDecay(randFn, meta.Decay(), cell.Count):
				if cell.Count <= 1 {
					cell.Fingerprint = fingerprint
					cell.Count = 1
				} else {
					cell.Count--
				}
				modified = true
			}

			if modified {
				writeRedisTopKCounterCell(block, cellIndex, cell)
				dirtyBlocks[[2]uint64{uint64(row), blockIndex}] = struct{}{}
			}
		}
	}

	for cacheKey := range dirtyBlocks {
		if err := saveRedisTopKCounterBlock(bucket, cacheKey[0], cacheKey[1], blockCache[cacheKey]); err != nil {
			return 0, err
		}
	}

	var minCount uint64
	found := false
	for row, column := range columns {
		blockIndex := column / uint64(redisTopKCounterCellsBlock)
		cellIndex := column % uint64(redisTopKCounterCellsBlock)
		block, err := loadBlock(uint64(row), blockIndex)
		if err != nil {
			return 0, err
		}
		cell := readRedisTopKCounterCell(block, cellIndex)
		if cell.Fingerprint != fingerprint || cell.Count == 0 {
			return 0, nil
		}
		if !found || cell.Count < minCount {
			minCount = cell.Count
			found = true
		}
	}
	if !found {
		return 0, nil
	}
	return minCount, nil
}

func redisTopKReconcileHeap(entries []redisTopKHeapEntry, k uint64, item []byte, count uint64) ([]redisTopKHeapEntry, []byte, bool) {
	if k == 0 || count == 0 {
		return entries, nil, false
	}

	heapEntries := redisTopKHeapEntries(entries)
	heap.Init(&heapEntries)
	entries = []redisTopKHeapEntry(heapEntries)

	for i := range entries {
		if bytes.Equal(entries[i].Item, item) {
			if entries[i].Count == count {
				return entries, nil, false
			}
			entries[i].Count = count
			heapEntries = redisTopKHeapEntries(entries)
			heap.Init(&heapEntries)
			return []redisTopKHeapEntry(heapEntries), nil, true
		}
	}

	candidate := redisTopKHeapEntry{
		Count: count,
		Item:  cloneBytes(item),
	}
	if uint64(len(entries)) < k {
		entries = append(entries, candidate)
		heapEntries = redisTopKHeapEntries(entries)
		heap.Init(&heapEntries)
		return []redisTopKHeapEntry(heapEntries), nil, true
	}
	if len(entries) == 0 || !redisTopKEntryBetter(candidate, entries[0]) {
		return entries, nil, false
	}

	dropped := cloneBytes(entries[0].Item)
	entries[0] = candidate
	heapEntries = redisTopKHeapEntries(entries)
	heap.Init(&heapEntries)
	return []redisTopKHeapEntry(heapEntries), dropped, true
}

func redisTopKReserveTx(tx *storage.Tx, ns redisNamespace, key []byte, k uint64, width uint64, depth uint64, decay float64, nowMs int64) error {
	_, _, err := createRedisTopKTx(tx, ns, key, k, width, depth, decay, nowMs)
	return err
}

func redisTopKAddTx(tx *storage.Tx, ns redisNamespace, key []byte, items [][]byte, nowMs int64, randFn func() uint64) ([][]byte, error) {
	return redisTopKIncrByTx(tx, ns, key, items, make([]uint64, len(items)), nowMs, randFn)
}

func redisTopKIncrByTx(tx *storage.Tx, ns redisNamespace, key []byte, items [][]byte, increments []uint64, nowMs int64, randFn func() uint64) ([][]byte, error) {
	if len(items) != len(increments) {
		return nil, errors.New("topk item/increment mismatch")
	}
	dataBucket, meta, err := getRedisTopKBucketTx(tx, ns, key, nowMs)
	if err != nil {
		return nil, err
	}

	results := make([][]byte, 0, len(items))
	for idx, item := range items {
		increment := increments[idx]
		if increment == 0 {
			increment = 1
		}

		count, err := redisTopKIncrementItem(dataBucket, meta, item, increment, randFn)
		if err != nil {
			return nil, err
		}

		entries, err := loadRedisTopKHeap(dataBucket, meta.HeapSize)
		if err != nil {
			return nil, err
		}
		updatedEntries, dropped, changed := redisTopKReconcileHeap(entries, meta.K, item, count)
		if changed {
			previousSize := meta.HeapSize
			meta.HeapSize = uint64(len(updatedEntries))
			if err = saveRedisTopKHeap(dataBucket, previousSize, updatedEntries); err != nil {
				return nil, err
			}
			if meta.HeapSize != previousSize {
				if err = saveRedisTopKMetaTx(tx, ns, key, meta); err != nil {
					return nil, err
				}
			}
		}
		results = append(results, dropped)
	}
	return results, nil
}

func redisTopKQueryTx(tx *storage.Tx, ns redisNamespace, key []byte, items [][]byte, nowMs int64) ([]int64, error) {
	dataBucket, meta, err := getRedisTopKBucketTx(tx, ns, key, nowMs)
	if err != nil {
		return nil, err
	}
	entries, err := loadRedisTopKHeap(dataBucket, meta.HeapSize)
	if err != nil {
		return nil, err
	}
	results := make([]int64, 0, len(items))
	for _, item := range items {
		found := false
		for _, entry := range entries {
			if bytes.Equal(entry.Item, item) {
				found = true
				break
			}
		}
		if found {
			results = append(results, 1)
		} else {
			results = append(results, 0)
		}
	}
	return results, nil
}

func redisTopKCountTx(tx *storage.Tx, ns redisNamespace, key []byte, items [][]byte, nowMs int64) ([]int64, error) {
	dataBucket, meta, err := getRedisTopKBucketTx(tx, ns, key, nowMs)
	if err != nil {
		return nil, err
	}
	results := make([]int64, 0, len(items))
	for _, item := range items {
		count, err := redisTopKEstimateCountTx(dataBucket, meta, item)
		if err != nil {
			return nil, err
		}
		results = append(results, uint64ToRedisInt(count))
	}
	return results, nil
}

func redisTopKListTx(tx *storage.Tx, ns redisNamespace, key []byte, nowMs int64) ([]redisTopKHeapEntry, error) {
	dataBucket, meta, err := getRedisTopKBucketTx(tx, ns, key, nowMs)
	if err != nil {
		return nil, err
	}
	entries, err := loadRedisTopKHeap(dataBucket, meta.HeapSize)
	if err != nil {
		return nil, err
	}
	sortedEntries := make([]redisTopKHeapEntry, len(entries))
	copy(sortedEntries, entries)
	sort.Slice(sortedEntries, func(i, j int) bool {
		if sortedEntries[i].Count != sortedEntries[j].Count {
			return sortedEntries[i].Count > sortedEntries[j].Count
		}
		return strings.Compare(string(sortedEntries[i].Item), string(sortedEntries[j].Item)) < 0
	})
	return sortedEntries, nil
}

func redisTopKInfoTx(tx *storage.Tx, ns redisNamespace, key []byte, nowMs int64) (*redisTopKMeta, error) {
	meta, found, err := loadRedisTopKMetaForReadTx(tx, ns, key, nowMs)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errRedisNoSuchKey
	}
	return meta, nil
}

func uint64ToRedisInt(value uint64) int64 {
	if value > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(value)
}
