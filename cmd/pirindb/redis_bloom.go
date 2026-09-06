package main

import (
	"encoding/binary"
	"errors"
	"hash/crc64"
	"hash/fnv"
	"math"
	"strconv"
	"strings"

	"github.com/timson/pirindb/storage"
)

const (
	redisBloomMetaSize              = 48
	redisBloomSubFilterMetaSize     = 40
	redisBloomBlockSize             = 512
	redisBloomBlockBits             = redisBloomBlockSize * 8
	redisBloomScalingTighteningRate = 0.5
	redisBloomSubMetaPrefix         = byte('m')
	redisBloomBlockPrefix           = byte('b')
	redisBloomNonScalingFlag        = uint64(1)

	redisBloomAutoCreateErrorRate = 0.01
	redisBloomAutoCreateCapacity  = uint64(100)
	redisBloomAutoCreateExpansion = uint64(2)
)

var (
	redisBloomNextIDKey           = []byte("next_id")
	redisBloomCRC64Table          = crc64.MakeTable(crc64.ECMA)
	errRedisBloomItemExists       = errors.New("item exists")
	errRedisBloomFull             = errors.New("nonscaling bloom filter is full")
	errRedisBloomInvalidRate      = errors.New("error rate must be a float between 0 and 1")
	errRedisBloomInvalidCapacity  = errors.New("capacity must be a positive integer")
	errRedisBloomInvalidExpansion = errors.New("expansion must be a positive integer")
)

type redisBloomMeta struct {
	ID              uint64
	InitialCapacity uint64
	ErrorRateBits   uint64
	Expansion       uint64
	Flags           uint64
	SubFilterCount  uint64
	StorageFormat   byte
}

type redisBloomSubFilterMeta struct {
	Capacity      uint64
	InsertedCount uint64
	BitCount      uint64
	HashCount     uint64
	ErrorRateBits uint64
}

type redisBloomReserveOptions struct {
	errorRate  float64
	capacity   uint64
	expansion  uint64
	nonScaling bool
}

func (meta *redisBloomMeta) serialize() []byte {
	buf := make([]byte, redisBloomMetaSize)
	binary.BigEndian.PutUint64(buf[0:8], meta.ID)
	binary.BigEndian.PutUint64(buf[8:16], meta.InitialCapacity)
	binary.BigEndian.PutUint64(buf[16:24], meta.ErrorRateBits)
	binary.BigEndian.PutUint64(buf[24:32], meta.Expansion)
	binary.BigEndian.PutUint64(buf[32:40], meta.Flags)
	binary.BigEndian.PutUint64(buf[40:48], meta.SubFilterCount)
	return buf
}

func (meta *redisBloomMeta) ErrorRate() float64 {
	return math.Float64frombits(meta.ErrorRateBits)
}

func (meta *redisBloomMeta) SetErrorRate(rate float64) {
	meta.ErrorRateBits = math.Float64bits(rate)
}

func (meta *redisBloomMeta) NonScaling() bool {
	return meta.Flags&redisBloomNonScalingFlag != 0
}

func (meta *redisBloomMeta) SetNonScaling(nonScaling bool) {
	if nonScaling {
		meta.Flags |= redisBloomNonScalingFlag
		return
	}
	meta.Flags &^= redisBloomNonScalingFlag
}

func deserializeRedisBloomMeta(buf []byte) (*redisBloomMeta, error) {
	if len(buf) != redisBloomMetaSize {
		return nil, errors.New("corrupted redis bloom metadata")
	}
	return &redisBloomMeta{
		ID:              binary.BigEndian.Uint64(buf[0:8]),
		InitialCapacity: binary.BigEndian.Uint64(buf[8:16]),
		ErrorRateBits:   binary.BigEndian.Uint64(buf[16:24]),
		Expansion:       binary.BigEndian.Uint64(buf[24:32]),
		Flags:           binary.BigEndian.Uint64(buf[32:40]),
		SubFilterCount:  binary.BigEndian.Uint64(buf[40:48]),
		StorageFormat:   redisKeyStorageLegacy,
	}, nil
}

func (meta *redisBloomSubFilterMeta) serialize() []byte {
	buf := make([]byte, redisBloomSubFilterMetaSize)
	binary.BigEndian.PutUint64(buf[0:8], meta.Capacity)
	binary.BigEndian.PutUint64(buf[8:16], meta.InsertedCount)
	binary.BigEndian.PutUint64(buf[16:24], meta.BitCount)
	binary.BigEndian.PutUint64(buf[24:32], meta.HashCount)
	binary.BigEndian.PutUint64(buf[32:40], meta.ErrorRateBits)
	return buf
}

func (meta *redisBloomSubFilterMeta) ErrorRate() float64 {
	return math.Float64frombits(meta.ErrorRateBits)
}

func (meta *redisBloomSubFilterMeta) SetErrorRate(rate float64) {
	meta.ErrorRateBits = math.Float64bits(rate)
}

func deserializeRedisBloomSubFilterMeta(buf []byte) (*redisBloomSubFilterMeta, error) {
	if len(buf) != redisBloomSubFilterMetaSize {
		return nil, errors.New("corrupted redis bloom sub-filter metadata")
	}
	return &redisBloomSubFilterMeta{
		Capacity:      binary.BigEndian.Uint64(buf[0:8]),
		InsertedCount: binary.BigEndian.Uint64(buf[8:16]),
		BitCount:      binary.BigEndian.Uint64(buf[16:24]),
		HashCount:     binary.BigEndian.Uint64(buf[24:32]),
		ErrorRateBits: binary.BigEndian.Uint64(buf[32:40]),
	}, nil
}

func redisBloomDataBucketName(ns redisNamespace, id uint64) []byte {
	name := make([]byte, len(ns.bloomDataPrefix)+8)
	copy(name, ns.bloomDataPrefix)
	binary.BigEndian.PutUint64(name[len(ns.bloomDataPrefix):], id)
	return name
}

func redisBloomSubMetaKey(index uint64) []byte {
	key := make([]byte, 9)
	key[0] = redisBloomSubMetaPrefix
	binary.BigEndian.PutUint64(key[1:], index)
	return key
}

func redisBloomBlockKey(subFilterIndex uint64, blockIndex uint64) []byte {
	key := make([]byte, 17)
	key[0] = redisBloomBlockPrefix
	binary.BigEndian.PutUint64(key[1:9], subFilterIndex)
	binary.BigEndian.PutUint64(key[9:17], blockIndex)
	return key
}

func parseRedisBloomErrorRate(raw []byte) (float64, error) {
	rate, err := strconv.ParseFloat(string(raw), 64)
	if err != nil || math.IsNaN(rate) || math.IsInf(rate, 0) || rate <= 0 || rate >= 1 {
		return 0, errRedisBloomInvalidRate
	}
	return rate, nil
}

func parseRedisBloomReserveArgs(args [][]byte) ([]byte, redisBloomReserveOptions, error) {
	if len(args) < 4 {
		return nil, redisBloomReserveOptions{}, errors.New("wrong number of arguments for 'bf.reserve' command")
	}

	errorRate, err := parseRedisBloomErrorRate(args[2])
	if err != nil {
		return nil, redisBloomReserveOptions{}, err
	}

	capacity, err := strconv.ParseUint(string(args[3]), 10, 64)
	if err != nil || capacity == 0 {
		return nil, redisBloomReserveOptions{}, errRedisBloomInvalidCapacity
	}

	opts := redisBloomReserveOptions{
		errorRate: errorRate,
		capacity:  capacity,
		expansion: redisBloomAutoCreateExpansion,
	}
	expansionSet := false

	for i := 4; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "EXPANSION":
			if i+1 >= len(args) {
				return nil, redisBloomReserveOptions{}, errors.New("syntax error")
			}
			if expansionSet {
				return nil, redisBloomReserveOptions{}, errors.New("EXPANSION was already specified")
			}
			expansion, parseErr := strconv.ParseUint(string(args[i+1]), 10, 64)
			if parseErr != nil || expansion == 0 {
				return nil, redisBloomReserveOptions{}, errRedisBloomInvalidExpansion
			}
			opts.expansion = expansion
			expansionSet = true
			i++
		case "NONSCALING":
			if opts.nonScaling {
				return nil, redisBloomReserveOptions{}, errors.New("NONSCALING was already specified")
			}
			opts.nonScaling = true
		default:
			return nil, redisBloomReserveOptions{}, errors.New("unsupported reserve option")
		}
	}

	return args[1], opts, nil
}

func redisBloomScaledErrorRate(globalRate float64, index uint64, nonScaling bool) float64 {
	if nonScaling {
		return globalRate
	}
	rate := globalRate * (1 - redisBloomScalingTighteningRate) * math.Pow(redisBloomScalingTighteningRate, float64(index))
	if rate <= 0 {
		return math.SmallestNonzeroFloat64
	}
	return rate
}

func redisBloomCalculateBitCount(capacity uint64, errorRate float64) (uint64, error) {
	if capacity == 0 {
		return 0, errRedisBloomInvalidCapacity
	}
	if errorRate <= 0 || errorRate >= 1 || math.IsNaN(errorRate) || math.IsInf(errorRate, 0) {
		return 0, errRedisBloomInvalidRate
	}

	ln2 := math.Log(2)
	bitCount := math.Ceil(-(float64(capacity) * math.Log(errorRate)) / (ln2 * ln2))
	if math.IsNaN(bitCount) || math.IsInf(bitCount, 0) || bitCount <= 0 || bitCount > float64(^uint64(0)) {
		return 0, errors.New("invalid bloom filter size")
	}

	result := uint64(bitCount)
	if result%8 != 0 {
		result += 8 - (result % 8)
	}
	if result == 0 {
		result = 8
	}
	return result, nil
}

func redisBloomCalculateHashCount(errorRate float64) uint64 {
	ln2 := math.Log(2)
	hashCount := math.Ceil(-math.Log(errorRate) / ln2)
	if hashCount < 1 {
		return 1
	}
	return uint64(hashCount)
}

func redisBloomBuildSubFilterMeta(capacity uint64, errorRate float64) (*redisBloomSubFilterMeta, error) {
	bitCount, err := redisBloomCalculateBitCount(capacity, errorRate)
	if err != nil {
		return nil, err
	}
	return &redisBloomSubFilterMeta{
		Capacity:      capacity,
		InsertedCount: 0,
		BitCount:      bitCount,
		HashCount:     redisBloomCalculateHashCount(errorRate),
		ErrorRateBits: math.Float64bits(errorRate),
	}, nil
}

func redisBloomSubFilterBlockCount(bitCount uint64) uint64 {
	if bitCount == 0 {
		return 0
	}
	return (bitCount + redisBloomBlockBits - 1) / redisBloomBlockBits
}

func redisBloomHashPositions(item []byte, bitCount uint64, hashCount uint64) []uint64 {
	h1 := crc64.Checksum(item, redisBloomCRC64Table)
	hasher := fnv.New64a()
	_, _ = hasher.Write(item)
	h2 := hasher.Sum64()
	if h2 == 0 {
		h2 = 1
	}

	positions := make([]uint64, 0, hashCount)
	for i := uint64(0); i < hashCount; i++ {
		positions = append(positions, (h1+(i*h2))%bitCount)
	}
	return positions
}

func loadRedisBloomMetaTx(tx *storage.Tx, ns redisNamespace, key []byte) (*redisBloomMeta, bool, error) {
	bucket, err := tx.GetBucket(ns.bloomMetaBucket)
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
	meta, err := deserializeRedisBloomMeta(raw)
	if err != nil {
		return nil, false, err
	}
	meta.StorageFormat, err = redisObjectStorageFormatTx(tx, ns, key, redisKeyTypeBloom, meta.ID)
	if err != nil {
		return nil, false, err
	}
	return meta, true, nil
}

func saveRedisBloomMetaTx(tx *storage.Tx, ns redisNamespace, key []byte, meta *redisBloomMeta) error {
	bucket, err := tx.CreateBucketIfNotExists(ns.bloomMetaBucket)
	if err != nil {
		return err
	}
	if err = bucket.Put(key, meta.serialize()); err != nil {
		return err
	}
	if err = saveRedisKeyMetaForTypeWithFormatTx(tx, ns, key, redisKeyTypeBloom, meta.ID, meta.StorageFormat); err != nil {
		return err
	}
	return ensureRedisSlotIndexEntryTx(tx, ns, key)
}

func deleteRedisBloomMetaTx(tx *storage.Tx, ns redisNamespace, key []byte) error {
	bucket, err := tx.GetBucket(ns.bloomMetaBucket)
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

func nextRedisBloomIDTx(tx *storage.Tx, ns redisNamespace) (uint64, error) {
	bucket, err := tx.CreateBucketIfNotExists(ns.bloomSysBucket)
	if err != nil {
		return 0, err
	}
	raw, found := bucket.Get(redisBloomNextIDKey)
	if !found {
		initial := make([]byte, 8)
		binary.BigEndian.PutUint64(initial, 2)
		if err = bucket.Put(redisBloomNextIDKey, initial); err != nil {
			return 0, err
		}
		return 1, nil
	}
	if len(raw) != 8 {
		return 0, errors.New("corrupted redis bloom id counter")
	}
	id := binary.BigEndian.Uint64(raw)
	next := make([]byte, 8)
	binary.BigEndian.PutUint64(next, id+1)
	if err = bucket.Put(redisBloomNextIDKey, next); err != nil {
		return 0, err
	}
	return id, nil
}

func ensureRedisBloomDataBucketTx(tx *storage.Tx, ns redisNamespace, meta *redisBloomMeta) (*redisObjectBucket, error) {
	return openRedisObjectBucketTx(tx, ns.bloomDataBucket, redisBloomDataBucketName(ns, meta.ID), meta.ID, meta.StorageFormat, true)
}

func getRedisBloomDataBucketTx(tx *storage.Tx, ns redisNamespace, meta *redisBloomMeta) (*redisObjectBucket, error) {
	return openRedisObjectBucketTx(tx, ns.bloomDataBucket, redisBloomDataBucketName(ns, meta.ID), meta.ID, meta.StorageFormat, false)
}

func saveRedisBloomSubFilterMeta(bucket *redisObjectBucket, index uint64, meta *redisBloomSubFilterMeta) error {
	return bucket.Put(redisBloomSubMetaKey(index), meta.serialize())
}

func loadRedisBloomSubFilterMeta(bucket *redisObjectBucket, index uint64) (*redisBloomSubFilterMeta, bool, error) {
	raw, found := bucket.Get(redisBloomSubMetaKey(index))
	if !found {
		return nil, false, nil
	}
	meta, err := deserializeRedisBloomSubFilterMeta(raw)
	if err != nil {
		return nil, false, err
	}
	return meta, true, nil
}

func loadRedisBloomBlock(bucket *redisObjectBucket, subFilterIndex uint64, blockIndex uint64) ([]byte, error) {
	raw, found := bucket.Get(redisBloomBlockKey(subFilterIndex, blockIndex))
	if !found {
		return make([]byte, redisBloomBlockSize), nil
	}
	if len(raw) != redisBloomBlockSize {
		return nil, errors.New("corrupted redis bloom block")
	}
	return cloneBytes(raw), nil
}

func saveRedisBloomBlock(bucket *redisObjectBucket, subFilterIndex uint64, blockIndex uint64, block []byte) error {
	if len(block) != redisBloomBlockSize {
		return errors.New("corrupted redis bloom block")
	}
	return bucket.Put(redisBloomBlockKey(subFilterIndex, blockIndex), block)
}

func deleteRedisBloomTx(tx *storage.Tx, ns redisNamespace, key []byte, meta *redisBloomMeta) error {
	store, err := getRedisBloomDataBucketTx(tx, ns, meta)
	if err != nil && !errors.Is(err, storage.ErrBucketNotFound) {
		return err
	}
	if err == nil {
		if err = deleteRedisObjectBucketTx(tx, store, redisBloomDataBucketName(ns, meta.ID)); err != nil {
			return err
		}
	}
	if err := deleteRedisBloomMetaTx(tx, ns, key); err != nil {
		return err
	}
	return deleteRedisExpireAtMsTx(tx, ns, key)
}

func deleteRedisBloomIfExistsTx(tx *storage.Tx, ns redisNamespace, key []byte) (bool, error) {
	meta, found, err := loadRedisBloomMetaTx(tx, ns, key)
	if err != nil || !found {
		return false, err
	}
	return true, deleteRedisBloomTx(tx, ns, key, meta)
}

func loadRedisBloomMetaForReadTx(tx *storage.Tx, ns redisNamespace, key []byte, nowMs int64) (*redisBloomMeta, bool, error) {
	meta, found, err := loadRedisBloomMetaTx(tx, ns, key)
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
	if keyType == redisKeyTypeString || keyType == redisKeyTypeList || keyType == redisKeyTypeHash || keyType == redisKeyTypeZSet || keyType == redisKeyTypeTopK {
		return nil, false, errRedisWrongType
	}
	return nil, false, nil
}

func createRedisBloomTx(tx *storage.Tx, ns redisNamespace, key []byte, opts redisBloomReserveOptions, nowMs int64) (*redisBloomMeta, *redisObjectBucket, error) {
	keyType, err := redisKeyTypeTx(tx, ns, key, nowMs)
	if err != nil {
		return nil, nil, err
	}
	switch keyType {
	case redisKeyTypeNone:
	case redisKeyTypeBloom:
		return nil, nil, errRedisBloomItemExists
	case redisKeyTypeTopK:
		return nil, nil, errRedisWrongType
	default:
		return nil, nil, errRedisWrongType
	}

	id, err := nextRedisBloomIDTx(tx, ns)
	if err != nil {
		return nil, nil, err
	}
	meta := &redisBloomMeta{
		ID:              id,
		InitialCapacity: opts.capacity,
		Expansion:       opts.expansion,
		SubFilterCount:  1,
		StorageFormat:   redisKeyStorageShared,
	}
	meta.SetErrorRate(opts.errorRate)
	meta.SetNonScaling(opts.nonScaling)

	dataBucket, err := ensureRedisBloomDataBucketTx(tx, ns, meta)
	if err != nil {
		return nil, nil, err
	}
	subMeta, err := redisBloomBuildSubFilterMeta(opts.capacity, redisBloomScaledErrorRate(opts.errorRate, 0, opts.nonScaling))
	if err != nil {
		return nil, nil, err
	}
	if err = saveRedisBloomSubFilterMeta(dataBucket, 0, subMeta); err != nil {
		return nil, nil, err
	}
	if err = saveRedisBloomMetaTx(tx, ns, key, meta); err != nil {
		return nil, nil, err
	}
	if err = deleteRedisExpireAtMsTx(tx, ns, key); err != nil {
		return nil, nil, err
	}
	return meta, dataBucket, nil
}

func getOrCreateRedisBloomTx(tx *storage.Tx, ns redisNamespace, key []byte, nowMs int64) (*redisBloomMeta, *redisObjectBucket, error) {
	if _, err := purgeExpiredRedisKeyTx(tx, ns, key, nowMs); err != nil {
		return nil, nil, err
	}
	meta, found, err := loadRedisBloomMetaTx(tx, ns, key)
	if err != nil {
		return nil, nil, err
	}
	if found {
		dataBucket, err := getRedisBloomDataBucketTx(tx, ns, meta)
		if err != nil {
			return nil, nil, err
		}
		return meta, dataBucket, nil
	}

	keyType, err := redisKeyTypeTx(tx, ns, key, nowMs)
	if err != nil {
		return nil, nil, err
	}
	if keyType != redisKeyTypeNone {
		return nil, nil, errRedisWrongType
	}
	return createRedisBloomTx(tx, ns, key, redisBloomReserveOptions{
		errorRate: redisBloomAutoCreateErrorRate,
		capacity:  redisBloomAutoCreateCapacity,
		expansion: redisBloomAutoCreateExpansion,
	}, nowMs)
}

func redisBloomEnsureWritableSubFilterTx(tx *storage.Tx, ns redisNamespace, key []byte, meta *redisBloomMeta, dataBucket *redisObjectBucket) (uint64, *redisBloomSubFilterMeta, error) {
	if meta.SubFilterCount == 0 {
		meta.SubFilterCount = 1
		subMeta, err := redisBloomBuildSubFilterMeta(meta.InitialCapacity, redisBloomScaledErrorRate(meta.ErrorRate(), 0, meta.NonScaling()))
		if err != nil {
			return 0, nil, err
		}
		if err = saveRedisBloomSubFilterMeta(dataBucket, 0, subMeta); err != nil {
			return 0, nil, err
		}
		if err = saveRedisBloomMetaTx(tx, ns, key, meta); err != nil {
			return 0, nil, err
		}
		return 0, subMeta, nil
	}

	currentIndex := meta.SubFilterCount - 1
	subMeta, found, err := loadRedisBloomSubFilterMeta(dataBucket, currentIndex)
	if err != nil {
		return 0, nil, err
	}
	if !found {
		return 0, nil, errors.New("corrupted redis bloom filter")
	}
	if subMeta.InsertedCount < subMeta.Capacity {
		return currentIndex, subMeta, nil
	}
	if meta.NonScaling() {
		return 0, nil, errRedisBloomFull
	}

	nextCapacity := subMeta.Capacity * meta.Expansion
	if nextCapacity < subMeta.Capacity || nextCapacity == 0 {
		return 0, nil, errors.New("bloom filter capacity overflow")
	}
	nextIndex := meta.SubFilterCount
	nextMeta, err := redisBloomBuildSubFilterMeta(nextCapacity, redisBloomScaledErrorRate(meta.ErrorRate(), nextIndex, false))
	if err != nil {
		return 0, nil, err
	}
	if err = saveRedisBloomSubFilterMeta(dataBucket, nextIndex, nextMeta); err != nil {
		return 0, nil, err
	}
	meta.SubFilterCount++
	if err = saveRedisBloomMetaTx(tx, ns, key, meta); err != nil {
		return 0, nil, err
	}
	return nextIndex, nextMeta, nil
}

func redisBloomMightContainInSubFilter(dataBucket *redisObjectBucket, subFilterIndex uint64, meta *redisBloomSubFilterMeta, item []byte) (bool, error) {
	positions := redisBloomHashPositions(item, meta.BitCount, meta.HashCount)
	blockCache := make(map[uint64][]byte)

	for _, position := range positions {
		blockIndex := position / redisBloomBlockBits
		block, ok := blockCache[blockIndex]
		if !ok {
			loadedBlock, err := loadRedisBloomBlock(dataBucket, subFilterIndex, blockIndex)
			if err != nil {
				return false, err
			}
			block = loadedBlock
			blockCache[blockIndex] = block
		}

		blockBitOffset := position % redisBloomBlockBits
		byteIndex := blockBitOffset / 8
		mask := byte(1 << (blockBitOffset % 8))
		if block[byteIndex]&mask == 0 {
			return false, nil
		}
	}

	return true, nil
}

func redisBloomInsertIntoSubFilter(dataBucket *redisObjectBucket, subFilterIndex uint64, meta *redisBloomSubFilterMeta, item []byte) (bool, error) {
	positions := redisBloomHashPositions(item, meta.BitCount, meta.HashCount)
	blockCache := make(map[uint64][]byte)
	modifiedBlocks := make(map[uint64]struct{})
	allSet := true

	for _, position := range positions {
		blockIndex := position / redisBloomBlockBits
		block, ok := blockCache[blockIndex]
		if !ok {
			loadedBlock, err := loadRedisBloomBlock(dataBucket, subFilterIndex, blockIndex)
			if err != nil {
				return false, err
			}
			block = loadedBlock
			blockCache[blockIndex] = block
		}

		blockBitOffset := position % redisBloomBlockBits
		byteIndex := blockBitOffset / 8
		mask := byte(1 << (blockBitOffset % 8))
		if block[byteIndex]&mask == 0 {
			allSet = false
			block[byteIndex] |= mask
			modifiedBlocks[blockIndex] = struct{}{}
		}
	}

	if allSet {
		return false, nil
	}
	for blockIndex := range modifiedBlocks {
		if err := saveRedisBloomBlock(dataBucket, subFilterIndex, blockIndex, blockCache[blockIndex]); err != nil {
			return false, err
		}
	}
	return true, nil
}

func redisBloomContainsInFilter(dataBucket *redisObjectBucket, meta *redisBloomMeta, item []byte) (bool, error) {
	for subFilterIndex := meta.SubFilterCount; subFilterIndex > 0; subFilterIndex-- {
		subMeta, found, err := loadRedisBloomSubFilterMeta(dataBucket, subFilterIndex-1)
		if err != nil {
			return false, err
		}
		if !found {
			return false, errors.New("corrupted redis bloom filter")
		}
		exists, err := redisBloomMightContainInSubFilter(dataBucket, subFilterIndex-1, subMeta, item)
		if err != nil {
			return false, err
		}
		if exists {
			return true, nil
		}
	}
	return false, nil
}

func redisBloomReserveTx(tx *storage.Tx, ns redisNamespace, key []byte, opts redisBloomReserveOptions, nowMs int64) error {
	if _, err := purgeExpiredRedisKeyTx(tx, ns, key, nowMs); err != nil {
		return err
	}
	_, _, err := createRedisBloomTx(tx, ns, key, opts, nowMs)
	return err
}

func redisBloomAddTx(tx *storage.Tx, ns redisNamespace, key []byte, item []byte, nowMs int64) (int64, error) {
	meta, dataBucket, err := getOrCreateRedisBloomTx(tx, ns, key, nowMs)
	if err != nil {
		return 0, err
	}
	exists, err := redisBloomContainsInFilter(dataBucket, meta, item)
	if err != nil {
		return 0, err
	}
	if exists {
		return 0, nil
	}

	subFilterIndex, subMeta, err := redisBloomEnsureWritableSubFilterTx(tx, ns, key, meta, dataBucket)
	if err != nil {
		return 0, err
	}
	inserted, err := redisBloomInsertIntoSubFilter(dataBucket, subFilterIndex, subMeta, item)
	if err != nil {
		return 0, err
	}
	if !inserted {
		return 0, nil
	}
	subMeta.InsertedCount++
	if err = saveRedisBloomSubFilterMeta(dataBucket, subFilterIndex, subMeta); err != nil {
		return 0, err
	}
	return 1, nil
}

func redisBloomMAddTx(tx *storage.Tx, ns redisNamespace, key []byte, items [][]byte, nowMs int64) ([]int64, error) {
	meta, dataBucket, err := getOrCreateRedisBloomTx(tx, ns, key, nowMs)
	if err != nil {
		return nil, err
	}

	results := make([]int64, 0, len(items))
	for _, item := range items {
		exists, err := redisBloomContainsInFilter(dataBucket, meta, item)
		if err != nil {
			return nil, err
		}
		if exists {
			results = append(results, 0)
			continue
		}

		subFilterIndex, subMeta, err := redisBloomEnsureWritableSubFilterTx(tx, ns, key, meta, dataBucket)
		if err != nil {
			return nil, err
		}
		inserted, err := redisBloomInsertIntoSubFilter(dataBucket, subFilterIndex, subMeta, item)
		if err != nil {
			return nil, err
		}
		if inserted {
			subMeta.InsertedCount++
			if err = saveRedisBloomSubFilterMeta(dataBucket, subFilterIndex, subMeta); err != nil {
				return nil, err
			}
			results = append(results, 1)
			continue
		}
		results = append(results, 0)
	}

	return results, nil
}

func redisBloomExistsTx(tx *storage.Tx, ns redisNamespace, key []byte, item []byte, nowMs int64) (bool, error) {
	meta, found, err := loadRedisBloomMetaForReadTx(tx, ns, key, nowMs)
	if err != nil || !found {
		return false, err
	}
	dataBucket, err := getRedisBloomDataBucketTx(tx, ns, meta)
	if err != nil {
		return false, err
	}
	return redisBloomContainsInFilter(dataBucket, meta, item)
}

func redisBloomMExistsTx(tx *storage.Tx, ns redisNamespace, key []byte, items [][]byte, nowMs int64) ([]int64, error) {
	meta, found, err := loadRedisBloomMetaForReadTx(tx, ns, key, nowMs)
	if err != nil {
		return nil, err
	}
	if !found {
		results := make([]int64, len(items))
		return results, nil
	}
	dataBucket, err := getRedisBloomDataBucketTx(tx, ns, meta)
	if err != nil {
		return nil, err
	}

	results := make([]int64, 0, len(items))
	for _, item := range items {
		exists, err := redisBloomContainsInFilter(dataBucket, meta, item)
		if err != nil {
			return nil, err
		}
		if exists {
			results = append(results, 1)
		} else {
			results = append(results, 0)
		}
	}
	return results, nil
}
