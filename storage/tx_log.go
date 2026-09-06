package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"time"
)

// Current tx log header map
// 0      4      6      7      8             16            18        22            30             38            46
// +------+------+------+------+-------------+-------------+---------+-------------+--------------+-------------+------+
// |Magic | Ver. | Slot | Res. | Num Pages   | Page Size   |  CRC    | Slot0Offset | Slot0Cap     | Slot1Offset | ...  |
// |[4]b  | u16  | u8   | u8   | uint64      | uint16      | uint32  | uint64      | uint64       | uint64      | ...  |
// +------+------+------+------+-------------+-------------+---------+-------------+--------------+-------------+------+
//
// Trailing bytes:
// - Slot1Cap uint64
// - Padding  uint16
//
// The committed journal lives in one of two reusable slots. On every commit we
// write the new body to the alternate slot and only then sync a new header.
// This keeps the previous committed body intact until the new header is durable.

// Legacy tx log header map
// 0            8                    14
// +------------+------------+--------+
// | Num Pages  | Page Size  |  CRC   |
// |  uint64    |  uint16    | uint32 |
// +------------+------------+--------+

// TxLog page entry map
// 0         8              16             page size + header
// +---------+---------------+--------------------+ ... N pages
// | offset  | Page Number   |      Page data     |
// | uint64  |   uint64      |       uint8[]      |
// +---------+---------------+--------------------+

const (
	txLogMagicValue        = "PTLG"
	txLogVersion    uint16 = 3
	txLogVersionV2  uint16 = 2

	txLogActiveSlotUnset = 0xFF

	txLogMagicSize              = 4
	txLogVersionSize            = UInt16Size
	txLogActiveSlotSize         = UInt8Size
	txLogHeaderReservedSize     = UInt8Size
	txLogHeaderNumPagesSize     = UInt64Size
	txLogHeaderPageSizeSize     = UInt16Size
	txLogHeaderCRCSize          = UInt32Size
	txLogHeaderSlotOffsetSize   = UInt64Size
	txLogHeaderSlotCapacitySize = UInt64Size
	txLogHeaderPaddingSize      = UInt16Size

	txLogMagicOffset             = 0
	txLogVersionOffset           = txLogMagicOffset + txLogMagicSize
	txLogActiveSlotOffset        = txLogVersionOffset + txLogVersionSize
	txLogHeaderReservedOffset    = txLogActiveSlotOffset + txLogActiveSlotSize
	txLogHeaderNumPagesOffset    = txLogHeaderReservedOffset + txLogHeaderReservedSize
	txLogHeaderPageSizeOffset    = txLogHeaderNumPagesOffset + txLogHeaderNumPagesSize
	txLogHeaderCRCOffset         = txLogHeaderPageSizeOffset + txLogHeaderPageSizeSize
	txLogHeaderSlot0OffsetOffset = txLogHeaderCRCOffset + txLogHeaderCRCSize
	txLogHeaderSlot0CapOffset    = txLogHeaderSlot0OffsetOffset + txLogHeaderSlotOffsetSize
	txLogHeaderSlot1OffsetOffset = txLogHeaderSlot0CapOffset + txLogHeaderSlotCapacitySize
	txLogHeaderSlot1CapOffset    = txLogHeaderSlot1OffsetOffset + txLogHeaderSlotOffsetSize
	txLogV2HeaderSize            = txLogHeaderSlot1CapOffset + txLogHeaderSlotCapacitySize + txLogHeaderPaddingSize
	txLogHeaderGenerationOffset  = txLogV2HeaderSize
	txLogHeaderChecksumOffset    = txLogHeaderGenerationOffset + UInt64Size
	txLogHeaderSize              = txLogHeaderChecksumOffset + UInt32Size
	txLogHeaderCopies            = 2
	txLogDataOffset              = txLogHeaderCopies * txLogHeaderSize

	txLogLegacyNumPagesSize = UInt64Size
	txLogLegacyPageSizeSize = UInt16Size
	txLogLegacyCRCSize      = UInt32Size
	txLogLegacyHeaderSize   = txLogLegacyNumPagesSize + txLogLegacyPageSizeSize + txLogLegacyCRCSize
	txLogLegacyNumPages     = 0
	txLogLegacyPageSize     = txLogLegacyNumPages + txLogLegacyNumPagesSize
	txLogLegacyCRC          = txLogLegacyPageSize + txLogLegacyPageSizeSize

	txLogPageOffsetSize = UInt64Size
	txLogPageNumberSize = UInt64Size
	txLogPageHeaderSize = txLogPageOffsetSize + txLogPageNumberSize

	txLogPageOffset = 0
	txLogPageNumber = txLogPageOffset + txLogPageOffsetSize

	txLogReusableBodyLimit = 1 << 20
)

type txLogHeader struct {
	activeSlot     byte
	numPages       uint64
	pageSize       uint16
	crc            uint32
	slotOffsets    [2]uint64
	slotCapacities [2]uint64
	generation     uint64
}

type txLogBufferedPage struct {
	offset     uint64
	pageNumber uint64
	data       []byte
}

type TxLog struct {
	lock              sync.Mutex
	file              *os.File
	numPages          int
	pageSize          int
	crc               hash.Hash32
	table             *crc32.Table
	active            bool
	header            txLogHeader
	headerLoaded      bool
	lastCommittedSlot byte
	bufferedPages     []txLogBufferedPage
	bodyBuffer        []byte
	openErr           error
	beforeWriteHook   func() error
	headerCopy        int
	headerVersion     uint16
	entryHeader       txLogHeader
	entryLastSlot     byte
	entryValid        bool
}

type txLogWriteStats struct {
	writeDuration time.Duration
	syncDuration  time.Duration
	bufferedPages int
	bufferedBytes int
}

type PageRecoveryCallback func(offset uint64, page *Page) error

func newTxLogHeader(pageSize int) txLogHeader {
	return txLogHeader{
		activeSlot: txLogActiveSlotUnset,
		pageSize:   uint16(pageSize),
		slotOffsets: [2]uint64{
			uint64(txLogDataOffset),
			uint64(txLogDataOffset),
		},
	}
}

func (h txLogHeader) isActive() bool {
	return h.activeSlot != txLogActiveSlotUnset && h.numPages > 0
}

func NewTxLog(filename string, mode os.FileMode, pageSize uint64) *TxLog {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, mode)
	if err == nil && (pageSize == 0 || pageSize > uint64(^uint16(0))) {
		err = fmt.Errorf("invalid transaction-log page size %d", pageSize)
		_ = file.Close()
		file = nil
	}
	if err != nil {
		logger.Error("Failed to open log file", "filename", filename, "error", err)
	}
	return &TxLog{
		lock:              sync.Mutex{},
		file:              file,
		pageSize:          int(pageSize),
		table:             crc32.MakeTable(crc32.IEEE),
		header:            newTxLogHeader(int(pageSize)),
		lastCommittedSlot: txLogActiveSlotUnset,
		openErr:           err,
		headerCopy:        -1,
		headerVersion:     txLogVersion,
	}
}

func (txlog *TxLog) Close() error {
	if txlog == nil {
		return nil
	}
	txlog.lock.Lock()
	defer txlog.lock.Unlock()
	if txlog.file == nil {
		return txlog.openErr
	}
	err := txlog.file.Close()
	txlog.file = nil
	return err
}

func (txlog *TxLog) ensureHeaderLoaded() error {
	if txlog.headerLoaded {
		return nil
	}
	if txlog.file == nil {
		return fmt.Errorf("tx log file is nil")
	}

	info, err := txlog.file.Stat()
	if err != nil {
		return err
	}
	if info.Size() == 0 {
		txlog.header = newTxLogHeader(txlog.pageSize)
		txlog.headerLoaded = true
		txlog.lastCommittedSlot = txLogActiveSlotUnset
		return nil
	}

	header, ok, err := txlog.readCurrentHeader()
	if err != nil {
		return err
	}
	if ok {
		txlog.header = header
		txlog.headerLoaded = true
		if header.isActive() {
			txlog.lastCommittedSlot = header.activeSlot
		} else {
			txlog.lastCommittedSlot = txLogActiveSlotUnset
		}
		return nil
	}

	txlog.header = newTxLogHeader(txlog.pageSize)
	txlog.headerLoaded = true
	txlog.lastCommittedSlot = txLogActiveSlotUnset
	return nil
}

func (txlog *TxLog) readCurrentHeader() (txLogHeader, bool, error) {
	if txlog.file == nil {
		return txLogHeader{}, false, fmt.Errorf("tx log file is nil")
	}

	info, err := txlog.file.Stat()
	if err != nil {
		return txLogHeader{}, false, err
	}
	var bestHeader txLogHeader
	bestCopy := -1
	sawV3 := false
	for copyIndex := 0; copyIndex < txLogHeaderCopies; copyIndex++ {
		offset := int64(copyIndex * txLogHeaderSize)
		if info.Size() < offset+txLogHeaderSize {
			continue
		}
		data := make([]byte, txLogHeaderSize)
		if _, readErr := txlog.file.ReadAt(data, offset); readErr != nil {
			continue
		}
		if string(data[txLogMagicOffset:txLogMagicOffset+txLogMagicSize]) != txLogMagicValue || binary.LittleEndian.Uint16(data[txLogVersionOffset:]) != txLogVersion {
			continue
		}
		sawV3 = true
		expectedChecksum := binary.LittleEndian.Uint32(data[txLogHeaderChecksumOffset:])
		actualChecksum := crc32.ChecksumIEEE(data[:txLogHeaderChecksumOffset])
		if expectedChecksum != actualChecksum {
			continue
		}
		header, decodeErr := decodeTxLogHeader(data, true)
		if decodeErr != nil {
			continue
		}
		if bestCopy == -1 || header.generation > bestHeader.generation {
			bestHeader = header
			bestCopy = copyIndex
		}
	}
	if bestCopy >= 0 {
		txlog.headerCopy = bestCopy
		txlog.headerVersion = txLogVersion
		return bestHeader, true, nil
	}
	if sawV3 {
		return txLogHeader{}, false, fmt.Errorf("both transaction-log headers are corrupted")
	}

	if info.Size() < txLogV2HeaderSize {
		return txLogHeader{}, false, nil
	}
	data := make([]byte, txLogV2HeaderSize)
	if _, err = txlog.file.ReadAt(data, 0); err != nil {
		return txLogHeader{}, false, err
	}
	if string(data[txLogMagicOffset:txLogMagicOffset+txLogMagicSize]) != txLogMagicValue {
		return txLogHeader{}, false, nil
	}
	version := binary.LittleEndian.Uint16(data[txLogVersionOffset:])
	if version != txLogVersionV2 {
		return txLogHeader{}, false, fmt.Errorf("unsupported tx log version %d", version)
	}
	header, err := decodeTxLogHeader(data, false)
	if err != nil {
		return txLogHeader{}, false, err
	}
	txlog.headerCopy = -1
	txlog.headerVersion = txLogVersionV2
	return header, true, nil
}

func decodeTxLogHeader(data []byte, withGeneration bool) (txLogHeader, error) {
	if len(data) < txLogV2HeaderSize {
		return txLogHeader{}, fmt.Errorf("transaction-log header is truncated")
	}
	header := txLogHeader{
		activeSlot: data[txLogActiveSlotOffset],
		numPages:   binary.LittleEndian.Uint64(data[txLogHeaderNumPagesOffset:]),
		pageSize:   binary.LittleEndian.Uint16(data[txLogHeaderPageSizeOffset:]),
		crc:        binary.LittleEndian.Uint32(data[txLogHeaderCRCOffset:]),
		slotOffsets: [2]uint64{
			binary.LittleEndian.Uint64(data[txLogHeaderSlot0OffsetOffset:]),
			binary.LittleEndian.Uint64(data[txLogHeaderSlot1OffsetOffset:]),
		},
		slotCapacities: [2]uint64{
			binary.LittleEndian.Uint64(data[txLogHeaderSlot0CapOffset:]),
			binary.LittleEndian.Uint64(data[txLogHeaderSlot1CapOffset:]),
		},
	}
	if withGeneration {
		if len(data) < txLogHeaderSize {
			return txLogHeader{}, fmt.Errorf("transaction-log v3 header is truncated")
		}
		header.generation = binary.LittleEndian.Uint64(data[txLogHeaderGenerationOffset:])
	}
	if header.pageSize == 0 {
		return txLogHeader{}, fmt.Errorf("invalid page size in tx log header: %d", header.pageSize)
	}
	if header.activeSlot != txLogActiveSlotUnset && header.activeSlot > 1 {
		return txLogHeader{}, fmt.Errorf("invalid tx log active slot %d", header.activeSlot)
	}
	if (header.activeSlot == txLogActiveSlotUnset) != (header.numPages == 0) {
		return txLogHeader{}, fmt.Errorf("inconsistent transaction-log active state")
	}
	minimumOffset := uint64(txLogV2HeaderSize)
	if withGeneration {
		minimumOffset = uint64(txLogDataOffset)
	}
	maxFileOffset := uint64(^uint64(0) >> 1)
	for slot := range 2 {
		if header.slotOffsets[slot] < minimumOffset || header.slotOffsets[slot] > maxFileOffset ||
			header.slotCapacities[slot] > maxFileOffset || header.slotOffsets[slot] > maxFileOffset-header.slotCapacities[slot] {
			return txLogHeader{}, fmt.Errorf("invalid transaction-log slot %d bounds", slot)
		}
	}
	if header.slotCapacities[0] > 0 && header.slotCapacities[1] > 0 {
		end0 := header.slotOffsets[0] + header.slotCapacities[0]
		end1 := header.slotOffsets[1] + header.slotCapacities[1]
		if header.slotOffsets[0] < end1 && header.slotOffsets[1] < end0 {
			return txLogHeader{}, fmt.Errorf("transaction-log slots overlap")
		}
	}
	return header, nil
}

func (txlog *TxLog) writeCurrentHeader(header txLogHeader, sync bool) error {
	if txlog.file == nil {
		return fmt.Errorf("tx log file is nil")
	}

	header.generation = txlog.header.generation + 1
	data := marshalTxLogHeader(header)

	writeCopy := 0
	if txlog.headerCopy == 0 {
		writeCopy = 1
	}
	if _, err := txlog.file.WriteAt(data, int64(writeCopy*txLogHeaderSize)); err != nil {
		return err
	}
	if sync {
		if err := txlog.file.Sync(); err != nil {
			return err
		}
	}
	txlog.header = header
	txlog.headerCopy = writeCopy
	txlog.headerVersion = txLogVersion
	return nil
}

func marshalTxLogHeader(header txLogHeader) []byte {
	data := make([]byte, txLogHeaderSize)
	copy(data[txLogMagicOffset:], []byte(txLogMagicValue))
	binary.LittleEndian.PutUint16(data[txLogVersionOffset:], txLogVersion)
	data[txLogActiveSlotOffset] = header.activeSlot
	binary.LittleEndian.PutUint64(data[txLogHeaderNumPagesOffset:], header.numPages)
	binary.LittleEndian.PutUint16(data[txLogHeaderPageSizeOffset:], header.pageSize)
	binary.LittleEndian.PutUint32(data[txLogHeaderCRCOffset:], header.crc)
	binary.LittleEndian.PutUint64(data[txLogHeaderSlot0OffsetOffset:], header.slotOffsets[0])
	binary.LittleEndian.PutUint64(data[txLogHeaderSlot0CapOffset:], header.slotCapacities[0])
	binary.LittleEndian.PutUint64(data[txLogHeaderSlot1OffsetOffset:], header.slotOffsets[1])
	binary.LittleEndian.PutUint64(data[txLogHeaderSlot1CapOffset:], header.slotCapacities[1])
	binary.LittleEndian.PutUint64(data[txLogHeaderGenerationOffset:], header.generation)
	binary.LittleEndian.PutUint32(data[txLogHeaderChecksumOffset:], crc32.ChecksumIEEE(data[:txLogHeaderChecksumOffset]))
	return data
}

func (txlog *TxLog) mirrorCurrentHeader(sync bool) error {
	if txlog.file == nil || txlog.headerCopy < 0 {
		return nil
	}
	data := marshalTxLogHeader(txlog.header)
	mirrorCopy := txlog.headerCopy ^ 1
	if _, err := txlog.file.WriteAt(data, int64(mirrorCopy*txLogHeaderSize)); err != nil {
		return err
	}
	if sync {
		if err := txlog.file.Sync(); err != nil {
			return err
		}
	}
	return nil
}

func (txlog *TxLog) enter() error {
	if err := txlog.ensureHeaderLoaded(); err != nil {
		return err
	}
	if txlog.headerCopy < 0 {
		if err := txlog.writeCurrentHeader(txlog.header, true); err != nil {
			return err
		}
		if err := txlog.mirrorCurrentHeader(false); err != nil {
			return err
		}
	}
	txlog.entryHeader = txlog.header
	txlog.entryLastSlot = txlog.lastCommittedSlot
	txlog.entryValid = true
	txlog.active = true
	txlog.crc = crc32.New(txlog.table)
	txlog.numPages = 0
	txlog.clearBufferedPages()
	return nil
}

func (txlog *TxLog) clearBufferedPages() {
	// Retain the small entry slice for reuse, but do not retain references to
	// every page buffer from the largest transaction ever committed.
	clear(txlog.bufferedPages)
	txlog.bufferedPages = txlog.bufferedPages[:0]
}

func (txlog *TxLog) chooseWriteSlot() int {
	if txlog.lastCommittedSlot == 0 {
		return 1
	}
	return 0
}

func (txlog *TxLog) prepareWriteSlot(slot int, requiredBytes uint64) uint64 {
	if slot < 0 || slot > 1 {
		return uint64(txLogDataOffset)
	}

	offset := txlog.header.slotOffsets[slot]
	if offset < uint64(txLogDataOffset) {
		offset = uint64(txLogDataOffset)
	}
	if requiredBytes == 0 {
		txlog.header.slotOffsets[slot] = offset
		return offset
	}
	if txlog.header.slotCapacities[slot] >= requiredBytes && offset >= uint64(txLogDataOffset) {
		txlog.header.slotOffsets[slot] = offset
		return offset
	}

	otherSlot := slot ^ 1
	newOffset := uint64(txLogDataOffset)
	otherOffset := txlog.header.slotOffsets[otherSlot]
	otherCapacity := txlog.header.slotCapacities[otherSlot]
	if otherOffset >= uint64(txLogDataOffset) && otherCapacity > 0 {
		newOffset = otherOffset + otherCapacity
	}
	txlog.header.slotOffsets[slot] = newOffset
	txlog.header.slotCapacities[slot] = requiredBytes
	return newOffset
}

func (txlog *TxLog) marshalBufferedPages() ([]byte, error) {
	if txlog.numPages == 0 {
		return nil, nil
	}
	if txlog.numPages != len(txlog.bufferedPages) || txlog.pageSize <= 0 {
		return nil, fmt.Errorf("invalid transaction-log buffer state")
	}
	entrySize := txLogPageHeaderSize + txlog.pageSize
	if txlog.numPages > int(^uint(0)>>1)/entrySize {
		return nil, fmt.Errorf("transaction-log body is too large")
	}
	bodySize := txlog.numPages * entrySize
	var body []byte
	if bodySize <= txLogReusableBodyLimit {
		if cap(txlog.bodyBuffer) < bodySize {
			txlog.bodyBuffer = make([]byte, bodySize)
		}
		body = txlog.bodyBuffer[:bodySize]
	} else {
		body = make([]byte, bodySize)
	}
	cursor := 0
	for _, entry := range txlog.bufferedPages {
		binary.LittleEndian.PutUint64(body[cursor:], entry.offset)
		cursor += txLogPageOffsetSize
		binary.LittleEndian.PutUint64(body[cursor:], entry.pageNumber)
		cursor += txLogPageNumberSize
		copy(body[cursor:cursor+txlog.pageSize], entry.data)
		cursor += txlog.pageSize
	}
	return body, nil
}

func (txlog *TxLog) writeBuffered(sync bool) (txLogWriteStats, error) {
	var stats txLogWriteStats
	defer func() {
		txlog.active = false
		txlog.numPages = 0
		txlog.crc = nil
		txlog.clearBufferedPages()
	}()
	if txlog.file == nil {
		return stats, fmt.Errorf("tx log file is nil")
	}
	if txlog.beforeWriteHook != nil {
		if err := txlog.beforeWriteHook(); err != nil {
			return stats, err
		}
	}

	writeStart := time.Now()
	var body []byte
	if txlog.numPages > 0 {
		var err error
		body, err = txlog.marshalBufferedPages()
		if err != nil {
			return stats, err
		}
		writeSlot := txlog.chooseWriteSlot()
		required := uint64(len(body))
		if !txlog.header.isActive() && txlog.header.slotCapacities[writeSlot] < required {
			if err = txlog.compact(); err != nil {
				return stats, err
			}
			if required > ^uint64(0)-uint64(txLogDataOffset) {
				return stats, fmt.Errorf("transaction-log slot size overflows")
			}
			txlog.header.slotOffsets = [2]uint64{uint64(txLogDataOffset), uint64(txLogDataOffset) + required}
			txlog.header.slotCapacities = [2]uint64{required, required}
		}
	}

	nextHeader := txlog.header
	nextHeader.pageSize = uint16(txlog.pageSize)
	nextHeader.numPages = uint64(txlog.numPages)
	nextHeader.crc = 0
	stats.bufferedPages = txlog.numPages

	if txlog.numPages == 0 {
		nextHeader.activeSlot = txLogActiveSlotUnset
		syncStart := time.Now()
		if err := txlog.writeCurrentHeader(nextHeader, sync); err != nil {
			return stats, err
		}
		if mirrorErr := txlog.mirrorCurrentHeader(false); mirrorErr != nil {
			logger.Error("failed to mirror transaction-log header", "error", mirrorErr)
		}
		if sync {
			stats.syncDuration = time.Since(syncStart)
		}
		return stats, nil
	}

	writeSlot := txlog.chooseWriteSlot()
	writeOffset := txlog.prepareWriteSlot(writeSlot, uint64(len(body)))
	nextHeader.slotOffsets = txlog.header.slotOffsets
	nextHeader.slotCapacities = txlog.header.slotCapacities
	if _, err := txlog.file.WriteAt(body, int64(writeOffset)); err != nil {
		return stats, err
	}
	stats.bufferedBytes = len(body)
	stats.writeDuration = time.Since(writeStart)

	nextHeader.activeSlot = byte(writeSlot)
	nextHeader.crc = txlog.crc.Sum32()
	syncStart := time.Now()
	if err := txlog.writeCurrentHeader(nextHeader, sync); err != nil {
		return stats, err
	}
	if mirrorErr := txlog.mirrorCurrentHeader(false); mirrorErr != nil {
		logger.Error("failed to mirror transaction-log header", "error", mirrorErr)
	}
	if sync {
		stats.syncDuration = time.Since(syncStart)
	}

	txlog.lastCommittedSlot = byte(writeSlot)
	return stats, nil
}

func (txlog *TxLog) abort() error {
	defer func() {
		txlog.active = false
		txlog.numPages = 0
		txlog.crc = nil
		txlog.clearBufferedPages()
		txlog.entryValid = false
	}()
	if !txlog.entryValid {
		return txlog.reset(true)
	}
	// A failed header sync can still have placed the attempted generation on
	// disk. Skip that generation so the restored header always wins.
	txlog.header.generation++
	if err := txlog.writeCurrentHeader(txlog.entryHeader, true); err != nil {
		return fmt.Errorf("restore previous transaction-log header: %w", err)
	}
	if err := txlog.mirrorCurrentHeader(false); err != nil {
		return fmt.Errorf("mirror restored transaction-log header: %w", err)
	}
	txlog.lastCommittedSlot = txlog.entryLastSlot
	return nil
}

func (txlog *TxLog) abortError(operationErr error) error {
	if abortErr := txlog.abort(); abortErr != nil {
		return errors.Join(operationErr, fmt.Errorf("%w: %v", ErrJournalRestoreFailed, abortErr))
	}
	return operationErr
}

func (txlog *TxLog) reset(sync bool) error {
	defer func() {
		txlog.active = false
		txlog.numPages = 0
		txlog.crc = nil
		txlog.clearBufferedPages()
	}()
	if err := txlog.ensureHeaderLoaded(); err != nil {
		return err
	}

	nextHeader := txlog.header
	nextHeader.activeSlot = txLogActiveSlotUnset
	nextHeader.numPages = 0
	nextHeader.pageSize = uint16(txlog.pageSize)
	nextHeader.crc = 0
	if err := txlog.writeCurrentHeader(nextHeader, sync); err != nil {
		return err
	}
	if mirrorErr := txlog.mirrorCurrentHeader(false); mirrorErr != nil {
		logger.Error("failed to mirror transaction-log header", "error", mirrorErr)
	}
	return nil
}

func (txlog *TxLog) Clear() error {
	txlog.lock.Lock()
	defer txlog.lock.Unlock()
	if err := txlog.reset(true); err != nil {
		return err
	}
	return txlog.compact()
}

func (txlog *TxLog) compact() error {
	if txlog.file == nil {
		return fmt.Errorf("tx log file is nil")
	}
	info, err := txlog.file.Stat()
	if err != nil {
		return err
	}
	if info.Size() <= int64(txLogDataOffset) {
		txlog.lastCommittedSlot = txLogActiveSlotUnset
		return nil
	}
	if err = txlog.file.Truncate(int64(txLogDataOffset)); err != nil {
		return fmt.Errorf("truncate transaction log: %w", err)
	}
	nextHeader := txlog.header
	nextHeader.activeSlot = txLogActiveSlotUnset
	nextHeader.numPages = 0
	nextHeader.crc = 0
	nextHeader.slotOffsets = [2]uint64{uint64(txLogDataOffset), uint64(txLogDataOffset)}
	nextHeader.slotCapacities = [2]uint64{}
	if err = txlog.writeCurrentHeader(nextHeader, true); err != nil {
		return fmt.Errorf("persist compacted transaction log: %w", err)
	}
	if mirrorErr := txlog.mirrorCurrentHeader(false); mirrorErr != nil {
		logger.Error("failed to mirror compacted transaction-log header", "error", mirrorErr)
	}
	txlog.lastCommittedSlot = txLogActiveSlotUnset
	return nil
}

// ClearFast marks the journal inactive without forcing a durability barrier.
// This is safe only after the main database file has been synced, because a
// crash can leave the previously committed slot visible on disk and recovery
// will replay it idempotently.
func (txlog *TxLog) ClearFast() error {
	txlog.lock.Lock()
	defer txlog.lock.Unlock()
	return txlog.reset(false)
}

func (txlog *TxLog) Sync() error {
	txlog.lock.Lock()
	defer txlog.lock.Unlock()
	if txlog.file == nil {
		return fmt.Errorf("tx log file is nil")
	}
	return txlog.file.Sync()
}

func (txlog *TxLog) Size() (int64, error) {
	txlog.lock.Lock()
	defer txlog.lock.Unlock()
	if txlog.file == nil {
		return 0, fmt.Errorf("tx log file is nil")
	}
	info, err := txlog.file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func (txlog *TxLog) writePage(offset uint64, page *Page) error {
	return txlog.writePageInternal(offset, page, true)
}

func (txlog *TxLog) writePageInternal(offset uint64, page *Page, copyData bool) error {
	if page == nil {
		return fmt.Errorf("cannot journal a nil page")
	}
	if !txlog.active || txlog.crc == nil {
		return fmt.Errorf("transaction log is not active")
	}
	if len(page.Data) != txlog.pageSize {
		return fmt.Errorf("unexpected tx log page size %d, expected %d", len(page.Data), txlog.pageSize)
	}

	entryHeader := make([]byte, txLogPageHeaderSize)
	binary.LittleEndian.PutUint64(entryHeader, offset)
	binary.LittleEndian.PutUint64(entryHeader[txLogPageNumber:], page.PageNumber)

	pageData := page.Data
	if copyData {
		pageData = append([]byte(nil), page.Data...)
	}
	txlog.bufferedPages = append(txlog.bufferedPages, txLogBufferedPage{
		offset:     offset,
		pageNumber: page.PageNumber,
		data:       pageData,
	})

	_, _ = txlog.crc.Write(entryHeader)
	_, _ = txlog.crc.Write(pageData)
	txlog.numPages++
	return nil
}

func (txlog *TxLog) With(fn func() error) error {
	_, err := txlog.withStats(fn)
	return err
}

func (txlog *TxLog) ReplaceWithPages(pages []*Page, sync bool) (txLogWriteStats, error) {
	var stats txLogWriteStats

	txlog.lock.Lock()
	defer txlog.lock.Unlock()

	if err := txlog.enter(); err != nil {
		return stats, err
	}

	defer func() {
		if recoverErr := recover(); recoverErr != nil {
			_ = txlog.abort()
			panic(recoverErr)
		}
	}()

	writeStart := time.Now()
	for _, page := range pages {
		if page == nil || page.PageNumber > ^uint64(0)/uint64(txlog.pageSize) {
			return stats, txlog.abortError(fmt.Errorf("invalid page in transaction log"))
		}
		if err := txlog.writePageInternal(page.PageNumber*uint64(txlog.pageSize), page, false); err != nil {
			return stats, txlog.abortError(err)
		}
	}
	stats.writeDuration = time.Since(writeStart)

	leaveStats, err := txlog.writeBuffered(sync)
	stats.writeDuration += leaveStats.writeDuration
	stats.syncDuration = leaveStats.syncDuration
	stats.bufferedPages = leaveStats.bufferedPages
	stats.bufferedBytes = leaveStats.bufferedBytes
	if err != nil {
		return stats, txlog.abortError(err)
	}

	return stats, nil
}

func (txlog *TxLog) withStats(fn func() error) (txLogWriteStats, error) {
	var stats txLogWriteStats
	if fn == nil {
		return stats, fmt.Errorf("transaction-log callback is nil")
	}
	txlog.lock.Lock()
	defer txlog.lock.Unlock()

	if err := txlog.enter(); err != nil {
		return stats, err
	}

	defer func() {
		if recoverErr := recover(); recoverErr != nil {
			_ = txlog.abort()
			panic(recoverErr)
		}
	}()

	writeStart := time.Now()
	if err := fn(); err != nil {
		return stats, txlog.abortError(err)
	}
	stats.writeDuration = time.Since(writeStart)

	leaveStats, err := txlog.writeBuffered(true)
	stats.writeDuration += leaveStats.writeDuration
	stats.syncDuration = leaveStats.syncDuration
	stats.bufferedPages = leaveStats.bufferedPages
	stats.bufferedBytes = leaveStats.bufferedBytes
	if err != nil {
		return stats, txlog.abortError(err)
	}

	return stats, nil
}

func (txlog *TxLog) recoverFromFile(dataOffset, dataSize int64, numPages, pageSize int, expectedCRC uint32, callback PageRecoveryCallback) error {
	if dataOffset < 0 || dataSize < 0 || numPages < 0 || pageSize <= 0 {
		return fmt.Errorf("invalid transaction-log recovery bounds")
	}
	expectedDataSize := int64(numPages) * int64(txLogPageHeaderSize+pageSize)
	if expectedDataSize != dataSize {
		return fmt.Errorf("tx log size mismatch: expected %d, got %d", expectedDataSize, dataSize)
	}

	crc := crc32.New(txlog.table)
	section := io.NewSectionReader(txlog.file, dataOffset, dataSize)
	buffer := make([]byte, 1024*1024)
	copied, err := io.CopyBuffer(crc, section, buffer)
	if err != nil {
		return fmt.Errorf("read transaction log for CRC: %w", err)
	}
	if copied != dataSize {
		return fmt.Errorf("short transaction-log body: read %d of %d bytes", copied, dataSize)
	}
	if actualCRC := crc.Sum32(); actualCRC != expectedCRC {
		return fmt.Errorf("CRC mismatch: expected %08x, got %08x", expectedCRC, actualCRC)
	}

	entryHeader := make([]byte, txLogPageHeaderSize)
	cursor := dataOffset
	for range numPages {
		if _, err := txlog.file.ReadAt(entryHeader, cursor); err != nil {
			return err
		}
		cursor += txLogPageHeaderSize
		offset := binary.LittleEndian.Uint64(entryHeader[txLogPageOffset : txLogPageOffset+txLogPageOffsetSize])
		pageNum := binary.LittleEndian.Uint64(entryHeader[txLogPageNumber : txLogPageNumber+txLogPageNumberSize])
		if offset%uint64(pageSize) != 0 {
			return fmt.Errorf("tx log offset %d is not aligned to page size %d", offset, pageSize)
		}
		if expectedPageNum := offset / uint64(pageSize); expectedPageNum != pageNum {
			return fmt.Errorf("tx log offset/page mismatch: offset=%d page_num=%d expected_page_num=%d", offset, pageNum, expectedPageNum)
		}
		page := &Page{PageNumber: pageNum, Data: make([]byte, pageSize)}
		if _, err := txlog.file.ReadAt(page.Data, cursor); err != nil {
			return err
		}
		cursor += int64(pageSize)
		if err := callback(offset, page); err != nil {
			return err
		}
	}
	return nil
}

func (txlog *TxLog) recoverCurrent(header txLogHeader, totalSize int64, callback PageRecoveryCallback) error {
	if header.pageSize == 0 {
		return fmt.Errorf("invalid page size in tx log: %d", header.pageSize)
	}
	if !header.isActive() {
		return nil
	}
	if header.activeSlot > 1 {
		return fmt.Errorf("invalid tx log active slot %d", header.activeSlot)
	}

	pageSize := int(header.pageSize)
	if pageSize != txlog.pageSize {
		return fmt.Errorf("tx log page size %d does not match database page size %d", pageSize, txlog.pageSize)
	}
	entrySize := uint64(txLogPageHeaderSize + pageSize)
	if header.numPages > uint64(^uint64(0)>>1)/entrySize {
		return fmt.Errorf("tx log page count %d overflows body size", header.numPages)
	}
	dataSizeU := header.numPages * entrySize
	if dataSizeU > uint64(^uint64(0)>>1) || header.numPages > uint64(^uint(0)>>1) {
		return fmt.Errorf("tx log body is too large")
	}
	numPages := int(header.numPages)
	dataSize := int64(dataSizeU)
	if header.slotOffsets[header.activeSlot] > uint64(^uint64(0)>>1) || header.slotCapacities[header.activeSlot] > uint64(^uint64(0)>>1) {
		return fmt.Errorf("tx log slot metadata overflows file offsets")
	}
	slotOffset := int64(header.slotOffsets[header.activeSlot])
	slotCapacity := int64(header.slotCapacities[header.activeSlot])
	minimumOffset := int64(txLogDataOffset)
	if txlog.headerVersion == txLogVersionV2 {
		minimumOffset = txLogV2HeaderSize
	}
	if slotOffset < minimumOffset {
		return fmt.Errorf("invalid tx log slot offset %d", slotOffset)
	}
	if slotCapacity > 0 && dataSize > slotCapacity {
		return fmt.Errorf("tx log slot capacity mismatch: need %d, have %d", dataSize, slotCapacity)
	}
	if dataSize > totalSize || slotOffset > totalSize-dataSize {
		return fmt.Errorf("tx log body exceeds file size: end=%d size=%d", slotOffset+dataSize, totalSize)
	}
	return txlog.recoverFromFile(slotOffset, dataSize, numPages, pageSize, header.crc, callback)
}

func (txlog *TxLog) recoverLegacy(totalSize int64, callback PageRecoveryCallback) error {
	if totalSize < txLogLegacyHeaderSize {
		return fmt.Errorf("invalid tx log size %d", totalSize)
	}

	header := make([]byte, txLogLegacyHeaderSize)
	_, err := txlog.file.ReadAt(header, 0)
	if err != nil {
		return err
	}

	numPagesU := binary.LittleEndian.Uint64(header[txLogLegacyNumPages:])
	pageSize := int(binary.LittleEndian.Uint16(header[txLogLegacyPageSize:]))
	expectedCRC := binary.LittleEndian.Uint32(header[txLogLegacyCRC:])
	if pageSize <= 0 {
		return fmt.Errorf("invalid page size in tx log: %d", pageSize)
	}
	if numPagesU == 0 {
		return nil
	}
	if numPagesU > uint64(^uint(0)>>1) {
		return fmt.Errorf("legacy tx log page count is too large: %d", numPagesU)
	}
	if numPagesU > uint64(^uint64(0)>>1)/uint64(txLogPageHeaderSize+pageSize) {
		return fmt.Errorf("legacy tx log body size overflows")
	}
	numPages := int(numPagesU)

	dataSize := totalSize - txLogLegacyHeaderSize
	if dataSize <= 0 {
		return fmt.Errorf("tx log has pages in header but no body data")
	}
	expectedSize := int64(numPages) * int64(txLogPageHeaderSize+pageSize)
	if expectedSize != dataSize {
		return fmt.Errorf("legacy tx log size mismatch: expected %d, got %d", expectedSize, dataSize)
	}

	return txlog.recoverFromFile(txLogLegacyHeaderSize, dataSize, numPages, pageSize, expectedCRC, callback)
}

func (txlog *TxLog) Recover(callback PageRecoveryCallback) error {
	txlog.lock.Lock()
	defer txlog.lock.Unlock()
	if txlog.file == nil {
		return fmt.Errorf("tx log file is nil")
	}
	if callback == nil {
		return fmt.Errorf("transaction-log recovery callback is nil")
	}

	info, err := txlog.file.Stat()
	if err != nil {
		return err
	}

	totalSize := info.Size()
	if totalSize == 0 {
		return nil
	}

	header, ok, err := txlog.readCurrentHeader()
	if err != nil {
		return err
	}
	if ok {
		txlog.header = header
		txlog.headerLoaded = true
		if header.isActive() {
			txlog.lastCommittedSlot = header.activeSlot
		} else {
			txlog.lastCommittedSlot = txLogActiveSlotUnset
		}
		return txlog.recoverCurrent(header, totalSize, callback)
	}

	return txlog.recoverLegacy(totalSize, callback)
}

func (txlog *TxLog) HasActiveJournal() (bool, error) {
	txlog.lock.Lock()
	defer txlog.lock.Unlock()
	if txlog.file == nil {
		return false, fmt.Errorf("tx log file is nil")
	}

	info, err := txlog.file.Stat()
	if err != nil {
		return false, err
	}
	if info.Size() == 0 {
		return false, nil
	}

	header, ok, err := txlog.readCurrentHeader()
	if err != nil {
		return false, err
	}
	if ok {
		return header.isActive(), nil
	}

	return true, nil
}
