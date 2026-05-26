package storage

import (
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc32"
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
	txLogVersion    uint16 = 2

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
	txLogHeaderSize              = txLogHeaderSlot1CapOffset + txLogHeaderSlotCapacitySize + txLogHeaderPaddingSize

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
)

type txLogHeader struct {
	activeSlot     byte
	numPages       uint64
	pageSize       uint16
	crc            uint32
	slotOffsets    [2]uint64
	slotCapacities [2]uint64
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
			uint64(txLogHeaderSize),
			uint64(txLogHeaderSize),
		},
	}
}

func (h txLogHeader) isActive() bool {
	return h.activeSlot != txLogActiveSlotUnset && h.numPages > 0
}

func NewTxLog(filename string, mode os.FileMode, pageSize uint64) *TxLog {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, mode)
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
	}
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
	if info.Size() < txLogHeaderSize {
		return txLogHeader{}, false, nil
	}

	data := make([]byte, txLogHeaderSize)
	_, err = txlog.file.ReadAt(data, 0)
	if err != nil {
		return txLogHeader{}, false, err
	}

	if string(data[txLogMagicOffset:txLogMagicOffset+txLogMagicSize]) != txLogMagicValue {
		return txLogHeader{}, false, nil
	}
	version := binary.LittleEndian.Uint16(data[txLogVersionOffset:])
	if version != txLogVersion {
		return txLogHeader{}, false, fmt.Errorf("unsupported tx log version %d", version)
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
	if header.pageSize == 0 {
		return txLogHeader{}, false, fmt.Errorf("invalid page size in tx log header: %d", header.pageSize)
	}
	if header.activeSlot != txLogActiveSlotUnset && header.activeSlot > 1 {
		return txLogHeader{}, false, fmt.Errorf("invalid tx log active slot %d", header.activeSlot)
	}
	return header, true, nil
}

func (txlog *TxLog) writeCurrentHeader(header txLogHeader, sync bool) error {
	if txlog.file == nil {
		return fmt.Errorf("tx log file is nil")
	}

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

	if _, err := txlog.file.WriteAt(data, 0); err != nil {
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
	txlog.active = true
	txlog.crc = crc32.New(txlog.table)
	txlog.numPages = 0
	txlog.bufferedPages = txlog.bufferedPages[:0]
	return nil
}

func (txlog *TxLog) chooseWriteSlot() int {
	if txlog.lastCommittedSlot == 0 {
		return 1
	}
	return 0
}

func (txlog *TxLog) prepareWriteSlot(slot int, requiredBytes uint64) uint64 {
	if slot < 0 || slot > 1 {
		return uint64(txLogHeaderSize)
	}

	offset := txlog.header.slotOffsets[slot]
	if offset < uint64(txLogHeaderSize) {
		offset = uint64(txLogHeaderSize)
	}
	if requiredBytes == 0 {
		txlog.header.slotOffsets[slot] = offset
		return offset
	}
	if txlog.header.slotCapacities[slot] >= requiredBytes && offset >= uint64(txLogHeaderSize) {
		txlog.header.slotOffsets[slot] = offset
		return offset
	}

	otherSlot := slot ^ 1
	newOffset := uint64(txLogHeaderSize)
	otherOffset := txlog.header.slotOffsets[otherSlot]
	otherCapacity := txlog.header.slotCapacities[otherSlot]
	if otherOffset >= uint64(txLogHeaderSize) && otherCapacity > 0 {
		newOffset = otherOffset + otherCapacity
	}
	txlog.header.slotOffsets[slot] = newOffset
	txlog.header.slotCapacities[slot] = requiredBytes
	return newOffset
}

func (txlog *TxLog) marshalBufferedPages() []byte {
	if txlog.numPages == 0 {
		return nil
	}
	bodySize := txlog.numPages * (txLogPageHeaderSize + txlog.pageSize)
	body := make([]byte, bodySize)
	cursor := 0
	for _, entry := range txlog.bufferedPages {
		binary.LittleEndian.PutUint64(body[cursor:], entry.offset)
		cursor += txLogPageOffsetSize
		binary.LittleEndian.PutUint64(body[cursor:], entry.pageNumber)
		cursor += txLogPageNumberSize
		copy(body[cursor:cursor+txlog.pageSize], entry.data)
		cursor += txlog.pageSize
	}
	return body
}

func (txlog *TxLog) writeBuffered(sync bool) (txLogWriteStats, error) {
	var stats txLogWriteStats
	defer func() {
		txlog.active = false
		txlog.numPages = 0
		txlog.crc = nil
		txlog.bufferedPages = txlog.bufferedPages[:0]
	}()
	if txlog.file == nil {
		return stats, fmt.Errorf("tx log file is nil")
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
		if sync {
			stats.syncDuration = time.Since(syncStart)
		}
		txlog.header = nextHeader
		return stats, nil
	}

	writeStart := time.Now()
	body := txlog.marshalBufferedPages()
	writeSlot := txlog.chooseWriteSlot()
	writeOffset := txlog.prepareWriteSlot(writeSlot, uint64(len(body)))
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
	if sync {
		stats.syncDuration = time.Since(syncStart)
	}

	txlog.header = nextHeader
	txlog.lastCommittedSlot = byte(writeSlot)
	return stats, nil
}

func (txlog *TxLog) abort() error {
	return txlog.reset(true)
}

func (txlog *TxLog) reset(sync bool) error {
	defer func() {
		txlog.active = false
		txlog.numPages = 0
		txlog.crc = nil
		txlog.bufferedPages = txlog.bufferedPages[:0]
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
	txlog.header = nextHeader
	return nil
}

func (txlog *TxLog) Clear() error {
	txlog.lock.Lock()
	defer txlog.lock.Unlock()
	return txlog.reset(true)
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

func (txlog *TxLog) writePage(offset uint64, page *Page) error {
	if len(page.Data) != txlog.pageSize {
		return fmt.Errorf("unexpected tx log page size %d, expected %d", len(page.Data), txlog.pageSize)
	}

	entryHeader := make([]byte, txLogPageHeaderSize)
	binary.LittleEndian.PutUint64(entryHeader, offset)
	binary.LittleEndian.PutUint64(entryHeader[txLogPageNumber:], page.PageNumber)

	pageCopy := append([]byte(nil), page.Data...)
	txlog.bufferedPages = append(txlog.bufferedPages, txLogBufferedPage{
		offset:     offset,
		pageNumber: page.PageNumber,
		data:       pageCopy,
	})

	_, _ = txlog.crc.Write(entryHeader)
	_, _ = txlog.crc.Write(pageCopy)
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
		if err := txlog.writePage(page.PageNumber*uint64(txlog.pageSize), page); err != nil {
			_ = txlog.abort()
			return stats, err
		}
	}
	stats.writeDuration = time.Since(writeStart)

	leaveStats, err := txlog.writeBuffered(sync)
	stats.writeDuration += leaveStats.writeDuration
	stats.syncDuration = leaveStats.syncDuration
	stats.bufferedPages = leaveStats.bufferedPages
	stats.bufferedBytes = leaveStats.bufferedBytes
	if err != nil {
		_ = txlog.abort()
		return stats, err
	}

	return stats, nil
}

func (txlog *TxLog) withStats(fn func() error) (txLogWriteStats, error) {
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
	if err := fn(); err != nil {
		_ = txlog.abort()
		return stats, err
	}
	stats.writeDuration = time.Since(writeStart)

	leaveStats, err := txlog.writeBuffered(true)
	stats.writeDuration += leaveStats.writeDuration
	stats.syncDuration = leaveStats.syncDuration
	stats.bufferedPages = leaveStats.bufferedPages
	stats.bufferedBytes = leaveStats.bufferedBytes
	if err != nil {
		_ = txlog.abort()
		return stats, err
	}

	return stats, nil
}

func (txlog *TxLog) recoverFromBuffer(data []byte, numPages, pageSize int, expectedCRC uint32, callback PageRecoveryCallback) error {
	crc := crc32.New(txlog.table)
	_, _ = crc.Write(data)
	actualCRC := crc.Sum32()
	if actualCRC != expectedCRC {
		return fmt.Errorf("CRC mismatch: expected %08x, got %08x", expectedCRC, actualCRC)
	}

	expectedDataSize := int64(numPages) * int64(txLogPageHeaderSize+pageSize)
	if expectedDataSize != int64(len(data)) {
		return fmt.Errorf("tx log size mismatch: expected %d, got %d", expectedDataSize, len(data))
	}

	cursor := 0
	for range numPages {
		if cursor+txLogPageHeaderSize+pageSize > len(data) {
			return fmt.Errorf("tx log entry overflow at cursor %d", cursor)
		}
		offset := binary.LittleEndian.Uint64(data[cursor : cursor+txLogPageOffsetSize])
		cursor += txLogPageOffsetSize
		pageNum := binary.LittleEndian.Uint64(data[cursor : cursor+txLogPageNumberSize])
		cursor += txLogPageNumberSize
		if offset%uint64(pageSize) != 0 {
			return fmt.Errorf("tx log offset %d is not aligned to page size %d", offset, pageSize)
		}
		expectedPageNum := offset / uint64(pageSize)
		if expectedPageNum != pageNum {
			return fmt.Errorf(
				"tx log offset/page mismatch: offset=%d page_num=%d expected_page_num=%d",
				offset, pageNum, expectedPageNum,
			)
		}

		page := &Page{
			PageNumber: pageNum,
			Data:       make([]byte, pageSize),
		}
		copy(page.Data, data[cursor:cursor+pageSize])
		cursor += pageSize

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
	numPages := int(header.numPages)
	dataSize := int64(numPages) * int64(txLogPageHeaderSize+pageSize)
	slotOffset := int64(header.slotOffsets[header.activeSlot])
	slotCapacity := int64(header.slotCapacities[header.activeSlot])
	if slotOffset < txLogHeaderSize {
		return fmt.Errorf("invalid tx log slot offset %d", slotOffset)
	}
	if slotCapacity > 0 && dataSize > slotCapacity {
		return fmt.Errorf("tx log slot capacity mismatch: need %d, have %d", dataSize, slotCapacity)
	}
	if slotOffset+dataSize > totalSize {
		return fmt.Errorf("tx log body exceeds file size: end=%d size=%d", slotOffset+dataSize, totalSize)
	}
	if dataSize > int64(^uint(0)>>1) {
		return fmt.Errorf("tx log data too large: %d", dataSize)
	}

	data := make([]byte, int(dataSize))
	_, err := txlog.file.ReadAt(data, slotOffset)
	if err != nil {
		return err
	}
	return txlog.recoverFromBuffer(data, numPages, pageSize, header.crc, callback)
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

	numPages := int(binary.LittleEndian.Uint64(header[txLogLegacyNumPages:]))
	pageSize := int(binary.LittleEndian.Uint16(header[txLogLegacyPageSize:]))
	expectedCRC := binary.LittleEndian.Uint32(header[txLogLegacyCRC:])
	if pageSize <= 0 {
		return fmt.Errorf("invalid page size in tx log: %d", pageSize)
	}
	if numPages == 0 {
		return nil
	}

	dataSize := totalSize - txLogLegacyHeaderSize
	if dataSize <= 0 {
		return fmt.Errorf("tx log has pages in header but no body data")
	}
	if dataSize > int64(^uint(0)>>1) {
		return fmt.Errorf("tx log data too large: %d", dataSize)
	}

	data := make([]byte, int(dataSize))
	_, err = txlog.file.ReadAt(data, txLogLegacyHeaderSize)
	if err != nil {
		return err
	}
	return txlog.recoverFromBuffer(data, numPages, pageSize, expectedCRC, callback)
}

func (txlog *TxLog) Recover(callback PageRecoveryCallback) error {
	txlog.lock.Lock()
	defer txlog.lock.Unlock()
	if txlog.file == nil {
		return fmt.Errorf("tx log file is nil")
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
