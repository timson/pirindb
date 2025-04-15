package storage

import (
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc32"
	"os"
	"sync"
)

// TxLog header map
// 0            8                    14
// +------------+------------+--------+
// | Num Pages  | Page Size  |  CRC   |
// |  uint8     |  uint16    | uint32 |
// +------------+------------+--------+

// TxLog pages map
// 0         8              16             page size + header
// +---------+---------------+--------------------+ ... N pages
// | offset  | Page Number   |      Page data     |
// | uint64  |   uint64      |       uint8[]      |
// +---------+---------------+--------------------+

const (
	txLogNumPagesSize   = UInt64Size
	txLogPageSizeBytes  = UInt16Size
	txLogCRCSize        = UInt32Size
	txLogHeaderSize     = txLogNumPagesSize + txLogPageSizeBytes + txLogCRCSize
	txLogPageOffsetSize = UInt64Size
	txLogPageNumberSize = UInt64Size
	txLogPageHeaderSize = txLogPageOffsetSize + txLogPageNumberSize

	txLogNumPages = 0
	txLogPageSize = txLogNumPages + txLogNumPagesSize
	txLogCRC      = txLogPageSize + txLogPageSizeBytes

	txLogPageOffset = 0
	txLogPageNumber = txLogPageOffset + txLogPageOffsetSize
)

type TxLog struct {
	lock     sync.Mutex
	file     *os.File
	numPages int
	pageSize int
	crc      hash.Hash32
	table    *crc32.Table
	active   bool
}

type PageRecoveryCallback func(offset uint64, page *Page) error

func NewTxLog(filename string, mode os.FileMode) *TxLog {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, mode)
	if err != nil {
		logger.Error("Failed to open log file", "filename", filename, "error", err)
	}
	return &TxLog{
		lock:     sync.Mutex{},
		file:     file,
		pageSize: BTreePageSize,
		table:    crc32.MakeTable(crc32.IEEE),
	}
}

func (txlog *TxLog) enter() {
	err := txlog.file.Truncate(0)
	_, err = txlog.file.Seek(txLogHeaderSize, 0)
	txlog.active = true
	txlog.crc = crc32.New(txlog.table)
	txlog.numPages = 0
	if err != nil {
		return
	}
}

func (txlog *TxLog) leave() error {
	defer func() {
		txlog.active = false
	}()
	_, err := txlog.file.Seek(0, 0)
	header := make([]byte, txLogHeaderSize)
	binary.LittleEndian.PutUint64(header[txLogNumPages:], uint64(txlog.numPages))
	binary.LittleEndian.PutUint16(header[txLogPageSize:], uint16(txlog.pageSize))
	binary.LittleEndian.PutUint32(header[txLogCRC:], txlog.crc.Sum32())
	_, err = txlog.file.Write(header)
	if err != nil {
		return err
	}
	err = txlog.file.Sync()
	if err != nil {
		return err
	}
	return nil
}

func (txlog *TxLog) writePage(offset uint64, page *Page) error {
	data := make([]byte, txLogPageHeaderSize)
	binary.LittleEndian.PutUint64(data, offset)
	binary.LittleEndian.PutUint64(data[txLogPageNumber:], page.PageNumber)
	_, err := txlog.file.Write(data)
	if err != nil {
		return err
	}
	_, _ = txlog.crc.Write(data)
	_, err = txlog.file.Write(page.Data)
	if err != nil {
		return err
	}
	_, _ = txlog.crc.Write(page.Data)
	txlog.numPages++
	return nil
}

func (txlog *TxLog) With(fn func() error) error {
	txlog.lock.Lock()
	defer txlog.lock.Unlock()

	txlog.enter()
	defer txlog.leave()

	return fn()
}

func (txlog *TxLog) Recover(callback PageRecoveryCallback) error {
	txlog.lock.Lock()
	defer txlog.lock.Unlock()

	info, err := txlog.file.Stat()
	if err != nil {
		return err
	}

	totalSize := info.Size()
	if totalSize == 0 {
		return nil
	}

	// Read header
	header := make([]byte, 14)
	_, err = txlog.file.ReadAt(header, 0)
	if err != nil {
		return err
	}

	numPages := int(binary.LittleEndian.Uint64(header[txLogNumPages:]))
	pageSize := int(binary.LittleEndian.Uint16(header[txLogPageSize:]))
	expectedCRC := binary.LittleEndian.Uint32(header[txLogCRC:])

	// Read rest of the file

	dataSize := totalSize - txLogHeaderSize
	if dataSize <= 0 {
		return nil
	}
	data := make([]byte, dataSize)
	_, err = txlog.file.ReadAt(data, txLogHeaderSize)
	if err != nil {
		return err
	}

	// Validate CRC
	crc := crc32.New(txlog.table)
	_, _ = crc.Write(data)
	actualCRC := crc.Sum32()
	if actualCRC != expectedCRC {
		return fmt.Errorf("CRC mismatch: expected %08x, got %08x", expectedCRC, actualCRC)
	}

	// Process each (offset, page)
	cursor := 0
	for i := 0; i < numPages; i++ {
		offset := binary.LittleEndian.Uint64(data[cursor : cursor+txLogPageOffsetSize])
		cursor += txLogPageOffsetSize
		pageNum := binary.LittleEndian.Uint64(data[cursor : cursor+txLogPageNumberSize])
		cursor += txLogPageNumberSize

		page := &Page{
			PageNumber: pageNum, // optional
			Data:       make([]byte, pageSize),
		}
		copy(page.Data, data[cursor:cursor+pageSize])
		cursor += pageSize

		if err = callback(offset, page); err != nil {
			return err
		}
	}

	return nil
}
