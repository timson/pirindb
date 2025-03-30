package storage

import (
	"encoding/binary"
	"fmt"
)

// Meta page map
// 0            1            8            10                       18                       26                       34
// +------------+------------+------------+------------------------+------------------------+------------------------+
// | Page Type  | DB Name    | DB Version |       Root Page        |     Freelist Page      |      Page Size         |
// |  uint8     |  7 bytes   |  uint16    |        uint64          |        uint64          |        uint64          |
// +------------+------------+------------+------------------------+------------------------+------------------------+

const (
	metaPageNumber     = 0
	freelistPageNumber = 1
	rootPageNumber     = 2
	dbName             = "pirindb"
	dbVersionMinor     = 1
	dbVersionMajor     = 0

	metaPageSize               = UInt8Size
	metaDbNameSize             = len(dbName)
	metaDbVersionSize          = UInt16Size
	metaRootPageNumberSize     = UInt64Size
	metaFreelistPageNumberSize = UInt64Size

	metaPageTypeOffset           = 0
	metaDbNameOffset             = metaPageTypeOffset + metaPageSize
	metaDbVersionOffset          = metaDbNameOffset + metaDbNameSize
	metaRootPageNumberOffset     = metaDbVersionOffset + metaDbVersionSize
	metaFreelistPageNumberOffset = metaRootPageNumberOffset + metaRootPageNumberSize
	metaPageSizeOffset           = metaFreelistPageNumberOffset + metaFreelistPageNumberSize
)

type Meta struct {
	dbName             string
	dbVersion          uint16
	root               uint64
	freelistPageNumber uint64
	pageSize           uint64
}

func NewMeta(pageSize uint64) *Meta {
	return &Meta{
		dbName:             dbName,
		dbVersion:          uint16(dbVersionMajor)<<8 | uint16(dbVersionMinor),
		root:               rootPageNumber,
		freelistPageNumber: freelistPageNumber,
		pageSize:           pageSize,
	}
}

func (m *Meta) GetDbName() string {
	return m.dbName
}

func (m *Meta) GetDbVersion() (major byte, minor byte) {
	return byte(m.dbVersion >> 8), byte(m.dbVersion & 0xff)
}

func (m *Meta) GetDbVersionString() string {
	major, minor := m.GetDbVersion()
	return fmt.Sprintf("%d.%d", major, minor)
}

func (m *Meta) Serialize(data []byte) {
	data[metaPageTypeOffset] = MetaPage
	copy(data[metaDbNameOffset:], m.dbName)
	binary.LittleEndian.PutUint16(data[metaDbVersionOffset:], m.dbVersion)
	binary.LittleEndian.PutUint64(data[metaRootPageNumberOffset:], m.root)
	binary.LittleEndian.PutUint64(data[metaFreelistPageNumberOffset:], m.freelistPageNumber)
	binary.LittleEndian.PutUint64(data[metaPageSizeOffset:], m.pageSize)
}

func (m *Meta) Deserialize(data []byte) {
	pos := 0
	if data[pos] != MetaPage {
		logger.Warn("page type is not a meta page", "type", data[pos])
	}
	m.dbName = string(data[metaDbNameOffset : metaDbNameOffset+metaDbNameSize])
	m.dbVersion = binary.LittleEndian.Uint16(data[metaDbVersionOffset:])
	m.root = binary.LittleEndian.Uint64(data[metaRootPageNumberOffset:])
	m.freelistPageNumber = binary.LittleEndian.Uint64(data[metaFreelistPageNumberOffset:])
	m.pageSize = binary.LittleEndian.Uint64(data[metaPageSizeOffset:])
}

func WriteMeta(dal *Dal, m *Meta) error {
	page, err := dal.GetPage(0)
	if err != nil {
		return fmt.Errorf("failed to get pageNum 0: %w", err)
	}
	page.PageNumber = metaPageNumber
	m.Serialize(page.Data)
	logger.Debug("write meta pageNum", "rootPage", m.root)
	return dal.SetPage(page)
}

func ReadMeta(dal *Dal) (*Meta, error) {
	page, err := dal.GetPage(0)
	if err != nil {
		return nil, fmt.Errorf("failed to get pageNum 0: %w", err)
	}
	m := NewMeta(0)
	m.Deserialize(page.Data)
	if m.dbName != dbName {
		return nil, ErrBadDbName
	}
	if m.dbVersion>>8 != uint16(dbVersionMajor) {
		return nil, ErrBadDbVersion
	}
	logger.Debug("read meta pageNum", "dbName", m.dbName, "version", m.GetDbVersionString(), "rootPage", m.root)
	return m, nil
}
