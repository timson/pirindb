package storage

import (
	"encoding/binary"
	"fmt"
	"os"
)

// Meta page map
// 0            1            8            10                       18                       26                       34
// +------------+------------+------------+------------------------+------------------------+------------------------+
// | Page Type  | DB Name    | DB Version |       Root Page        |     Freelist Page      |      Page Size         |
// |  uint8     |  7 bytes   |  uint16    |        uint64          |        uint64          |        uint64          |
// +------------+------------+------------+------------------------+------------------------+------------------------+

const (
	metaPageNumber       = 0
	freelistPageNumber   = 1
	legacyRootPageNumber = 2
	metaMirrorPageNumber = 2 // Reserved by the checksummed (v0.3+) format.
	rootPageNumber       = 3
	dbName               = "pirindb"
	dbVersionMinor       = 3
	dbVersionMajor       = 0

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
	metaHeaderSize               = metaPageSizeOffset + UInt64Size
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

func (m *Meta) Serialize(data []byte) error {
	if len(data) < metaHeaderSize {
		return fmt.Errorf("%w: metadata page has %d bytes, need at least %d", ErrCorruptedMeta, len(data), metaHeaderSize)
	}
	data[metaPageTypeOffset] = MetaPage
	copy(data[metaDbNameOffset:], m.dbName)
	binary.LittleEndian.PutUint16(data[metaDbVersionOffset:], m.dbVersion)
	binary.LittleEndian.PutUint64(data[metaRootPageNumberOffset:], m.root)
	binary.LittleEndian.PutUint64(data[metaFreelistPageNumberOffset:], m.freelistPageNumber)
	binary.LittleEndian.PutUint64(data[metaPageSizeOffset:], m.pageSize)
	return nil
}

func (m *Meta) Deserialize(data []byte) error {
	if len(data) < metaHeaderSize {
		return fmt.Errorf("%w: metadata page has %d bytes, need at least %d", ErrCorruptedMeta, len(data), metaHeaderSize)
	}
	if data[metaPageTypeOffset] != MetaPage {
		return fmt.Errorf("%w: invalid page type %d", ErrCorruptedMeta, data[metaPageTypeOffset])
	}
	m.dbName = string(data[metaDbNameOffset : metaDbNameOffset+metaDbNameSize])
	m.dbVersion = binary.LittleEndian.Uint16(data[metaDbVersionOffset:])
	m.root = binary.LittleEndian.Uint64(data[metaRootPageNumberOffset:])
	m.freelistPageNumber = binary.LittleEndian.Uint64(data[metaFreelistPageNumberOffset:])
	m.pageSize = binary.LittleEndian.Uint64(data[metaPageSizeOffset:])
	if m.dbName != dbName {
		return ErrBadDbName
	}
	if m.dbVersion>>8 != uint16(dbVersionMajor) {
		return ErrBadDbVersion
	}
	if m.dbVersion&0xff > uint16(dbVersionMinor) {
		return ErrBadDbVersion
	}
	if err := validatePageSize(m.pageSize); err != nil {
		return fmt.Errorf("%w: %v", ErrCorruptedMeta, err)
	}
	minimumRoot := uint64(legacyRootPageNumber)
	if m.usesPageChecksums() {
		minimumRoot = rootPageNumber
	}
	if m.root < minimumRoot {
		return fmt.Errorf("%w: invalid root page %d", ErrCorruptedMeta, m.root)
	}
	if m.freelistPageNumber != freelistPageNumber || m.freelistPageNumber == m.root {
		return fmt.Errorf("%w: invalid freelist page %d", ErrCorruptedMeta, m.freelistPageNumber)
	}
	return nil
}

func (m *Meta) usesPageChecksums() bool {
	return m != nil && byte(m.dbVersion&0xff) >= 3
}

func BuildMetaPage(pageSize uint64, m *Meta) *Page {
	return BuildMetaPageAt(metaPageNumber, pageSize, m)
}

func BuildMetaPageAt(pageNum uint64, pageSize uint64, m *Meta) *Page {
	page, _ := buildMetaPageAt(pageNum, pageSize, m)
	return page
}

func buildMetaPageAt(pageNum uint64, pageSize uint64, m *Meta) (*Page, error) {
	if m == nil {
		return nil, fmt.Errorf("%w: metadata is nil", ErrCorruptedMeta)
	}
	page := &Page{
		PageNumber: pageNum,
		Data:       make([]byte, pageSize),
	}
	if err := m.Serialize(page.Data); err != nil {
		return nil, err
	}
	return page, nil
}

func WriteMeta(dal *Dal, m *Meta) error {
	page, err := buildMetaPageAt(metaPageNumber, dal.meta.pageSize, m)
	if err != nil {
		return err
	}
	logger.Debug("write meta pageNum", "rootPage", m.root)
	if err := dal.SetPage(page); err != nil {
		return err
	}
	if dal.pageChecksums {
		mirror, buildErr := buildMetaPageAt(metaMirrorPageNumber, dal.meta.pageSize, m)
		if buildErr != nil {
			return buildErr
		}
		return dal.SetPage(mirror)
	}
	return nil
}

func ReadMeta(dal *Dal) (*Meta, error) {
	primary, primaryErr := readMetaPage(dal, metaPageNumber)
	if !dal.pageChecksums {
		if primaryErr != nil {
			return nil, primaryErr
		}
		return primary, nil
	}
	mirror, mirrorErr := readMetaPage(dal, metaMirrorPageNumber)
	if primaryErr != nil && mirrorErr != nil {
		return nil, fmt.Errorf("%w: both metadata copies are invalid: primary=%v mirror=%v", ErrCorruptedMeta, primaryErr, mirrorErr)
	}
	if primaryErr == nil && mirrorErr == nil {
		if *primary != *mirror {
			return nil, fmt.Errorf("%w: metadata copies disagree", ErrCorruptedMeta)
		}
		return primary, nil
	}
	dal.metaNeedsRepair = true
	if primaryErr == nil {
		dal.metaRepairPage = metaMirrorPageNumber
		return primary, nil
	}
	dal.metaRepairPage = metaPageNumber
	return mirror, nil
}

func readMetaPage(dal *Dal, pageNum uint64) (*Meta, error) {
	page, err := dal.GetPage(pageNum)
	if err != nil {
		return nil, fmt.Errorf("read metadata page %d: %w", pageNum, err)
	}
	m := NewMeta(0)
	if err = m.Deserialize(page.Data); err != nil {
		return nil, err
	}
	logger.Debug("read meta pageNum", "dbName", m.dbName, "version", m.GetDbVersionString(), "rootPage", m.root)
	return m, nil
}

func ReadMetaHeader(file *os.File) (*Meta, error) {
	if file == nil {
		return nil, fmt.Errorf("%w: database file is nil", ErrCorruptedMeta)
	}
	var legacyCandidate *Meta
	data := make([]byte, metaHeaderSize)
	if _, err := file.ReadAt(data, 0); err == nil {
		m := NewMeta(0)
		if deserializeErr := m.Deserialize(data); deserializeErr == nil {
			if !m.usesPageChecksums() {
				// Keep looking for a valid mirrored v0.3 header first. A damaged
				// v0.3 version byte can otherwise make the primary look legacy.
				if fullPage := readMetaPageAt(file, metaPageNumber, m.pageSize); fullPage != nil && metadataTailIsZero(fullPage) {
					legacyCandidate = m
				}
			} else if fullPage := readMetaPageAt(file, 0, m.pageSize); fullPage != nil && verifyPageData(fullPage, true) == nil {
				return m, nil
			}
		}
	}
	for pageSize := uint64(MinPageSize); pageSize <= MaxPageSize; pageSize *= 2 {
		fullPage := readMetaPageAt(file, metaMirrorPageNumber, pageSize)
		if fullPage == nil {
			continue
		}
		if verifyPageData(fullPage, true) != nil {
			continue
		}
		m := NewMeta(0)
		if err := m.Deserialize(fullPage); err == nil && m.usesPageChecksums() && m.pageSize == pageSize {
			return m, nil
		}
	}
	if legacyCandidate != nil {
		return legacyCandidate, nil
	}
	return nil, fmt.Errorf("%w: no valid metadata copy", ErrCorruptedMeta)
}

func metadataTailIsZero(data []byte) bool {
	if len(data) < metaHeaderSize {
		return false
	}
	for _, value := range data[metaHeaderSize:] {
		if value != 0 {
			return false
		}
	}
	return true
}

func readMetaPageAt(file *os.File, pageNum, pageSize uint64) []byte {
	if pageSize == 0 || pageNum > uint64(^uint64(0)>>1)/pageSize {
		return nil
	}
	data := make([]byte, pageSize)
	if _, err := file.ReadAt(data, int64(pageNum*pageSize)); err != nil {
		return nil
	}
	return data
}
