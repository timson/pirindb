// Package storage
package storage

import (
	"encoding/binary"
)

// Blob - first page
// 0            1            5                     9                        17                  ...
// +------------+------------+---------------------+------------------------+---------------------+
// | Page Type  | Page Count |     Data Size       |      Next Page         |       Data          |
// |   uint8    |   uint32   |      uint32         |       uint64           |     (bytes)         |
// +------------+------------+---------------------+------------------------+---------------------+

// Blob - extra page
// 0            1                        9                   ...
// +------------+------------------------+---------------------+
// | Page Type  |      Next Page         |       Data          |
// |   uint8    |       uint64           |     (bytes)         |
// +------------+------------------------+---------------------+

const (
	blobPageTypeSize    = UInt8Size
	blobTotalPagesSize  = UInt32Size
	blobDataSizeBytes   = UInt32Size
	blobNextPageNumSize = UInt64Size

	blobFirstPageTypeOffset       = 0
	blobFirstPageTotalPagesOffset = blobFirstPageTypeOffset + blobPageTypeSize
	blobFirstPageDataSizeOffset   = blobFirstPageTotalPagesOffset + blobTotalPagesSize
	blobFirstPageNextPageOffset   = blobFirstPageDataSizeOffset + blobDataSizeBytes
	blobFirstPageDataOffset       = blobFirstPageNextPageOffset + blobNextPageNumSize

	blobExtraPageTypeOffset     = 0
	blobExtraPageNextPageOffset = blobExtraPageTypeOffset + blobPageTypeSize

	firstPageHeaderSize = blobTotalPagesSize + blobDataSizeBytes + blobNextPageNumSize + blobPageTypeSize
	pageHeaderSize      = blobNextPageNumSize + blobPageTypeSize
	maxBlobSize         = OneGigabyte
)

// Blob represent data as linked page list
type Blob struct {
	startPageNum uint64
	pageCount    int
	size         int
	data         []byte
}

func NewBlob(data []byte) (*Blob, error) {
	dataLen := len(data)
	if dataLen > maxBlobSize {
		return nil, ErrBlobTooLarge
	}
	return &Blob{data: data, size: dataLen, pageCount: calcPageCount(dataLen)}, nil
}

func GetBlob(tx *Tx, startPageNum uint64) (*Blob, error) {
	startPage, err := tx.getPage(startPageNum)
	if err != nil {
		return nil, err
	}

	if startPage.Data[blobExtraPageTypeOffset] != BlobPage {
		return nil, ErrCorruptedBlob
	}
	pageCount := int(binary.LittleEndian.Uint32(startPage.Data[blobFirstPageTotalPagesOffset:]))
	dataLen := int(binary.LittleEndian.Uint32(startPage.Data[blobFirstPageDataSizeOffset:]))
	if pageCount <= 0 {
		return nil, ErrCorruptedBlob
	}

	blob := Blob{
		startPageNum: startPageNum,
		pageCount:    pageCount,
		size:         dataLen,
		data:         make([]byte, dataLen),
	}

	nextPageNum := binary.LittleEndian.Uint64(startPage.Data[blobFirstPageNextPageOffset:])
	pos := blobFirstPageDataOffset

	dataOffset := 0
	bytesRemaining := dataLen

	for pageIdx := range pageCount {
		var page *Page
		if pageIdx == 0 {
			page = startPage
		} else {
			page, err = tx.getPage(nextPageNum)
			if err != nil {
				return nil, err
			}
			pos = 0
			if page.Data[pos] != BlobPage {
				return nil, ErrCorruptedBlob
			}
			pos++
			nextPageNum = binary.LittleEndian.Uint64(page.Data[pos:])
			pos += blobNextPageNumSize
		}

		pageCapacity := len(page.Data[pos:])
		toCopy := min(bytesRemaining, pageCapacity)

		copy(blob.data[dataOffset:], page.Data[pos:pos+toCopy])

		dataOffset += toCopy
		bytesRemaining -= toCopy
	}

	return &blob, nil
}

func DeleteBlob(tx *Tx, startPageNum uint64) (int, error) {
	page, err := tx.getPage(startPageNum)
	if err != nil {
		return 0, err
	}
	if page.Data[blobFirstPageTypeOffset] != BlobPage {
		return 0, ErrCorruptedBlob
	}
	pageCount := binary.LittleEndian.Uint32(page.Data[blobFirstPageTotalPagesOffset:])
	if pageCount == 0 {
		return 0, ErrCorruptedBlob
	}
	dataLen := int(binary.LittleEndian.Uint32(page.Data[blobFirstPageDataSizeOffset:]))
	pages := make([]uint64, pageCount)
	for pageIndex := 0; pageIndex < int(pageCount); pageIndex++ {
		pages[pageIndex] = page.PageNumber
		if pageIndex == int(pageCount)-1 {
			break
		}

		var nextPageNum uint64
		if pageIndex == 0 {
			nextPageNum = binary.LittleEndian.Uint64(page.Data[blobFirstPageNextPageOffset:])
		} else {
			nextPageNum = binary.LittleEndian.Uint64(page.Data[blobExtraPageNextPageOffset:])
		}
		if nextPageNum == 0 {
			return 0, ErrCorruptedBlob
		}
		page, err = tx.getPage(nextPageNum)
		if err != nil {
			return 0, err
		}
		if page.Data[blobExtraPageTypeOffset] != BlobPage {
			return 0, ErrCorruptedBlob
		}
	}

	for _, pageNum := range pages {
		tx.deletePage(pageNum)
	}

	return dataLen, nil
}

func (blob *Blob) Save(tx *Tx) (uint64, error) {
	dataLen := len(blob.data)

	pages := make([]*Page, blob.pageCount)
	for pageIndex := 0; pageIndex < blob.pageCount; pageIndex++ {
		page, err := tx.allocatePage()
		if err != nil {
			return 0, err
		}
		pages[pageIndex] = page
	}

	dataOffset := 0
	bytesRemaining := dataLen

	for pageIndex, page := range pages {
		var nextPageNum uint64
		if pageIndex < blob.pageCount-1 {
			nextPageNum = pages[pageIndex+1].PageNumber
		}
		page.Data[blobExtraPageTypeOffset] = BlobPage

		pos := blobExtraPageNextPageOffset
		if pageIndex == 0 {
			binary.LittleEndian.PutUint32(page.Data[blobFirstPageTotalPagesOffset:], uint32(blob.pageCount))
			binary.LittleEndian.PutUint32(page.Data[blobFirstPageDataSizeOffset:], uint32(dataLen))
			pos = blobFirstPageNextPageOffset
		}

		binary.LittleEndian.PutUint64(page.Data[pos:], nextPageNum)
		pos += blobNextPageNumSize

		capacity := len(page.Data[pos:])
		toCopy := min(bytesRemaining, capacity)

		copy(page.Data[pos:], blob.data[dataOffset:dataOffset+toCopy])

		dataOffset += toCopy
		bytesRemaining -= toCopy

		tx.setPage(page)
	}

	return pages[0].PageNumber, nil
}

func calcPageCount(dataSize int) int {
	firstPageDataCap := BTreePageSize - firstPageHeaderSize
	otherPageDataCap := BTreePageSize - pageHeaderSize

	if dataSize <= firstPageDataCap {
		return 1
	}
	dataSize -= firstPageDataCap

	pages := dataSize / otherPageDataCap
	if dataSize%otherPageDataCap != 0 {
		pages++
	}

	return 1 + pages
}
