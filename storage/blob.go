// Package storage
package storage

import (
	"encoding/binary"
	"io"
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
	if !tx.write {
		if blob, ok := tx.readBlobs[startPageNum]; ok {
			return blob, nil
		}
	}

	startPage, err := tx.getPage(startPageNum)
	if err != nil {
		return nil, err
	}

	if startPage.Data[blobExtraPageTypeOffset] != BlobPage {
		return nil, ErrCorruptedBlob
	}
	pageCount, dataLen, err := decodeBlobHeader(startPage)
	if err != nil {
		return nil, err
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

	if !tx.write {
		if _, exists := tx.readBlobs[startPageNum]; exists || len(tx.readBlobs) < readBlobCacheLimit {
			tx.readBlobs[startPageNum] = &blob
		}
	}
	return &blob, nil
}

func BlobSize(tx *Tx, startPageNum uint64) (int, error) {
	page, err := tx.getPage(startPageNum)
	if err != nil {
		return 0, err
	}
	_, dataLen, err := decodeBlobHeader(page)
	return dataLen, err
}

func WriteBlobTo(tx *Tx, startPageNum uint64, w io.Writer) (int64, error) {
	startPage, err := tx.getPage(startPageNum)
	if err != nil {
		return 0, err
	}

	pageCount, dataLen, err := decodeBlobHeader(startPage)
	if err != nil {
		return 0, err
	}

	var written int64
	nextPageNum := binary.LittleEndian.Uint64(startPage.Data[blobFirstPageNextPageOffset:])
	pos := blobFirstPageDataOffset
	bytesRemaining := dataLen

	for pageIdx := range pageCount {
		var page *Page
		if pageIdx == 0 {
			page = startPage
		} else {
			page, err = tx.getPage(nextPageNum)
			if err != nil {
				return written, err
			}
			pos = 0
			if page.Data[pos] != BlobPage {
				return written, ErrCorruptedBlob
			}
			pos++
			nextPageNum = binary.LittleEndian.Uint64(page.Data[pos:])
			pos += blobNextPageNumSize
		}

		pageCapacity := len(page.Data[pos:])
		toWrite := min(bytesRemaining, pageCapacity)
		if toWrite > 0 {
			n, writeErr := w.Write(page.Data[pos : pos+toWrite])
			written += int64(n)
			if writeErr != nil {
				return written, writeErr
			}
			if n != toWrite {
				return written, io.ErrShortWrite
			}
			bytesRemaining -= toWrite
		}
	}

	if bytesRemaining != 0 {
		return written, ErrCorruptedBlob
	}
	return written, nil
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
	pageCount := calcPageCountForPageSize(dataLen, int(tx.db.dal.meta.pageSize))
	if pageCount <= 0 {
		return 0, ErrCorruptedBlob
	}
	blob.pageCount = pageCount
	blob.size = dataLen
	pageNums, err := tx.allocatePageNumbers(pageCount)
	if err != nil {
		return 0, err
	}
	startPageNum := pageNums[0]

	dataOffset := 0
	bytesRemaining := dataLen

	for pageIndex := 0; pageIndex < pageCount; pageIndex++ {
		var nextPageNum uint64
		if pageIndex < pageCount-1 {
			nextPageNum = pageNums[pageIndex+1]
		}
		page := &Page{
			PageNumber: pageNums[pageIndex],
			Data:       make([]byte, tx.db.dal.meta.pageSize),
		}
		page.Data[blobExtraPageTypeOffset] = BlobPage

		pos := blobExtraPageNextPageOffset
		if pageIndex == 0 {
			binary.LittleEndian.PutUint32(page.Data[blobFirstPageTotalPagesOffset:], uint32(pageCount))
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

	return startPageNum, nil
}

func SaveBlobFromReader(tx *Tx, r io.Reader, dataLen int64) (uint64, error) {
	if dataLen < 0 {
		return 0, ErrCorruptedBlob
	}
	if dataLen > maxBlobSize {
		return 0, ErrBlobTooLarge
	}

	pageCount := calcPageCountForPageSize(int(dataLen), int(tx.db.dal.meta.pageSize))
	if pageCount <= 0 {
		return 0, ErrCorruptedBlob
	}
	pageNums, err := tx.allocatePageNumbers(pageCount)
	if err != nil {
		return 0, err
	}
	startPageNum := pageNums[0]

	bytesRemaining := int(dataLen)
	for pageIndex := 0; pageIndex < pageCount; pageIndex++ {
		var nextPageNum uint64
		if pageIndex < pageCount-1 {
			nextPageNum = pageNums[pageIndex+1]
		}
		page := &Page{
			PageNumber: pageNums[pageIndex],
			Data:       make([]byte, tx.db.dal.meta.pageSize),
		}
		page.Data[blobExtraPageTypeOffset] = BlobPage

		pos := blobExtraPageNextPageOffset
		if pageIndex == 0 {
			binary.LittleEndian.PutUint32(page.Data[blobFirstPageTotalPagesOffset:], uint32(pageCount))
			binary.LittleEndian.PutUint32(page.Data[blobFirstPageDataSizeOffset:], uint32(dataLen))
			pos = blobFirstPageNextPageOffset
		}

		binary.LittleEndian.PutUint64(page.Data[pos:], nextPageNum)
		pos += blobNextPageNumSize

		capacity := len(page.Data[pos:])
		toRead := min(bytesRemaining, capacity)
		if toRead > 0 {
			if _, err = io.ReadFull(r, page.Data[pos:pos+toRead]); err != nil {
				return 0, err
			}
			bytesRemaining -= toRead
		}

		tx.setPage(page)
	}

	if bytesRemaining != 0 {
		return 0, ErrCorruptedBlob
	}
	return startPageNum, nil
}

func decodeBlobHeader(startPage *Page) (int, int, error) {
	if startPage.Data[blobExtraPageTypeOffset] != BlobPage {
		return 0, 0, ErrCorruptedBlob
	}
	pageCount := int(binary.LittleEndian.Uint32(startPage.Data[blobFirstPageTotalPagesOffset:]))
	dataLen := int(binary.LittleEndian.Uint32(startPage.Data[blobFirstPageDataSizeOffset:]))
	if pageCount <= 0 {
		return 0, 0, ErrCorruptedBlob
	}
	return pageCount, dataLen, nil
}

func calcPageCountForPageSize(dataSize int, pageSize int) int {
	firstPageDataCap := pageSize - firstPageHeaderSize
	otherPageDataCap := pageSize - pageHeaderSize
	if firstPageDataCap <= 0 || otherPageDataCap <= 0 {
		return 0
	}

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

func calcPageCount(dataSize int) int {
	return calcPageCountForPageSize(dataSize, BTreePageSize)
}
