// Package storage
package storage

import (
	"encoding/binary"
	"fmt"
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
	maxInMemoryBlobSize = 64 * 1024 * 1024
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
	if dataLen >= maxBlobSize {
		return nil, ErrBlobTooLarge
	}
	return &Blob{data: data, size: dataLen, pageCount: calcPageCount(dataLen)}, nil
}

func GetBlob(tx *Tx, startPageNum uint64) (*Blob, error) {
	if err := tx.ensureOpen(); err != nil {
		return nil, err
	}
	if err := tx.db.dal.validateLiveDataPage(startPageNum); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCorruptedBlob, err)
	}
	if !tx.write {
		if blob, ok := tx.readBlobs[startPageNum]; ok {
			return blob, nil
		}
	}

	startPage, err := tx.getPage(startPageNum)
	if err != nil {
		return nil, err
	}

	pageCount, dataLen, err := decodeBlobHeaderForSize(startPage, tx.db.dal.usablePageSize())
	if err != nil {
		return nil, err
	}
	if dataLen > maxInMemoryBlobSize {
		return nil, ErrBlobRequiresStreaming
	}

	blob := Blob{
		startPageNum: startPageNum,
		pageCount:    pageCount,
		size:         dataLen,
		data:         make([]byte, dataLen),
	}

	dataOffset := 0
	_, _, err = visitBlobPages(tx, startPageNum, startPage, func(_ uint64, chunk []byte) error {
		copy(blob.data[dataOffset:], chunk)
		dataOffset += len(chunk)
		return nil
	})
	if err != nil {
		return nil, err
	}

	if !tx.write {
		cacheLimit := tx.db.dal.opts.MaxTransactionBytes
		if _, exists := tx.readBlobs[startPageNum]; exists ||
			(len(tx.readBlobs) < readBlobCacheLimit && int64(dataLen) <= cacheLimit-tx.readBlobBytes) {
			if tx.readBlobs == nil {
				tx.readBlobs = make(map[uint64]*Blob)
			}
			tx.readBlobs[startPageNum] = &blob
			tx.readBlobBytes += int64(dataLen)
		}
	}
	return &blob, nil
}

func BlobSize(tx *Tx, startPageNum uint64) (int, error) {
	if err := tx.ensureOpen(); err != nil {
		return 0, err
	}
	if err := tx.db.dal.validateLiveDataPage(startPageNum); err != nil {
		return 0, fmt.Errorf("%w: %v", ErrCorruptedBlob, err)
	}
	page, err := tx.getPage(startPageNum)
	if err != nil {
		return 0, err
	}
	_, dataLen, err := decodeBlobHeaderForSize(page, tx.db.dal.usablePageSize())
	return dataLen, err
}

func WriteBlobTo(tx *Tx, startPageNum uint64, w io.Writer) (int64, error) {
	if err := tx.ensureOpen(); err != nil {
		return 0, err
	}
	if w == nil {
		return 0, fmt.Errorf("blob writer is nil")
	}
	if err := tx.db.dal.validateLiveDataPage(startPageNum); err != nil {
		return 0, fmt.Errorf("%w: %v", ErrCorruptedBlob, err)
	}
	startPage, err := tx.getPage(startPageNum)
	if err != nil {
		return 0, err
	}

	_, _, err = decodeBlobHeaderForSize(startPage, tx.db.dal.usablePageSize())
	if err != nil {
		return 0, err
	}

	var written int64
	_, _, err = visitBlobPages(tx, startPageNum, startPage, func(_ uint64, chunk []byte) error {
		if len(chunk) > 0 {
			n, writeErr := w.Write(chunk)
			written += int64(n)
			if writeErr != nil {
				return writeErr
			}
			if n != len(chunk) {
				return io.ErrShortWrite
			}
		}
		return nil
	})
	if err != nil {
		return written, err
	}
	return written, nil
}

func DeleteBlob(tx *Tx, startPageNum uint64) (int, error) {
	if err := tx.ensureWritable(); err != nil {
		return 0, err
	}
	if err := tx.db.dal.validateLiveDataPage(startPageNum); err != nil {
		return 0, fmt.Errorf("%w: %v", ErrCorruptedBlob, err)
	}
	page, err := tx.getPage(startPageNum)
	if err != nil {
		return 0, err
	}
	_, dataLen, err := decodeBlobHeaderForSize(page, tx.db.dal.usablePageSize())
	if err != nil {
		return 0, err
	}
	pages := make([]uint64, 0, calcPageCountForPageSize(dataLen, tx.db.dal.usablePageSize()))
	_, _, err = visitBlobPages(tx, startPageNum, page, func(pageNum uint64, _ []byte) error {
		pages = append(pages, pageNum)
		return nil
	})
	if err != nil {
		return 0, err
	}

	for _, pageNum := range pages {
		tx.deletePage(pageNum)
	}

	return dataLen, nil
}

func (blob *Blob) Save(tx *Tx) (_ uint64, returnErr error) {
	if err := tx.ensureWritable(); err != nil {
		return 0, err
	}
	if blob == nil {
		return 0, ErrCorruptedBlob
	}
	dataLen := len(blob.data)
	if dataLen >= maxBlobSize {
		return 0, ErrBlobTooLarge
	}
	pageCount := calcPageCountForPageSize(dataLen, tx.db.dal.usablePageSize())
	if pageCount <= 0 {
		return 0, ErrCorruptedBlob
	}
	if err := tx.ensureAdditionalDirtyPages(pageCount); err != nil {
		return 0, err
	}
	blob.pageCount = pageCount
	blob.size = dataLen
	pageNums, err := tx.allocatePageNumbers(pageCount)
	if err != nil {
		return 0, err
	}
	defer func() {
		if returnErr != nil {
			tx.recordError(returnErr)
		}
	}()
	startPageNum := pageNums[0]
	blob.startPageNum = startPageNum

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

		capacity := tx.db.dal.usablePageSize() - pos
		toCopy := min(bytesRemaining, capacity)

		copy(page.Data[pos:], blob.data[dataOffset:dataOffset+toCopy])

		dataOffset += toCopy
		bytesRemaining -= toCopy

		if err = tx.setPage(page); err != nil {
			return 0, err
		}
	}

	return startPageNum, nil
}

func SaveBlobFromReader(tx *Tx, r io.Reader, dataLen int64) (_ uint64, returnErr error) {
	if err := tx.ensureWritable(); err != nil {
		return 0, err
	}
	if dataLen < 0 {
		return 0, ErrCorruptedBlob
	}
	if dataLen >= maxBlobSize {
		return 0, ErrBlobTooLarge
	}
	if r == nil {
		return 0, fmt.Errorf("blob reader is nil")
	}

	pageCount := calcPageCountForPageSize(int(dataLen), tx.db.dal.usablePageSize())
	if pageCount <= 0 {
		return 0, ErrCorruptedBlob
	}
	if err := tx.ensureAdditionalDirtyPages(pageCount); err != nil {
		return 0, err
	}
	pageNums, err := tx.allocatePageNumbers(pageCount)
	if err != nil {
		return 0, err
	}
	defer func() {
		if returnErr != nil {
			tx.recordError(returnErr)
		}
	}()
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

		capacity := tx.db.dal.usablePageSize() - pos
		toRead := min(bytesRemaining, capacity)
		if toRead > 0 {
			if _, err = io.ReadFull(r, page.Data[pos:pos+toRead]); err != nil {
				return 0, err
			}
			bytesRemaining -= toRead
		}

		if err = tx.setPage(page); err != nil {
			return 0, err
		}
	}

	if bytesRemaining != 0 {
		return 0, ErrCorruptedBlob
	}
	return startPageNum, nil
}

func decodeBlobHeader(startPage *Page) (int, int, error) {
	if startPage == nil {
		return 0, 0, fmt.Errorf("%w: blob page is nil", ErrCorruptedBlob)
	}
	return decodeBlobHeaderForSize(startPage, len(startPage.Data)-pageChecksumSize)
}

func decodeBlobHeaderForSize(startPage *Page, usablePageSize int) (int, int, error) {
	if startPage == nil || len(startPage.Data) < firstPageHeaderSize {
		return 0, 0, fmt.Errorf("%w: blob header is truncated", ErrCorruptedBlob)
	}
	if startPage.Data[blobExtraPageTypeOffset] != BlobPage {
		return 0, 0, fmt.Errorf("%w: invalid first page type %d", ErrCorruptedBlob, startPage.Data[blobExtraPageTypeOffset])
	}
	pageCount := int(binary.LittleEndian.Uint32(startPage.Data[blobFirstPageTotalPagesOffset:]))
	dataLen := int(binary.LittleEndian.Uint32(startPage.Data[blobFirstPageDataSizeOffset:]))
	if pageCount <= 0 || dataLen < 0 || dataLen >= maxBlobSize {
		return 0, 0, fmt.Errorf("%w: invalid page count %d or data length %d", ErrCorruptedBlob, pageCount, dataLen)
	}
	if usablePageSize > len(startPage.Data) || usablePageSize < firstPageHeaderSize {
		return 0, 0, fmt.Errorf("%w: invalid usable page size %d", ErrCorruptedBlob, usablePageSize)
	}
	expectedPageCount := calcPageCountForPageSize(dataLen, usablePageSize)
	if expectedPageCount <= 0 || pageCount != expectedPageCount {
		return 0, 0, fmt.Errorf("%w: page count %d does not match data length %d (expected %d)", ErrCorruptedBlob, pageCount, dataLen, expectedPageCount)
	}
	return pageCount, dataLen, nil
}

func visitBlobPages(tx *Tx, startPageNum uint64, startPage *Page, visitor func(pageNum uint64, chunk []byte) error) (int, int, error) {
	if err := tx.db.dal.validateLiveDataPage(startPageNum); err != nil {
		return 0, 0, fmt.Errorf("%w: %v", ErrCorruptedBlob, err)
	}
	usablePageSize := tx.db.dal.usablePageSize()
	pageCount, dataLen, err := decodeBlobHeaderForSize(startPage, usablePageSize)
	if err != nil {
		return 0, 0, err
	}
	visited := make(map[uint64]struct{}, pageCount)
	currentPageNum := startPageNum
	page := startPage
	bytesRemaining := dataLen

	for pageIndex := 0; pageIndex < pageCount; pageIndex++ {
		if page == nil || len(page.Data) != int(tx.db.dal.meta.pageSize) {
			return 0, 0, fmt.Errorf("%w: page %d has an invalid size", ErrCorruptedBlob, currentPageNum)
		}
		if _, exists := visited[currentPageNum]; exists {
			return 0, 0, fmt.Errorf("%w: blob cycle at page %d", ErrCorruptedBlob, currentPageNum)
		}
		visited[currentPageNum] = struct{}{}
		if page.Data[blobExtraPageTypeOffset] != BlobPage {
			return 0, 0, fmt.Errorf("%w: page %d has type %d", ErrCorruptedBlob, currentPageNum, page.Data[blobExtraPageTypeOffset])
		}

		dataOffset := blobFirstPageDataOffset
		nextOffset := blobFirstPageNextPageOffset
		if pageIndex > 0 {
			dataOffset = pageHeaderSize
			nextOffset = blobExtraPageNextPageOffset
		}
		if dataOffset > usablePageSize || nextOffset > usablePageSize-UInt64Size {
			return 0, 0, fmt.Errorf("%w: page %d header is truncated", ErrCorruptedBlob, currentPageNum)
		}
		nextPageNum := binary.LittleEndian.Uint64(page.Data[nextOffset:])
		chunkLen := min(bytesRemaining, usablePageSize-dataOffset)
		if chunkLen < 0 {
			return 0, 0, fmt.Errorf("%w: invalid data capacity on page %d", ErrCorruptedBlob, currentPageNum)
		}
		if visitor != nil {
			if visitErr := visitor(currentPageNum, page.Data[dataOffset:dataOffset+chunkLen]); visitErr != nil {
				return 0, 0, visitErr
			}
		}
		bytesRemaining -= chunkLen

		if pageIndex == pageCount-1 {
			if nextPageNum != 0 || bytesRemaining != 0 {
				return 0, 0, fmt.Errorf("%w: invalid terminal page %d (next=%d remaining=%d)", ErrCorruptedBlob, currentPageNum, nextPageNum, bytesRemaining)
			}
			break
		}
		if liveErr := tx.db.dal.validateLiveDataPage(nextPageNum); liveErr != nil {
			return 0, 0, fmt.Errorf("%w: next page %d is not live: %v", ErrCorruptedBlob, nextPageNum, liveErr)
		}
		currentPageNum = nextPageNum
		page, err = tx.getPage(currentPageNum)
		if err != nil {
			return 0, 0, err
		}
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
	return calcPageCountForPageSize(dataSize, BTreePageSize-pageChecksumSize)
}
