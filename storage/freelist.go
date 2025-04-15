package storage

import (
	"encoding/binary"
	"fmt"
)

// Freelist first page map
// 0            1                        9                        17                       25                       33                   ...
// +------------+------------------------+------------------------+------------------------+------------------------+----------------------+
// | Page Type  |      Next Page         |    Current Page        |      Max Pages         |  Num Freelist Entries  |   Freelist Entries   |
// |  uint8     |       uint64           |        uint64          |        uint64          |        uint64          |   uint64[]           |
// +------------+------------------------+------------------------+------------------------+------------------------+----------------------+

// Freelist extra page map
// 0            1                        9                   ...
// +------------+------------------------+---------------------+
// | Page Type  |      Next Page         |  Freelist Entries   |
// |  uint8     |       uint64           |     uint64[]        |
// +------------+------------------------+---------------------+

const (
	freelistPageTypeSize    = UInt8Size
	freelistNextPageSize    = UInt64Size
	freelistCurrentPageSize = UInt64Size
	freelistMaxPagesSize    = UInt64Size
	freelistNumPagesSize    = UInt64Size

	freelistPageTypeOffset         = 0
	freelistNextPageOffset         = freelistPageTypeOffset + freelistPageTypeSize
	freelistCurrentPageOffset      = freelistNextPageOffset + freelistNextPageSize
	freelistMaxPagesOffset         = freelistCurrentPageOffset + freelistCurrentPageSize
	freelistNumPagesOffset         = freelistMaxPagesOffset + freelistMaxPagesSize
	freelistFirstPageEntriesOffset = freelistNumPagesOffset + freelistNumPagesSize
	freelistExtraPageEntriesOffset = freelistNextPageOffset + freelistNextPageSize
)

type Freelist struct {
	currentPage         uint64
	maxPages            uint64
	releasedPages       []uint64
	freelistPages       []uint64
	entriesPerFirstPage int
	entriesPerExtraPage int
	dirty               bool
}

func NewFreelist(pageSize uint64, maxPages uint64) *Freelist {
	entriesPerFirstPage, entriesPerExtraPage := calculateFreelistCapacity(int(pageSize))
	return &Freelist{
		currentPage:         rootPageNumber,
		maxPages:            maxPages,
		releasedPages:       make([]uint64, 0),
		freelistPages:       make([]uint64, 0),
		entriesPerFirstPage: entriesPerFirstPage,
		entriesPerExtraPage: entriesPerExtraPage,
		dirty:               false,
	}
}

func (f *Freelist) GetNextPageNumber() (uint64, error) {
	f.dirty = true
	if len(f.releasedPages) > 0 {
		pageNum := f.releasedPages[len(f.releasedPages)-1]
		f.releasedPages = f.releasedPages[:len(f.releasedPages)-1]
		return pageNum, nil
	}
	if f.currentPage >= (f.maxPages - 1) {
		return 0, ErrNoPagesLeft
	}
	f.currentPage += 1
	logger.Debug("freelist GetNextPage", "pageNum", f.currentPage)
	return f.currentPage, nil
}

func (f *Freelist) ReleasePage(pageNum uint64) {
	f.dirty = true
	logger.Debug("releasing pageNum", "pageNumber", pageNum)
	f.releasedPages = append(f.releasedPages, pageNum)
}

func calculatePagesNeeded(numEntries, entriesPerFirstPage, entriesPerExtraPage int) int {
	if numEntries <= entriesPerFirstPage {
		return 1
	}
	return 1 + (numEntries-entriesPerFirstPage+entriesPerExtraPage-1)/entriesPerExtraPage
}

// Reads entries from a freelist pageNum
func readEntriesFromPage(page *Page, startPos int, maxEntries int, entries *[]uint64, remaining *uint64) uint64 {
	pos := startPos
	for i := 0; i < maxEntries && uint64(len(*entries)) < *remaining; i++ {
		if pos+UInt64Size <= len(page.Data) {
			*entries = append(*entries, binary.LittleEndian.Uint64(page.Data[pos:]))
			pos += UInt64Size
		}
	}
	return binary.LittleEndian.Uint64(page.Data[1:]) // Return next pageNum number
}

func writeEntriesToPage(page *Page, startPos int, entries []uint64, startIdx int, maxEntries int) int {
	pos := startPos
	entriesWritten := 0
	for i := 0; i < maxEntries && startIdx+i < len(entries); i++ {
		binary.LittleEndian.PutUint64(page.Data[pos:], entries[startIdx+i])
		pos += UInt64Size
		entriesWritten++
	}
	return entriesWritten
}

func ReadFreelist(dal *Dal) (*Freelist, error) {
	freelist := NewFreelist(dal.meta.pageSize, 0)
	freelist.freelistPages = []uint64{dal.meta.freelistPageNumber}

	// Read the primary freelist pageNum
	firstPage, err := dal.GetPage(dal.meta.freelistPageNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to get freelist pageNum: %w", err)
	}

	// Read metadata, skip next pageNum pointer for now
	freelist.currentPage = binary.LittleEndian.Uint64(firstPage.Data[freelistCurrentPageOffset:])
	freelist.maxPages = binary.LittleEndian.Uint64(firstPage.Data[freelistMaxPagesOffset:])
	numPages := binary.LittleEndian.Uint64(firstPage.Data[freelistNumPagesOffset:])

	freelist.releasedPages = make([]uint64, 0)

	// Read entries from first pageNum and get next pageNum number
	nextPageNum := readEntriesFromPage(
		firstPage,
		freelistFirstPageEntriesOffset,
		freelist.entriesPerFirstPage,
		&freelist.releasedPages,
		&numPages,
	)

	// Read additional pages if needed
	for nextPageNum != 0 && uint64(len(freelist.releasedPages)) < numPages {
		freelist.freelistPages = append(freelist.freelistPages, nextPageNum)

		page, getPageErr := dal.GetPage(nextPageNum)
		if getPageErr != nil {
			return nil, fmt.Errorf("failed to read freelist pageNum %d: %w", nextPageNum, getPageErr)
		}

		nextPageNum = readEntriesFromPage(
			page,
			freelistExtraPageEntriesOffset,
			freelist.entriesPerExtraPage,
			&freelist.releasedPages,
			&numPages,
		)
	}

	logger.Debug("read freelist",
		"currentPage", freelist.currentPage,
		"releasedPages", len(freelist.releasedPages),
		"pagesRead", len(freelist.freelistPages))

	return freelist, nil
}

func WriteFreelist(dal *Dal, freelist *Freelist) error {
	if !freelist.dirty {
		return nil
	}
	pagesNeeded := calculatePagesNeeded(
		len(freelist.releasedPages),
		freelist.entriesPerFirstPage,
		freelist.entriesPerExtraPage,
	)

	// Initialize and ensure first pageNum is the meta.freelistPageNumber
	if len(freelist.freelistPages) == 0 {
		freelist.freelistPages = []uint64{dal.meta.freelistPageNumber}
	} else if freelist.freelistPages[0] != dal.meta.freelistPageNumber {
		freelist.freelistPages[0] = dal.meta.freelistPageNumber
	}

	managePageErr := manageFreelistPageAllocation(dal, freelist, pagesNeeded)
	if managePageErr != nil {
		return managePageErr
	}

	// Write the first pageNum with header
	firstPage, getFirstPageErr := dal.GetPage(dal.meta.freelistPageNumber)
	if getFirstPageErr != nil {
		return fmt.Errorf("failed to get first freelist pageNum: %w", getFirstPageErr)
	}

	// Set next pageNum pointer
	nextPageNum := uint64(0)
	if pagesNeeded > 1 {
		nextPageNum = freelist.freelistPages[1]
	}

	firstPage.Data[freelistPageTypeOffset] = FreeListPage
	binary.LittleEndian.PutUint64(firstPage.Data[freelistNextPageOffset:], nextPageNum)
	binary.LittleEndian.PutUint64(firstPage.Data[freelistCurrentPageOffset:], freelist.currentPage)
	binary.LittleEndian.PutUint64(firstPage.Data[freelistMaxPagesOffset:], freelist.maxPages)
	binary.LittleEndian.PutUint64(firstPage.Data[freelistNumPagesOffset:], uint64(len(freelist.releasedPages)))

	// Write entries to first pageNum
	entriesWritten := writeEntriesToPage(
		firstPage,
		freelistFirstPageEntriesOffset,
		freelist.releasedPages,
		0,
		freelist.entriesPerFirstPage,
	)
	if err := dal.SetPage(firstPage); err != nil {
		return err
	}

	// Write additional pages if needed
	entriesIdx := entriesWritten
	for i := 1; i < pagesNeeded; i++ {
		page, getPageErr := dal.GetPage(freelist.freelistPages[i])
		if getPageErr != nil {
			return fmt.Errorf("failed to get freelist pageNum %d: %w", i, getPageErr)
		}

		// Set next pageNum pointer
		nextPageNum = uint64(0)
		if i < pagesNeeded-1 {
			nextPageNum = freelist.freelistPages[i+1]
		}
		page.Data[freelistPageTypeOffset] = FreeListPage
		binary.LittleEndian.PutUint64(page.Data[freelistNextPageOffset:], nextPageNum)

		// Write entries
		written := writeEntriesToPage(
			page,
			freelistExtraPageEntriesOffset,
			freelist.releasedPages,
			entriesIdx,
			freelist.entriesPerExtraPage,
		)
		entriesIdx += written

		if err := dal.SetPage(page); err != nil {
			return err
		}
	}

	logger.Debug("write freelist",
		"releasedPages", len(freelist.releasedPages),
		"pagesUsed", pagesNeeded)

	freelist.dirty = false
	return nil
}

func calculateFreelistCapacity(pageSize int) (int, int) {
	entriesPerFirstPage := ((pageSize - freelistFirstPageEntriesOffset) / UInt64Size) - 1
	entriesPerExtraPage := ((pageSize - freelistExtraPageEntriesOffset) / UInt64Size) - 1
	return entriesPerFirstPage, entriesPerExtraPage
}

func manageFreelistPageAllocation(dal *Dal, freelist *Freelist, pagesNeeded int) error {
	// Manage pageNum allocations
	oldPageCount := len(freelist.freelistPages)
	if pagesNeeded > oldPageCount {
		// Need more pages - allocate them
		for i := oldPageCount; i < pagesNeeded; i++ {
			newPage, err := dal.AllocatePage()
			if err != nil {
				return fmt.Errorf("failed to allocate freelist pageNum: %w", err)
			}
			freelist.freelistPages = append(freelist.freelistPages, newPage.PageNumber)
		}
	} else if pagesNeeded < oldPageCount {
		// Have excess pages - release them
		for i := pagesNeeded; i < oldPageCount; i++ {
			logger.Debug("freelist drop own pageNum", "pageNum", freelist.freelistPages[i])
			err := dal.ReleasePage(freelist.freelistPages[i])
			if err != nil {
				return err
			}
		}
		freelist.freelistPages = freelist.freelistPages[:pagesNeeded]
	}
	return nil
}
