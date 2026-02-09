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
	releasedPagesSet    map[uint64]struct{}
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
		releasedPagesSet:    make(map[uint64]struct{}),
		freelistPages:       make([]uint64, 0),
		entriesPerFirstPage: entriesPerFirstPage,
		entriesPerExtraPage: entriesPerExtraPage,
		dirty:               false,
	}
}

func (f *Freelist) availablePageN() int {
	if f.maxPages == 0 || f.currentPage >= f.maxPages-1 {
		return len(f.releasedPages)
	}
	return int((f.maxPages-1)-f.currentPage) + len(f.releasedPages)
}

func (f *Freelist) ensureReleasedPagesSet() {
	if f.releasedPagesSet != nil {
		return
	}
	f.rebuildReleasedPagesSet()
}

func (f *Freelist) rebuildReleasedPagesSet() {
	f.releasedPagesSet = make(map[uint64]struct{}, len(f.releasedPages))
	for _, pageNum := range f.releasedPages {
		f.releasedPagesSet[pageNum] = struct{}{}
	}
}

func (f *Freelist) isFreelistStoragePage(pageNum uint64) bool {
	for _, freelistPageNum := range f.freelistPages {
		if pageNum == freelistPageNum {
			return true
		}
	}
	return false
}

func (f *Freelist) popReleasedPage() (uint64, bool) {
	if len(f.releasedPages) == 0 {
		return 0, false
	}
	f.ensureReleasedPagesSet()
	lastIdx := len(f.releasedPages) - 1
	pageNum := f.releasedPages[lastIdx]
	f.releasedPages = f.releasedPages[:lastIdx]
	delete(f.releasedPagesSet, pageNum)
	return pageNum, true
}

func (f *Freelist) addReleasedPage(pageNum uint64) bool {
	f.ensureReleasedPagesSet()
	if _, exists := f.releasedPagesSet[pageNum]; exists {
		return false
	}
	f.releasedPages = append(f.releasedPages, pageNum)
	f.releasedPagesSet[pageNum] = struct{}{}
	return true
}

func (f *Freelist) sanitizeReleasedPages() int {
	freelistPageSet := make(map[uint64]struct{}, len(f.freelistPages))
	for _, pageNum := range f.freelistPages {
		freelistPageSet[pageNum] = struct{}{}
	}

	cleaned := f.releasedPages[:0]
	dedup := make(map[uint64]struct{}, len(f.releasedPages))
	removed := 0

	for _, pageNum := range f.releasedPages {
		if pageNum == metaPageNumber {
			removed++
			continue
		}
		if pageNum >= f.maxPages {
			removed++
			continue
		}
		if pageNum > f.currentPage {
			removed++
			continue
		}
		if _, exists := freelistPageSet[pageNum]; exists {
			removed++
			continue
		}
		if _, exists := dedup[pageNum]; exists {
			removed++
			continue
		}
		dedup[pageNum] = struct{}{}
		cleaned = append(cleaned, pageNum)
	}

	f.releasedPages = cleaned
	f.releasedPagesSet = dedup
	if removed > 0 {
		f.dirty = true
	}
	return removed
}

func (f *Freelist) GetNextPageNumber() (uint64, error) {
	f.ensureReleasedPagesSet()

	for len(f.releasedPages) > 0 {
		pageNum, _ := f.popReleasedPage()
		if pageNum == metaPageNumber || pageNum >= f.maxPages || pageNum > f.currentPage || f.isFreelistStoragePage(pageNum) {
			logger.Warn("freelist dropped invalid page",
				"page_num", pageNum,
				"current_page", f.currentPage,
				"max_pages", f.maxPages)
			f.dirty = true
			continue
		}

		f.dirty = true
		return pageNum, nil
	}

	if f.maxPages == 0 || f.currentPage >= (f.maxPages-1) {
		return 0, ErrNoPagesLeft
	}
	if f.currentPage < rootPageNumber {
		f.currentPage = rootPageNumber
	}

	f.currentPage += 1
	f.dirty = true
	logger.Debug("freelist GetNextPage", "pageNum", f.currentPage)
	return f.currentPage, nil
}

func (f *Freelist) ReleasePage(pageNum uint64) {
	if !f.addReleasedPage(pageNum) {
		return
	}
	f.dirty = true
	logger.Debug("releasing pageNum", "pageNumber", pageNum)
}

func calculatePagesNeeded(numEntries, entriesPerFirstPage, entriesPerExtraPage int) int {
	if numEntries <= entriesPerFirstPage {
		return 1
	}
	return 1 + (numEntries-entriesPerFirstPage+entriesPerExtraPage-1)/entriesPerExtraPage
}

// Reads entries from a freelist page.
func readEntriesFromPage(page *Page, startPos int, maxEntries int, entries *[]uint64, totalEntries *uint64) uint64 {
	pos := startPos
	for i := 0; i < maxEntries && uint64(len(*entries)) < *totalEntries; i++ {
		if pos+UInt64Size > len(page.Data) {
			break
		}
		*entries = append(*entries, binary.LittleEndian.Uint64(page.Data[pos:]))
		pos += UInt64Size
	}

	if freelistNextPageOffset+UInt64Size > len(page.Data) {
		return 0
	}
	return binary.LittleEndian.Uint64(page.Data[freelistNextPageOffset:])
}

func writeEntriesToPage(page *Page, startPos int, entries []uint64, startIdx int, maxEntries int) int {
	pos := startPos
	entriesWritten := 0
	for i := 0; i < maxEntries && startIdx+i < len(entries); i++ {
		if pos+UInt64Size > len(page.Data) {
			break
		}
		binary.LittleEndian.PutUint64(page.Data[pos:], entries[startIdx+i])
		pos += UInt64Size
		entriesWritten++
	}
	return entriesWritten
}

func isZeroPage(data []byte) bool {
	for _, b := range data {
		if b != 0 {
			return false
		}
	}
	return true
}

func ReadFreelist(dal *Dal) (*Freelist, error) {
	freelist := NewFreelist(dal.meta.pageSize, dal.maxPages)
	freelist.freelistPages = []uint64{dal.meta.freelistPageNumber}

	firstPage, err := dal.GetPage(dal.meta.freelistPageNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to get freelist pageNum: %w", err)
	}

	if firstPage.Data[freelistPageTypeOffset] != FreeListPage {
		if isZeroPage(firstPage.Data) {
			// Backward compatibility with databases created before freelist bootstrap fix.
			freelist.currentPage = rootPageNumber
			freelist.maxPages = dal.maxPages
			freelist.dirty = true
			logger.Warn("freelist page is empty, rebuilding freelist metadata")
			return freelist, nil
		}
		return nil, fmt.Errorf("%w: invalid first page type %d", ErrCorruptedFreelist, firstPage.Data[freelistPageTypeOffset])
	}

	freelist.currentPage = binary.LittleEndian.Uint64(firstPage.Data[freelistCurrentPageOffset:])
	storedMaxPages := binary.LittleEndian.Uint64(firstPage.Data[freelistMaxPagesOffset:])
	numReleasedPages := binary.LittleEndian.Uint64(firstPage.Data[freelistNumPagesOffset:])
	freelist.maxPages = dal.maxPages

	if freelist.currentPage < rootPageNumber || freelist.currentPage >= dal.maxPages {
		return nil, fmt.Errorf("%w: invalid current page %d (max pages %d)", ErrCorruptedFreelist, freelist.currentPage, dal.maxPages)
	}
	if storedMaxPages != dal.maxPages {
		freelist.dirty = true
	}

	freelist.releasedPages = make([]uint64, 0)
	nextPageNum := readEntriesFromPage(
		firstPage,
		freelistFirstPageEntriesOffset,
		freelist.entriesPerFirstPage,
		&freelist.releasedPages,
		&numReleasedPages,
	)

	visitedPages := map[uint64]struct{}{dal.meta.freelistPageNumber: {}}
	for nextPageNum != 0 {
		if nextPageNum >= dal.maxPages {
			return nil, fmt.Errorf("%w: next freelist page %d is out of bounds", ErrCorruptedFreelist, nextPageNum)
		}
		if _, exists := visitedPages[nextPageNum]; exists {
			return nil, fmt.Errorf("%w: freelist page cycle at %d", ErrCorruptedFreelist, nextPageNum)
		}
		visitedPages[nextPageNum] = struct{}{}
		freelist.freelistPages = append(freelist.freelistPages, nextPageNum)

		page, getPageErr := dal.GetPage(nextPageNum)
		if getPageErr != nil {
			return nil, fmt.Errorf("failed to read freelist pageNum %d: %w", nextPageNum, getPageErr)
		}
		if page.Data[freelistPageTypeOffset] != FreeListPage {
			return nil, fmt.Errorf("%w: page %d has type %d", ErrCorruptedFreelist, nextPageNum, page.Data[freelistPageTypeOffset])
		}

		nextPageNum = readEntriesFromPage(
			page,
			freelistExtraPageEntriesOffset,
			freelist.entriesPerExtraPage,
			&freelist.releasedPages,
			&numReleasedPages,
		)
	}

	if uint64(len(freelist.releasedPages)) != numReleasedPages {
		return nil, fmt.Errorf(
			"%w: expected %d released pages, read %d",
			ErrCorruptedFreelist,
			numReleasedPages,
			len(freelist.releasedPages),
		)
	}

	removedEntries := freelist.sanitizeReleasedPages()
	if removedEntries > 0 {
		logger.Warn("freelist sanitized invalid released pages", "removed", removedEntries)
	}

	logger.Debug("read freelist",
		"currentPage", freelist.currentPage,
		"releasedPages", len(freelist.releasedPages),
		"pagesRead", len(freelist.freelistPages))

	return freelist, nil
}

func WriteFreelist(dal *Dal, freelist *Freelist) error {
	if freelist == nil {
		return fmt.Errorf("freelist is nil")
	}

	if freelist.maxPages != dal.maxPages {
		freelist.maxPages = dal.maxPages
		freelist.dirty = true
	}
	if freelist.currentPage < rootPageNumber {
		freelist.currentPage = rootPageNumber
		freelist.dirty = true
	}

	if len(freelist.freelistPages) == 0 {
		freelist.freelistPages = []uint64{dal.meta.freelistPageNumber}
		freelist.dirty = true
	} else if freelist.freelistPages[0] != dal.meta.freelistPageNumber {
		freelist.freelistPages[0] = dal.meta.freelistPageNumber
		freelist.dirty = true
	}

	_ = freelist.sanitizeReleasedPages()

	if !freelist.dirty {
		return nil
	}

	pagesNeeded := calculatePagesNeeded(
		len(freelist.releasedPages),
		freelist.entriesPerFirstPage,
		freelist.entriesPerExtraPage,
	)
	managePageErr := manageFreelistPageAllocation(dal, freelist, pagesNeeded)
	if managePageErr != nil {
		return managePageErr
	}

	pagesToWrite := len(freelist.freelistPages)
	if pagesToWrite == 0 {
		return fmt.Errorf("%w: freelist page chain is empty", ErrCorruptedFreelist)
	}

	firstPage, getFirstPageErr := dal.GetPage(dal.meta.freelistPageNumber)
	if getFirstPageErr != nil {
		return fmt.Errorf("failed to get first freelist pageNum: %w", getFirstPageErr)
	}
	firstPage.Clear()

	nextPageNum := uint64(0)
	if pagesToWrite > 1 {
		nextPageNum = freelist.freelistPages[1]
	}
	firstPage.Data[freelistPageTypeOffset] = FreeListPage
	binary.LittleEndian.PutUint64(firstPage.Data[freelistNextPageOffset:], nextPageNum)
	binary.LittleEndian.PutUint64(firstPage.Data[freelistCurrentPageOffset:], freelist.currentPage)
	binary.LittleEndian.PutUint64(firstPage.Data[freelistMaxPagesOffset:], freelist.maxPages)
	binary.LittleEndian.PutUint64(firstPage.Data[freelistNumPagesOffset:], uint64(len(freelist.releasedPages)))

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

	entriesIdx := entriesWritten
	for i := 1; i < pagesToWrite; i++ {
		pageNum := freelist.freelistPages[i]
		page, getPageErr := dal.GetPage(pageNum)
		if getPageErr != nil {
			return fmt.Errorf("failed to get freelist pageNum %d: %w", pageNum, getPageErr)
		}
		page.Clear()

		nextPageNum = uint64(0)
		if i < pagesToWrite-1 {
			nextPageNum = freelist.freelistPages[i+1]
		}
		page.Data[freelistPageTypeOffset] = FreeListPage
		binary.LittleEndian.PutUint64(page.Data[freelistNextPageOffset:], nextPageNum)

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

	if entriesIdx < len(freelist.releasedPages) {
		return fmt.Errorf("%w: freelist entry overflow, wrote %d of %d entries", ErrCorruptedFreelist, entriesIdx, len(freelist.releasedPages))
	}

	logger.Debug("write freelist",
		"currentPage", freelist.currentPage,
		"releasedPages", len(freelist.releasedPages),
		"pagesUsed", pagesToWrite)

	if !dal.txLog.active {
		freelist.dirty = false
	}
	return nil
}

func calculateFreelistCapacity(pageSize int) (int, int) {
	entriesPerFirstPage := (pageSize - freelistFirstPageEntriesOffset) / UInt64Size
	entriesPerExtraPage := (pageSize - freelistExtraPageEntriesOffset) / UInt64Size
	return entriesPerFirstPage, entriesPerExtraPage
}

func allocateFreelistPage(dal *Dal, freelist *Freelist) (uint64, error) {
	// Prefer extending currentPage to avoid unstable shrink/grow cycles.
	if freelist.currentPage < freelist.maxPages-1 {
		freelist.currentPage++
		return freelist.currentPage, nil
	}

	for len(freelist.releasedPages) > 0 {
		pageNum, _ := freelist.popReleasedPage()
		if pageNum == metaPageNumber || pageNum == dal.meta.freelistPageNumber {
			continue
		}
		if pageNum >= freelist.maxPages || pageNum > freelist.currentPage {
			continue
		}
		if freelist.isFreelistStoragePage(pageNum) {
			continue
		}
		return pageNum, nil
	}

	return 0, ErrNoPagesLeft
}

func manageFreelistPageAllocation(dal *Dal, freelist *Freelist, pagesNeeded int) error {
	oldPageCount := len(freelist.freelistPages)
	if pagesNeeded > oldPageCount {
		for len(freelist.freelistPages) < pagesNeeded {
			newPageNum, err := allocateFreelistPage(dal, freelist)
			if err != nil {
				return fmt.Errorf("failed to allocate freelist pageNum: %w", err)
			}
			freelist.freelistPages = append(freelist.freelistPages, newPageNum)
			freelist.dirty = true
		}
	}

	for len(freelist.freelistPages) > pagesNeeded {
		prospectivePages := len(freelist.freelistPages) - 1
		prospectiveEntries := len(freelist.releasedPages) + 1
		prospectiveNeeded := calculatePagesNeeded(
			prospectiveEntries,
			freelist.entriesPerFirstPage,
			freelist.entriesPerExtraPage,
		)

		// Releasing this page would immediately require another freelist page.
		// Keep current allocation to avoid dropping pages.
		if prospectiveNeeded > prospectivePages {
			break
		}

		dropIdx := len(freelist.freelistPages) - 1
		droppedPageNum := freelist.freelistPages[dropIdx]
		freelist.freelistPages = freelist.freelistPages[:dropIdx]

		logger.Debug("freelist drop own pageNum", "pageNum", droppedPageNum)
		if err := dal.ReleasePage(droppedPageNum); err != nil {
			return err
		}
		freelist.dirty = true
	}

	return nil
}
