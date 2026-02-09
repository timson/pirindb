package storage

import (
	"encoding/binary"
	"fmt"
	"sort"
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
	currentPage          uint64
	maxPages             uint64
	releasedPages        []uint64
	releasedPagesDirty   bool
	releasedPagesSet     map[uint64]struct{}
	releasedPageIndex    map[uint64]int
	releasedExtents      []pageExtent
	releasedExtentsDirty bool
	freelistPages        []uint64
	entriesPerFirstPage  int
	entriesPerExtraPage  int
	persistedState       *freelistPersistedState
	dirty                bool
}

type pageExtent struct {
	start uint64
	end   uint64
}

type freelistPersistedState struct {
	currentPage   uint64
	maxPages      uint64
	releasedPages []uint64
	freelistPages []uint64
}

func NewFreelist(pageSize uint64, maxPages uint64) *Freelist {
	entriesPerFirstPage, entriesPerExtraPage := calculateFreelistCapacity(int(pageSize))
	return &Freelist{
		currentPage:          rootPageNumber,
		maxPages:             maxPages,
		releasedPages:        make([]uint64, 0),
		releasedPagesDirty:   false,
		releasedPagesSet:     make(map[uint64]struct{}),
		releasedPageIndex:    nil,
		releasedExtents:      nil,
		releasedExtentsDirty: true,
		freelistPages:        make([]uint64, 0),
		entriesPerFirstPage:  entriesPerFirstPage,
		entriesPerExtraPage:  entriesPerExtraPage,
		persistedState:       nil,
		dirty:                false,
	}
}

func (f *Freelist) availablePageN() int {
	f.ensureReleasedPagesSet()
	if f.maxPages == 0 || f.currentPage >= f.maxPages-1 {
		return len(f.releasedPagesSet)
	}
	return int((f.maxPages-1)-f.currentPage) + len(f.releasedPagesSet)
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
	f.releasedPageIndex = nil
	f.releasedExtentsDirty = true
}

func (f *Freelist) ensureReleasedPageIndex() {
	if f.releasedPageIndex != nil {
		return
	}
	f.releasedPageIndex = make(map[uint64]int, len(f.releasedPages))
	for idx, pageNum := range f.releasedPages {
		f.releasedPageIndex[pageNum] = idx
	}
}

func (f *Freelist) ensureReleasedExtents() {
	if !f.releasedExtentsDirty {
		return
	}
	f.ensureReleasedPagesSet()
	if len(f.releasedPagesSet) == 0 {
		f.releasedExtents = nil
		f.releasedExtentsDirty = false
		return
	}
	pages := make([]uint64, 0, len(f.releasedPagesSet))
	for pageNum := range f.releasedPagesSet {
		pages = append(pages, pageNum)
	}
	sort.Slice(pages, func(i, j int) bool {
		return pages[i] < pages[j]
	})

	extents := make([]pageExtent, 0, len(pages))
	start := pages[0]
	end := pages[0]
	for idx := 1; idx < len(pages); idx++ {
		if pages[idx] == end+1 {
			end = pages[idx]
			continue
		}
		extents = append(extents, pageExtent{start: start, end: end})
		start = pages[idx]
		end = pages[idx]
	}
	extents = append(extents, pageExtent{start: start, end: end})
	f.releasedExtents = extents
	f.releasedExtentsDirty = false
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
	f.releasedPageIndex = nil
	f.releasedPagesDirty = true
	f.releasedExtentsDirty = true
	return pageNum, true
}

func (f *Freelist) addReleasedPage(pageNum uint64) bool {
	f.ensureReleasedPagesSet()
	if _, exists := f.releasedPagesSet[pageNum]; exists {
		return false
	}
	f.releasedPages = append(f.releasedPages, pageNum)
	f.releasedPagesSet[pageNum] = struct{}{}
	f.releasedPageIndex = nil
	f.releasedPagesDirty = true
	f.releasedExtentsDirty = true
	return true
}

func (f *Freelist) removeReleasedPage(pageNum uint64) bool {
	return f.removeReleasedRange(pageNum, 1) == nil
}

func (f *Freelist) removeReleasedRange(startPageNum uint64, count int) error {
	if count <= 0 {
		return fmt.Errorf("count must be greater than zero")
	}
	f.ensureReleasedPagesSet()
	for idx := 0; idx < count; idx++ {
		pageNum := startPageNum + uint64(idx)
		if _, exists := f.releasedPagesSet[pageNum]; !exists {
			return fmt.Errorf("page %d is not released", pageNum)
		}
	}

	f.ensureReleasedPageIndex()
	for idx := 0; idx < count; idx++ {
		pageNum := startPageNum + uint64(idx)
		pageIdx, exists := f.releasedPageIndex[pageNum]
		if !exists {
			return fmt.Errorf("page %d index not found", pageNum)
		}

		lastIdx := len(f.releasedPages) - 1
		lastPageNum := f.releasedPages[lastIdx]
		f.releasedPages[pageIdx] = lastPageNum
		f.releasedPages = f.releasedPages[:lastIdx]

		if pageIdx < lastIdx {
			f.releasedPageIndex[lastPageNum] = pageIdx
		}
		delete(f.releasedPageIndex, pageNum)
		delete(f.releasedPagesSet, pageNum)
	}
	f.releasedPagesDirty = true
	f.releasedExtentsDirty = true
	return nil
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
	f.releasedPageIndex = nil
	f.releasedExtentsDirty = true
	if removed > 0 {
		f.releasedPagesDirty = true
		f.dirty = true
	}
	return removed
}

func (f *Freelist) GetNextPageNumber() (uint64, error) {
	f.ensureReleasedPagesSet()

	for len(f.releasedPages) > 0 {
		pageNum, _ := f.popReleasedPage()
		if !f.isAllocatablePage(pageNum) {
			logger.Warn("freelist dropped invalid page",
				"page_num", pageNum,
				"current_page", f.currentPage,
				"max_pages", f.maxPages)
			f.dirty = true
			continue
		}

		f.dirty = true
		logger.Debug("freelist GetNextPage", "pageNum", pageNum)
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

func (f *Freelist) isAllocatablePage(pageNum uint64) bool {
	if pageNum == metaPageNumber {
		return false
	}
	if pageNum >= f.maxPages {
		return false
	}
	if pageNum > f.currentPage {
		return false
	}
	if f.isFreelistStoragePage(pageNum) {
		return false
	}
	return true
}

func (f *Freelist) allocateFromReleasedRun(count int) ([]uint64, bool, error) {
	f.ensureReleasedPagesSet()
	for len(f.releasedPagesSet) > 0 {
		extentIdx, startPageNum, found := f.findContiguousReleasedRun(count)
		if !found {
			break
		}
		if err := f.removeReleasedRange(startPageNum, count); err != nil {
			return nil, false, err
		}

		extent := f.releasedExtents[extentIdx]
		if startPageNum == extent.start {
			f.releasedExtents = append(f.releasedExtents[:extentIdx], f.releasedExtents[extentIdx+1:]...)
		} else {
			f.releasedExtents[extentIdx].end = startPageNum - 1
		}
		f.releasedExtentsDirty = false

		pageNums := make([]uint64, count)
		validRun := true
		for idx := 0; idx < count; idx++ {
			pageNum := startPageNum + uint64(idx)
			pageNums[idx] = pageNum
			if !f.isAllocatablePage(pageNum) {
				validRun = false
			}
		}
		f.dirty = true
		if !validRun {
			logger.Warn("freelist dropped invalid contiguous run",
				"start_page", startPageNum,
				"count", count,
				"current_page", f.currentPage,
				"max_pages", f.maxPages)
			continue
		}
		return pageNums, true, nil
	}
	return nil, false, nil
}

func (f *Freelist) findContiguousReleasedRun(count int) (int, uint64, bool) {
	if count <= 0 {
		return -1, 0, false
	}
	f.ensureReleasedExtents()
	for idx := len(f.releasedExtents) - 1; idx >= 0; idx-- {
		extent := f.releasedExtents[idx]
		extentLen := int(extent.end - extent.start + 1)
		if extentLen < count {
			continue
		}
		startPageNum := extent.end - uint64(count) + 1
		return idx, startPageNum, true
	}
	return -1, 0, false
}

func (f *Freelist) GetConsecutivePageNumbers(count int) ([]uint64, error) {
	if count <= 0 {
		return nil, fmt.Errorf("count must be greater than zero")
	}
	if count == 1 {
		pageNum, err := f.GetNextPageNumber()
		if err != nil {
			return nil, err
		}
		return []uint64{pageNum}, nil
	}

	if pages, ok, err := f.allocateFromReleasedRun(count); err != nil {
		return nil, err
	} else if ok {
		logger.Debug("freelist GetConsecutivePages", "count", count, "start_page", pages[0])
		return pages, nil
	}

	if f.currentPage < rootPageNumber {
		f.currentPage = rootPageNumber
	}
	targetPage := f.currentPage + uint64(count)
	if f.maxPages == 0 || targetPage > f.maxPages-1 {
		return nil, ErrNoPagesLeft
	}
	pages := make([]uint64, count)
	for idx := 0; idx < count; idx++ {
		f.currentPage++
		pages[idx] = f.currentPage
	}
	f.dirty = true
	logger.Debug("freelist GetConsecutivePages", "count", count, "start_page", pages[0])
	return pages, nil
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
	freelist.releasedPagesDirty = false

	freelist.releasedExtentsDirty = true
	freelist.persistedState = &freelistPersistedState{
		currentPage:   freelist.currentPage,
		maxPages:      freelist.maxPages,
		releasedPages: append([]uint64(nil), freelist.releasedPages...),
		freelistPages: append([]uint64(nil), freelist.freelistPages...),
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
	if freelist.releasedPagesDirty && len(freelist.releasedPages) > 1 {
		sort.Slice(freelist.releasedPages, func(i, j int) bool {
			return freelist.releasedPages[i] < freelist.releasedPages[j]
		})
		freelist.releasedPageIndex = nil
	}
	freelist.releasedPagesDirty = false

	pagesToWrite := len(freelist.freelistPages)
	if pagesToWrite == 0 {
		return fmt.Errorf("%w: freelist page chain is empty", ErrCorruptedFreelist)
	}

	entryDiffIdx := 0
	if freelist.persistedState != nil {
		entryDiffIdx = firstDiffIndex(freelist.persistedState.releasedPages, freelist.releasedPages)
	}
	entryDirtyPageIdx := pagesToWrite
	if entryDiffIdx >= 0 {
		entryDirtyPageIdx = entriesToPageIndex(entryDiffIdx, freelist.entriesPerFirstPage, freelist.entriesPerExtraPage)
	}

	tailDirtyPageIdx := entryDirtyPageIdx
	if freelist.persistedState != nil && !equalPageSlices(freelist.persistedState.freelistPages, freelist.freelistPages) {
		commonPages := min(len(freelist.persistedState.freelistPages), len(freelist.freelistPages))
		if commonPages > 0 {
			lastCommonIdx := commonPages - 1
			if lastCommonIdx < tailDirtyPageIdx {
				tailDirtyPageIdx = lastCommonIdx
			}
		}
	}
	if tailDirtyPageIdx < 1 {
		tailDirtyPageIdx = 1
	}

	firstPage := &Page{
		PageNumber: dal.meta.freelistPageNumber,
		Data:       make([]byte, dal.meta.pageSize),
	}

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

	for i := 1; i < pagesToWrite; i++ {
		if i < tailDirtyPageIdx {
			continue
		}
		pageNum := freelist.freelistPages[i]
		page := &Page{
			PageNumber: pageNum,
			Data:       make([]byte, dal.meta.pageSize),
		}

		nextPageNum = uint64(0)
		if i < pagesToWrite-1 {
			nextPageNum = freelist.freelistPages[i+1]
		}
		page.Data[freelistPageTypeOffset] = FreeListPage
		binary.LittleEndian.PutUint64(page.Data[freelistNextPageOffset:], nextPageNum)

		entriesIdx := entriesWritten + (i-1)*freelist.entriesPerExtraPage
		_ = writeEntriesToPage(
			page,
			freelistExtraPageEntriesOffset,
			freelist.releasedPages,
			entriesIdx,
			freelist.entriesPerExtraPage,
		)

		if err := dal.SetPage(page); err != nil {
			return err
		}
	}

	maxEntriesCapacity := freelist.entriesPerFirstPage + max(0, pagesToWrite-1)*freelist.entriesPerExtraPage
	if maxEntriesCapacity < len(freelist.releasedPages) {
		return fmt.Errorf("%w: freelist entry overflow, capacity %d entries, have %d entries", ErrCorruptedFreelist, maxEntriesCapacity, len(freelist.releasedPages))
	}

	logger.Debug("write freelist",
		"currentPage", freelist.currentPage,
		"releasedPages", len(freelist.releasedPages),
		"pagesUsed", pagesToWrite)

	if !dal.txLog.active {
		freelist.dirty = false
		freelist.persistedState = &freelistPersistedState{
			currentPage:   freelist.currentPage,
			maxPages:      freelist.maxPages,
			releasedPages: append([]uint64(nil), freelist.releasedPages...),
			freelistPages: append([]uint64(nil), freelist.freelistPages...),
		}
	}
	return nil
}

func firstDiffIndex(previous []uint64, current []uint64) int {
	sharedLen := min(len(previous), len(current))
	for idx := 0; idx < sharedLen; idx++ {
		if previous[idx] != current[idx] {
			return idx
		}
	}
	if len(previous) == len(current) {
		return -1
	}
	return sharedLen
}

func entriesToPageIndex(entryIdx int, firstPageEntries int, extraPageEntries int) int {
	if entryIdx <= 0 {
		return 0
	}
	if entryIdx < firstPageEntries {
		return 0
	}
	if extraPageEntries <= 0 {
		return 0
	}
	return 1 + (entryIdx-firstPageEntries)/extraPageEntries
}

func equalPageSlices(a []uint64, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for idx := range a {
		if a[idx] != b[idx] {
			return false
		}
	}
	return true
}

func calculateFreelistCapacity(pageSize int) (int, int) {
	entriesPerFirstPage := (pageSize - freelistFirstPageEntriesOffset) / UInt64Size
	entriesPerExtraPage := (pageSize - freelistExtraPageEntriesOffset) / UInt64Size
	return entriesPerFirstPage, entriesPerExtraPage
}

func allocateFreelistPage(dal *Dal, freelist *Freelist) (uint64, error) {
	// Prefer extending currentPage to avoid unstable shrink/grow cycles.
	if freelist.maxPages > 0 && freelist.currentPage < freelist.maxPages-1 {
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
