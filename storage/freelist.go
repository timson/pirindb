package storage

import (
	"encoding/binary"
	"fmt"
	"sort"
)

// Version 3 stores released-page extents as (start,end) uint64 pairs. Legacy
// version 2 databases are read and written as individual uint64 page numbers.
const (
	freelistPageTypeOffset         = 0
	freelistNextPageOffset         = freelistPageTypeOffset + UInt8Size
	freelistCurrentPageOffset      = freelistNextPageOffset + UInt64Size
	freelistMaxPagesOffset         = freelistCurrentPageOffset + UInt64Size
	freelistNumPagesOffset         = freelistMaxPagesOffset + UInt64Size
	freelistFirstPageEntriesOffset = freelistNumPagesOffset + UInt64Size
	freelistExtraPageEntriesOffset = freelistNextPageOffset + UInt64Size

	freelistLegacyRecordSize = UInt64Size
	freelistExtentRecordSize = 2 * UInt64Size
)

type pageExtent struct {
	start uint64
	end   uint64
}

func (extent pageExtent) length() uint64 {
	if extent.end < extent.start {
		return 0
	}
	return extent.end - extent.start + 1
}

type freelistPersistedState struct {
	freelistPages []uint64
}

type Freelist struct {
	currentPage       uint64
	maxPages          uint64
	firstDataPage     uint64
	releasedExtents   []pageExtent
	releasedCount     uint64
	freelistPages     []uint64
	freelistPageSet   map[uint64]struct{}
	recordsPerFirst   int
	recordsPerExtra   int
	legacyPageRecords bool
	persistedState    *freelistPersistedState
	dirty             bool
	dirtyExtentIndex  int
}

func NewFreelist(pageSize uint64, maxPages uint64) *Freelist {
	return newFreelist(pageSize, maxPages, true)
}

func newFreelist(pageSize uint64, maxPages uint64, checksums bool) *Freelist {
	legacy := !checksums
	firstDataPage := uint64(rootPageNumber)
	if legacy {
		firstDataPage = legacyRootPageNumber
	}
	first, extra := calculateFreelistCapacityForFormat(int(pageSize), checksums)
	return &Freelist{
		currentPage:       firstDataPage,
		maxPages:          maxPages,
		firstDataPage:     firstDataPage,
		releasedExtents:   make([]pageExtent, 0),
		freelistPages:     make([]uint64, 0),
		freelistPageSet:   make(map[uint64]struct{}),
		recordsPerFirst:   first,
		recordsPerExtra:   extra,
		legacyPageRecords: legacy,
		dirtyExtentIndex:  -1,
	}
}

func (f *Freelist) releasedPageN() uint64 {
	if f == nil {
		return 0
	}
	return f.releasedCount
}

func uint64ToIntSaturated(value uint64) int {
	maxInt := uint64(^uint(0) >> 1)
	if value > maxInt {
		return int(maxInt)
	}
	return int(value)
}

func (f *Freelist) availablePageN() int {
	if f == nil {
		return 0
	}
	available := f.releasedCount
	if f.maxPages > 0 && f.currentPage < f.maxPages-1 {
		available += (f.maxPages - 1) - f.currentPage
	}
	return uint64ToIntSaturated(available)
}

func (f *Freelist) rebuildFreelistPageSet() {
	f.freelistPageSet = make(map[uint64]struct{}, len(f.freelistPages))
	for _, pageNum := range f.freelistPages {
		f.freelistPageSet[pageNum] = struct{}{}
	}
}

func (f *Freelist) isFreelistStoragePage(pageNum uint64) bool {
	if f.freelistPageSet == nil {
		f.rebuildFreelistPageSet()
	}
	_, exists := f.freelistPageSet[pageNum]
	return exists
}

func (f *Freelist) containsReleasedPage(pageNum uint64) bool {
	idx := sort.Search(len(f.releasedExtents), func(i int) bool {
		return f.releasedExtents[i].end >= pageNum
	})
	return idx < len(f.releasedExtents) && f.releasedExtents[idx].start <= pageNum
}

func (f *Freelist) markExtentDirty(index int) {
	if index < 0 {
		index = 0
	}
	if f.dirtyExtentIndex < 0 || index < f.dirtyExtentIndex {
		f.dirtyExtentIndex = index
	}
	f.dirty = true
}

func (f *Freelist) addReleasedPage(pageNum uint64) bool {
	idx := sort.Search(len(f.releasedExtents), func(i int) bool {
		return f.releasedExtents[i].end >= pageNum
	})
	if idx < len(f.releasedExtents) && f.releasedExtents[idx].start <= pageNum {
		return false
	}

	mergeLeft := idx > 0 && f.releasedExtents[idx-1].end != ^uint64(0) && f.releasedExtents[idx-1].end+1 == pageNum
	mergeRight := idx < len(f.releasedExtents) && pageNum != ^uint64(0) && pageNum+1 == f.releasedExtents[idx].start
	dirtyIndex := idx
	if mergeLeft {
		dirtyIndex--
	}
	switch {
	case mergeLeft && mergeRight:
		f.releasedExtents[idx-1].end = f.releasedExtents[idx].end
		f.releasedExtents = append(f.releasedExtents[:idx], f.releasedExtents[idx+1:]...)
	case mergeLeft:
		f.releasedExtents[idx-1].end = pageNum
	case mergeRight:
		f.releasedExtents[idx].start = pageNum
	default:
		f.releasedExtents = append(f.releasedExtents, pageExtent{})
		copy(f.releasedExtents[idx+1:], f.releasedExtents[idx:])
		f.releasedExtents[idx] = pageExtent{start: pageNum, end: pageNum}
	}
	f.releasedCount++
	f.markExtentDirty(dirtyIndex)
	return true
}

func appendUnionExtent(extents []pageExtent, extent pageExtent) []pageExtent {
	if len(extents) == 0 {
		return append(extents, extent)
	}
	last := &extents[len(extents)-1]
	adjacent := last.end != ^uint64(0) && extent.start == last.end+1
	if extent.start <= last.end || adjacent {
		if extent.end > last.end {
			last.end = extent.end
		}
		return extents
	}
	return append(extents, extent)
}

func (f *Freelist) addReleasedPages(pageNums []uint64) bool {
	if len(pageNums) == 0 {
		return false
	}
	sortedPages := append([]uint64(nil), pageNums...)
	sort.Slice(sortedPages, func(i, j int) bool { return sortedPages[i] < sortedPages[j] })
	newExtents := make([]pageExtent, 0, len(sortedPages))
	for _, pageNum := range sortedPages {
		if len(newExtents) > 0 && pageNum == newExtents[len(newExtents)-1].end {
			continue
		}
		if len(newExtents) > 0 && newExtents[len(newExtents)-1].end != ^uint64(0) && pageNum == newExtents[len(newExtents)-1].end+1 {
			newExtents[len(newExtents)-1].end = pageNum
			continue
		}
		newExtents = append(newExtents, pageExtent{start: pageNum, end: pageNum})
	}

	merged := make([]pageExtent, 0, len(f.releasedExtents)+len(newExtents))
	i, j := 0, 0
	for i < len(f.releasedExtents) || j < len(newExtents) {
		var next pageExtent
		if j >= len(newExtents) || i < len(f.releasedExtents) && f.releasedExtents[i].start <= newExtents[j].start {
			next = f.releasedExtents[i]
			i++
		} else {
			next = newExtents[j]
			j++
		}
		merged = appendUnionExtent(merged, next)
	}
	var count uint64
	for _, extent := range merged {
		count += extent.length()
	}
	if count == f.releasedCount {
		return false
	}
	dirtyIndex := firstDifferentExtent(f.releasedExtents, merged)
	f.releasedExtents = merged
	f.releasedCount = count
	f.markExtentDirty(dirtyIndex)
	return true
}

func (f *Freelist) removeReleasedRange(startPageNum uint64, count int) error {
	if count <= 0 {
		return fmt.Errorf("count must be greater than zero")
	}
	if startPageNum > ^uint64(0)-uint64(count-1) {
		return fmt.Errorf("released-page range overflows")
	}
	endPageNum := startPageNum + uint64(count-1)
	idx := sort.Search(len(f.releasedExtents), func(i int) bool {
		return f.releasedExtents[i].end >= startPageNum
	})
	if idx >= len(f.releasedExtents) || f.releasedExtents[idx].start > startPageNum || f.releasedExtents[idx].end < endPageNum {
		return fmt.Errorf("released range %d-%d is not available", startPageNum, endPageNum)
	}
	extent := f.releasedExtents[idx]
	switch {
	case startPageNum == extent.start && endPageNum == extent.end:
		f.releasedExtents = append(f.releasedExtents[:idx], f.releasedExtents[idx+1:]...)
	case startPageNum == extent.start:
		f.releasedExtents[idx].start = endPageNum + 1
	case endPageNum == extent.end:
		f.releasedExtents[idx].end = startPageNum - 1
	default:
		right := pageExtent{start: endPageNum + 1, end: extent.end}
		f.releasedExtents[idx].end = startPageNum - 1
		f.releasedExtents = append(f.releasedExtents, pageExtent{})
		copy(f.releasedExtents[idx+2:], f.releasedExtents[idx+1:])
		f.releasedExtents[idx+1] = right
	}
	f.releasedCount -= uint64(count)
	f.markExtentDirty(idx)
	return nil
}

func (f *Freelist) popReleasedPage() (uint64, bool) {
	if len(f.releasedExtents) == 0 {
		return 0, false
	}
	idx := len(f.releasedExtents) - 1
	pageNum := f.releasedExtents[idx].end
	if f.releasedExtents[idx].start == pageNum {
		f.releasedExtents = f.releasedExtents[:idx]
	} else {
		f.releasedExtents[idx].end--
	}
	f.releasedCount--
	f.markExtentDirty(idx)
	return pageNum, true
}

func (f *Freelist) isAllocatablePage(pageNum uint64) bool {
	return pageNum >= f.firstDataPage && pageNum < f.maxPages && pageNum <= f.currentPage && !f.isFreelistStoragePage(pageNum)
}

func (f *Freelist) GetNextPageNumber() (uint64, error) {
	for len(f.releasedExtents) > 0 {
		pageNum, _ := f.popReleasedPage()
		if f.isAllocatablePage(pageNum) {
			return pageNum, nil
		}
		logger.Warn("freelist dropped invalid page", "page_num", pageNum)
	}
	if f.maxPages == 0 || f.currentPage >= f.maxPages-1 {
		return 0, ErrNoPagesLeft
	}
	if f.currentPage < f.firstDataPage {
		f.currentPage = f.firstDataPage
	}
	f.currentPage++
	f.dirty = true
	return f.currentPage, nil
}

func (f *Freelist) findContiguousReleasedRun(count int) (int, uint64, bool) {
	if count <= 0 {
		return -1, 0, false
	}
	for idx := len(f.releasedExtents) - 1; idx >= 0; idx-- {
		extent := f.releasedExtents[idx]
		if extent.length() >= uint64(count) {
			return idx, extent.end - uint64(count) + 1, true
		}
	}
	return -1, 0, false
}

func (f *Freelist) GetConsecutivePageNumbers(count int) ([]uint64, error) {
	if count <= 0 {
		return nil, fmt.Errorf("count must be greater than zero")
	}
	if _, startPageNum, found := f.findContiguousReleasedRun(count); found {
		if err := f.removeReleasedRange(startPageNum, count); err != nil {
			return nil, err
		}
		pages := make([]uint64, count)
		for idx := range pages {
			pages[idx] = startPageNum + uint64(idx)
			if !f.isAllocatablePage(pages[idx]) {
				return nil, fmt.Errorf("freelist selected reserved page %d", pages[idx])
			}
		}
		return pages, nil
	}
	if f.currentPage < f.firstDataPage {
		f.currentPage = f.firstDataPage
	}
	if uint64(count) > ^uint64(0)-f.currentPage {
		return nil, ErrNoPagesLeft
	}
	targetPage := f.currentPage + uint64(count)
	if f.maxPages == 0 || targetPage >= f.maxPages {
		return nil, ErrNoPagesLeft
	}
	pages := make([]uint64, count)
	for idx := range pages {
		f.currentPage++
		pages[idx] = f.currentPage
	}
	f.dirty = true
	return pages, nil
}

func (f *Freelist) ReleasePage(pageNum uint64) {
	if f.addReleasedPage(pageNum) {
		logger.Debug("releasing page", "page_number", pageNum)
	}
}

func calculatePagesNeeded(numRecords, recordsPerFirstPage, recordsPerExtraPage int) int {
	if numRecords <= recordsPerFirstPage {
		return 1
	}
	if recordsPerExtraPage <= 0 {
		return 0
	}
	return 1 + (numRecords-recordsPerFirstPage+recordsPerExtraPage-1)/recordsPerExtraPage
}

func isZeroPage(data []byte) bool {
	for _, value := range data {
		if value != 0 {
			return false
		}
	}
	return true
}

func pageNext(page *Page) (uint64, error) {
	if page == nil || len(page.Data) < freelistNextPageOffset+UInt64Size {
		return 0, fmt.Errorf("%w: truncated freelist page", ErrCorruptedFreelist)
	}
	return binary.LittleEndian.Uint64(page.Data[freelistNextPageOffset:]), nil
}

func appendExtentCanonical(f *Freelist, extent pageExtent) error {
	if extent.start > extent.end {
		return fmt.Errorf("%w: invalid extent %d-%d", ErrCorruptedFreelist, extent.start, extent.end)
	}
	if len(f.releasedExtents) > 0 {
		previous := &f.releasedExtents[len(f.releasedExtents)-1]
		if extent.start <= previous.end {
			return fmt.Errorf("%w: overlapping freelist extents", ErrCorruptedFreelist)
		}
		if previous.end != ^uint64(0) && extent.start == previous.end+1 {
			return fmt.Errorf("%w: adjacent freelist extents are not canonical", ErrCorruptedFreelist)
		}
	}
	f.releasedExtents = append(f.releasedExtents, extent)
	f.releasedCount += extent.length()
	return nil
}

func readFreelistChain(dal *Dal, firstPage *Page, recordCount uint64, legacy bool, f *Freelist) error {
	visited := map[uint64]struct{}{dal.meta.freelistPageNumber: {}}
	page := firstPage
	pageNum := dal.meta.freelistPageNumber
	recordIndex := uint64(0)
	first := true
	var legacyRun *pageExtent

	for {
		startOffset := freelistExtraPageEntriesOffset
		capacity := f.recordsPerExtra
		if first {
			startOffset = freelistFirstPageEntriesOffset
			capacity = f.recordsPerFirst
			first = false
		}
		recordSize := freelistExtentRecordSize
		if legacy {
			recordSize = freelistLegacyRecordSize
		}
		for idx := 0; idx < capacity && recordIndex < recordCount; idx++ {
			pos := startOffset + idx*recordSize
			if pos > dal.usablePageSize()-recordSize {
				return fmt.Errorf("%w: freelist record exceeds page %d", ErrCorruptedFreelist, pageNum)
			}
			if legacy {
				releasedPage := binary.LittleEndian.Uint64(page.Data[pos:])
				if legacyRun != nil && legacyRun.end != ^uint64(0) && releasedPage == legacyRun.end+1 {
					legacyRun.end = releasedPage
					f.releasedCount++
				} else {
					if legacyRun != nil && releasedPage <= legacyRun.end {
						return fmt.Errorf("%w: legacy entries are not strictly ordered", ErrCorruptedFreelist)
					}
					f.releasedExtents = append(f.releasedExtents, pageExtent{start: releasedPage, end: releasedPage})
					legacyRun = &f.releasedExtents[len(f.releasedExtents)-1]
					f.releasedCount++
				}
			} else {
				extent := pageExtent{
					start: binary.LittleEndian.Uint64(page.Data[pos:]),
					end:   binary.LittleEndian.Uint64(page.Data[pos+UInt64Size:]),
				}
				if err := appendExtentCanonical(f, extent); err != nil {
					return err
				}
			}
			recordIndex++
		}

		nextPageNum, err := pageNext(page)
		if err != nil {
			return err
		}
		if nextPageNum == 0 {
			break
		}
		if nextPageNum < f.firstDataPage || nextPageNum >= dal.maxPages || nextPageNum > f.currentPage || nextPageNum == dal.meta.root {
			return fmt.Errorf("%w: next freelist page %d is out of bounds", ErrCorruptedFreelist, nextPageNum)
		}
		if _, exists := visited[nextPageNum]; exists {
			return fmt.Errorf("%w: freelist cycle at page %d", ErrCorruptedFreelist, nextPageNum)
		}
		visited[nextPageNum] = struct{}{}
		f.freelistPages = append(f.freelistPages, nextPageNum)
		f.freelistPageSet[nextPageNum] = struct{}{}
		pageNum = nextPageNum
		page, err = dal.GetPage(pageNum)
		if err != nil {
			return err
		}
		if page.Data[freelistPageTypeOffset] != FreeListPage {
			return fmt.Errorf("%w: page %d has type %d", ErrCorruptedFreelist, pageNum, page.Data[freelistPageTypeOffset])
		}
	}
	if recordIndex != recordCount {
		return fmt.Errorf("%w: expected %d records, read %d", ErrCorruptedFreelist, recordCount, recordIndex)
	}
	return nil
}

func (f *Freelist) sanitizeReleasedExtents() int {
	removed := uint64(0)
	originalExtents := f.releasedExtents
	cleaned := make([]pageExtent, 0, len(f.releasedExtents))
	for _, extent := range f.releasedExtents {
		original := extent.length()
		if extent.start < f.firstDataPage {
			extent.start = f.firstDataPage
		}
		maxValid := f.currentPage
		if f.maxPages > 0 && maxValid >= f.maxPages {
			maxValid = f.maxPages - 1
		}
		if extent.end > maxValid {
			extent.end = maxValid
		}
		if extent.start > extent.end {
			removed += original
			continue
		}
		removed += original - extent.length()
		cleaned = append(cleaned, extent)
	}
	f.releasedExtents = cleaned
	if diff := firstDifferentExtent(originalExtents, cleaned); diff >= 0 {
		f.markExtentDirty(diff)
	}
	f.releasedCount = 0
	for _, extent := range cleaned {
		f.releasedCount += extent.length()
	}
	for _, reservedPage := range f.freelistPages {
		if f.containsReleasedPage(reservedPage) {
			_ = f.removeReleasedRange(reservedPage, 1)
			removed++
		}
	}
	if removed > 0 {
		f.dirty = true
	}
	return uint64ToIntSaturated(removed)
}

func ReadFreelist(dal *Dal) (*Freelist, error) {
	f := newFreelist(dal.meta.pageSize, dal.maxPages, dal.pageChecksums)
	f.freelistPages = []uint64{dal.meta.freelistPageNumber}
	f.freelistPageSet[dal.meta.freelistPageNumber] = struct{}{}
	firstPage, err := dal.GetPage(dal.meta.freelistPageNumber)
	if err != nil {
		return nil, fmt.Errorf("read freelist page: %w", err)
	}
	if firstPage.Data[freelistPageTypeOffset] != FreeListPage {
		if !dal.pageChecksums && isZeroPage(firstPage.Data) {
			f.currentPage = f.firstDataPage
			f.dirty = true
			return f, nil
		}
		return nil, fmt.Errorf("%w: invalid first page type %d", ErrCorruptedFreelist, firstPage.Data[freelistPageTypeOffset])
	}
	f.currentPage = binary.LittleEndian.Uint64(firstPage.Data[freelistCurrentPageOffset:])
	storedMaxPages := binary.LittleEndian.Uint64(firstPage.Data[freelistMaxPagesOffset:])
	recordCount := binary.LittleEndian.Uint64(firstPage.Data[freelistNumPagesOffset:])
	if f.currentPage < f.firstDataPage || f.currentPage >= dal.maxPages {
		return nil, fmt.Errorf("%w: invalid current page %d", ErrCorruptedFreelist, f.currentPage)
	}
	if recordCount > f.currentPage+1 {
		return nil, fmt.Errorf("%w: impossible record count %d", ErrCorruptedFreelist, recordCount)
	}
	if storedMaxPages != dal.maxPages {
		f.dirty = true
	}
	if err = readFreelistChain(dal, firstPage, recordCount, f.legacyPageRecords, f); err != nil {
		return nil, err
	}
	if removed := f.sanitizeReleasedExtents(); removed > 0 {
		logger.Warn("freelist sanitized invalid released pages", "removed", removed)
	}
	if f.dirty {
		// Sanitized or resized state differs from disk; force one complete
		// freelist rewrite before enabling incremental dirty-page tracking.
		f.persistedState = nil
		f.dirtyExtentIndex = 0
	} else {
		f.persistedState = &freelistPersistedState{
			freelistPages: append([]uint64(nil), f.freelistPages...),
		}
		f.dirtyExtentIndex = -1
	}
	return f, nil
}

func WriteFreelist(dal *Dal, freelist *Freelist) error {
	pages, err := BuildFreelistPages(dal, freelist)
	if err != nil {
		return err
	}
	// Publish chain/data pages before the first page that contains the record
	// count and head pointer. WAL-backed commits are atomic either way, while
	// this ordering also makes bootstrap repairs safe against interruption.
	if len(pages) > 1 {
		for _, page := range pages[1:] {
			if err = dal.SetPage(page); err != nil {
				return err
			}
		}
	}
	if len(pages) > 0 {
		if err = dal.SetPage(pages[0]); err != nil {
			return err
		}
	}
	if !dal.txLog.active {
		markFreelistPersisted(freelist)
	}
	return nil
}

func allocateFreelistPage(dal *Dal, f *Freelist) (uint64, error) {
	if f.maxPages > 0 && f.currentPage < f.maxPages-1 {
		f.currentPage++
		f.dirty = true
		return f.currentPage, nil
	}
	for len(f.releasedExtents) > 0 {
		pageNum, _ := f.popReleasedPage()
		if pageNum != dal.meta.freelistPageNumber && f.isAllocatablePage(pageNum) {
			return pageNum, nil
		}
	}
	return 0, ErrNoPagesLeft
}

func ensureFreelistPageCapacity(dal *Dal, f *Freelist, records int) error {
	detached := false
	for {
		required := calculatePagesNeeded(records, f.recordsPerFirst, f.recordsPerExtra)
		if required <= 0 {
			return fmt.Errorf("%w: invalid freelist record capacity", ErrCorruptedFreelist)
		}
		if len(f.freelistPages) >= required {
			return nil
		}
		if !detached {
			// Commit rollback snapshots share these collections. Chain expansion
			// is rare; detach once before append/pop/map mutations so the common
			// commit path can snapshot the freelist in O(1).
			f.releasedExtents = append([]pageExtent(nil), f.releasedExtents...)
			f.freelistPages = append([]uint64(nil), f.freelistPages...)
			pageSet := make(map[uint64]struct{}, len(f.freelistPageSet))
			for pageNum := range f.freelistPageSet {
				pageSet[pageNum] = struct{}{}
			}
			f.freelistPageSet = pageSet
			detached = true
		}
		pageNum, err := allocateFreelistPage(dal, f)
		if err != nil {
			return fmt.Errorf("allocate freelist page: %w", err)
		}
		f.freelistPages = append(f.freelistPages, pageNum)
		f.freelistPageSet[pageNum] = struct{}{}
		f.dirty = true
		if !f.legacyPageRecords {
			records = len(f.releasedExtents)
		} else {
			records = uint64ToIntSaturated(f.releasedCount)
		}
	}
}

func legacyReleasedPages(f *Freelist) ([]uint64, error) {
	if f.releasedCount > uint64(^uint(0)>>1) {
		return nil, fmt.Errorf("%w: legacy freelist is too large", ErrCorruptedFreelist)
	}
	pages := make([]uint64, 0, int(f.releasedCount))
	for _, extent := range f.releasedExtents {
		for pageNum := extent.start; ; pageNum++ {
			pages = append(pages, pageNum)
			if pageNum == extent.end {
				break
			}
		}
	}
	return pages, nil
}

func firstDifferentExtent(previous, current []pageExtent) int {
	shared := min(len(previous), len(current))
	for idx := 0; idx < shared; idx++ {
		if previous[idx] != current[idx] {
			return idx
		}
	}
	if len(previous) == len(current) {
		return -1
	}
	return shared
}

func recordPageIndex(recordIndex, firstCapacity, extraCapacity int) int {
	if recordIndex < firstCapacity {
		return 0
	}
	return 1 + (recordIndex-firstCapacity)/extraCapacity
}

func writeExtentRecords(page *Page, offset int, extents []pageExtent, startIndex, capacity int) {
	for idx := 0; idx < capacity && startIndex+idx < len(extents); idx++ {
		pos := offset + idx*freelistExtentRecordSize
		extent := extents[startIndex+idx]
		binary.LittleEndian.PutUint64(page.Data[pos:], extent.start)
		binary.LittleEndian.PutUint64(page.Data[pos+UInt64Size:], extent.end)
	}
}

func writeLegacyRecords(page *Page, offset int, pages []uint64, startIndex, capacity int) {
	for idx := 0; idx < capacity && startIndex+idx < len(pages); idx++ {
		pos := offset + idx*freelistLegacyRecordSize
		binary.LittleEndian.PutUint64(page.Data[pos:], pages[startIndex+idx])
	}
}

func BuildFreelistPages(dal *Dal, f *Freelist) ([]*Page, error) {
	if f == nil {
		return nil, fmt.Errorf("freelist is nil")
	}
	if f.maxPages != dal.maxPages {
		f.maxPages = dal.maxPages
		f.dirty = true
	}
	if len(f.freelistPages) == 0 {
		f.freelistPages = []uint64{dal.meta.freelistPageNumber}
		f.rebuildFreelistPageSet()
		f.dirty = true
	}
	if f.freelistPages[0] != dal.meta.freelistPageNumber {
		return nil, fmt.Errorf("%w: first freelist page is %d", ErrCorruptedFreelist, f.freelistPages[0])
	}
	if !f.dirty {
		return nil, nil
	}

	recordCount := len(f.releasedExtents)
	var legacyPages []uint64
	var err error
	if f.legacyPageRecords {
		legacyPages, err = legacyReleasedPages(f)
		if err != nil {
			return nil, err
		}
		recordCount = len(legacyPages)
	}
	if err = ensureFreelistPageCapacity(dal, f, recordCount); err != nil {
		return nil, err
	}

	dirtyFromPage := 1
	if f.legacyPageRecords || f.persistedState == nil {
		dirtyFromPage = 0
	} else if f.dirtyExtentIndex >= 0 {
		dirtyFromPage = recordPageIndex(f.dirtyExtentIndex, f.recordsPerFirst, f.recordsPerExtra)
	} else {
		dirtyFromPage = len(f.freelistPages)
	}
	if f.persistedState == nil || len(f.persistedState.freelistPages) != len(f.freelistPages) {
		oldLast := 0
		if f.persistedState != nil && len(f.persistedState.freelistPages) > 0 {
			oldLast = len(f.persistedState.freelistPages) - 1
		}
		dirtyFromPage = min(dirtyFromPage, oldLast)
	}

	pages := make([]*Page, 0, 1+max(0, len(f.freelistPages)-max(1, dirtyFromPage)))
	for pageIndex, pageNum := range f.freelistPages {
		if pageIndex > 0 && pageIndex < dirtyFromPage {
			continue
		}
		page := &Page{PageNumber: pageNum, Data: make([]byte, dal.meta.pageSize)}
		page.Data[freelistPageTypeOffset] = FreeListPage
		if pageIndex+1 < len(f.freelistPages) {
			binary.LittleEndian.PutUint64(page.Data[freelistNextPageOffset:], f.freelistPages[pageIndex+1])
		}
		if pageIndex == 0 {
			binary.LittleEndian.PutUint64(page.Data[freelistCurrentPageOffset:], f.currentPage)
			binary.LittleEndian.PutUint64(page.Data[freelistMaxPagesOffset:], f.maxPages)
			binary.LittleEndian.PutUint64(page.Data[freelistNumPagesOffset:], uint64(recordCount))
			if f.legacyPageRecords {
				writeLegacyRecords(page, freelistFirstPageEntriesOffset, legacyPages, 0, f.recordsPerFirst)
			} else {
				writeExtentRecords(page, freelistFirstPageEntriesOffset, f.releasedExtents, 0, f.recordsPerFirst)
			}
		} else {
			startIndex := f.recordsPerFirst + (pageIndex-1)*f.recordsPerExtra
			if f.legacyPageRecords {
				writeLegacyRecords(page, freelistExtraPageEntriesOffset, legacyPages, startIndex, f.recordsPerExtra)
			} else {
				writeExtentRecords(page, freelistExtraPageEntriesOffset, f.releasedExtents, startIndex, f.recordsPerExtra)
			}
		}
		if err = dal.preparePage(page); err != nil {
			return nil, err
		}
		pages = append(pages, page)
	}
	return pages, nil
}

func markFreelistPersisted(f *Freelist) {
	if f == nil {
		return
	}
	if !f.dirty && f.persistedState != nil {
		return
	}
	f.dirty = false
	f.dirtyExtentIndex = -1
	f.persistedState = &freelistPersistedState{
		freelistPages: f.freelistPages,
	}
}

func calculateFreelistCapacity(pageSize int) (int, int) {
	return calculateFreelistCapacityForFormat(pageSize, true)
}

func calculateFreelistCapacityForFormat(pageSize int, checksums bool) (int, int) {
	usable := pageSize
	recordSize := freelistExtentRecordSize
	if checksums {
		usable -= pageChecksumSize
	} else {
		recordSize = freelistLegacyRecordSize
	}
	return (usable - freelistFirstPageEntriesOffset) / recordSize,
		(usable - freelistExtraPageEntriesOffset) / recordSize
}
