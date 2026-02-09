package storage

import (
	"reflect"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func allocateAndReleasePages(t *testing.T, db *DB, count int) {
	pages := make([]uint64, 0, count)
	for i := 0; i < count; i++ {
		page, err := db.dal.AllocatePage()
		require.NoError(t, err)
		pages = append(pages, page.PageNumber)
	}
	for _, pageNum := range pages {
		require.NoError(t, db.dal.ReleasePage(pageNum))
	}
}

func TestFreelistWriteRead(t *testing.T) {
	db, filename := CreateTestDB(t)

	allocateAndReleasePages(t, db, 1800)
	require.NoError(t, WriteFreelist(db.dal, db.dal.freelist))

	expectedFreelistPages := append([]uint64(nil), db.dal.freelist.freelistPages...)
	expectedReleasedPages := append([]uint64(nil), db.dal.freelist.releasedPages...)

	CloseTestDB(t, db)

	db = OpenTestDB(t, filename, nil)
	require.True(t, reflect.DeepEqual(expectedFreelistPages, db.dal.freelist.freelistPages))
	require.True(t, reflect.DeepEqual(expectedReleasedPages, db.dal.freelist.releasedPages))

	// Truncate released pages and persist once again.
	db.dal.freelist.releasedPages = append([]uint64(nil), db.dal.freelist.releasedPages[:300]...)
	db.dal.freelist.dirty = true
	require.NoError(t, WriteFreelist(db.dal, db.dal.freelist))

	expectedFreelistPages = append([]uint64(nil), db.dal.freelist.freelistPages...)
	expectedReleasedPages = append([]uint64(nil), db.dal.freelist.releasedPages...)
	CloseTestDB(t, db)

	db = OpenTestDB(t, filename, nil)
	require.True(t, reflect.DeepEqual(expectedFreelistPages, db.dal.freelist.freelistPages))
	require.True(t, reflect.DeepEqual(expectedReleasedPages, db.dal.freelist.releasedPages))
}

func TestFreelistBootstrappedOnFreshDB(t *testing.T) {
	db, filename := CreateTestDB(t)
	CloseTestDB(t, db)

	db = OpenTestDB(t, filename, nil)
	require.EqualValues(t, rootPageNumber, db.dal.freelist.currentPage)
	require.Equal(t, db.dal.maxPages, db.dal.freelist.maxPages)
	require.Equal(t, []uint64{freelistPageNumber}, db.dal.freelist.freelistPages)
	require.Empty(t, db.dal.freelist.releasedPages)

	page, err := db.dal.GetPage(freelistPageNumber)
	require.NoError(t, err)
	require.Equal(t, byte(FreeListPage), page.Data[freelistPageTypeOffset])
}

func TestFreelistShrinkBoundaryKeepsConsistency(t *testing.T) {
	db, filename := CreateTestDB(t)

	firstCap, extraCap := calculateFreelistCapacity(BTreePageSize)
	twoPageCapacity := firstCap + extraCap

	// Ensure enough valid page numbers so we can hit a 3-page freelist boundary.
	allocateAndReleasePages(t, db, twoPageCapacity+32)
	require.NoError(t, WriteFreelist(db.dal, db.dal.freelist))
	require.Equal(t, 3, len(db.dal.freelist.freelistPages))

	CloseTestDB(t, db)
	db = OpenTestDB(t, filename, nil)

	// Exact 2-page capacity: shrinking from 3->2 would force one released freelist page,
	// which would require 3 pages again. The freelist must remain consistent.
	db.dal.freelist.releasedPages = append([]uint64(nil), db.dal.freelist.releasedPages[:twoPageCapacity]...)
	db.dal.freelist.dirty = true
	require.NoError(t, WriteFreelist(db.dal, db.dal.freelist))
	require.Equal(t, 3, len(db.dal.freelist.freelistPages))

	CloseTestDB(t, db)
	db = OpenTestDB(t, filename, nil)
	require.Equal(t, twoPageCapacity, len(db.dal.freelist.releasedPages))
	require.Equal(t, 3, len(db.dal.freelist.freelistPages))
}

func TestFreelistGetConsecutivePageNumbersFromReleasedRun(t *testing.T) {
	freelist := NewFreelist(BTreePageSize, 200)
	freelist.currentPage = 120

	freelist.ReleasePage(10)
	freelist.ReleasePage(11)
	freelist.ReleasePage(12)
	freelist.ReleasePage(30)
	freelist.ReleasePage(31)

	pageNums, err := freelist.GetConsecutivePageNumbers(3)
	require.NoError(t, err)
	require.Equal(t, []uint64{10, 11, 12}, pageNums)

	_, exists := freelist.releasedPagesSet[10]
	require.False(t, exists)
	_, exists = freelist.releasedPagesSet[11]
	require.False(t, exists)
	_, exists = freelist.releasedPagesSet[12]
	require.False(t, exists)
}

func TestFreelistGetConsecutivePageNumbersFromHighWatermark(t *testing.T) {
	freelist := NewFreelist(BTreePageSize, 50)
	freelist.currentPage = 7

	pageNums, err := freelist.GetConsecutivePageNumbers(4)
	require.NoError(t, err)
	require.Equal(t, []uint64{8, 9, 10, 11}, pageNums)
	require.EqualValues(t, 11, freelist.currentPage)
}

func TestFreelistGetConsecutivePageNumbersRepeatedFromSameExtent(t *testing.T) {
	freelist := NewFreelist(BTreePageSize, 400)
	freelist.currentPage = 250
	for pageNum := uint64(200); pageNum <= 220; pageNum++ {
		freelist.ReleasePage(pageNum)
	}

	pageNums, err := freelist.GetConsecutivePageNumbers(5)
	require.NoError(t, err)
	require.Equal(t, []uint64{216, 217, 218, 219, 220}, pageNums)
	require.False(t, freelist.releasedExtentsDirty)
	require.Len(t, freelist.releasedExtents, 1)
	require.EqualValues(t, 200, freelist.releasedExtents[0].start)
	require.EqualValues(t, 215, freelist.releasedExtents[0].end)

	pageNums, err = freelist.GetConsecutivePageNumbers(6)
	require.NoError(t, err)
	require.Equal(t, []uint64{210, 211, 212, 213, 214, 215}, pageNums)
	require.False(t, freelist.releasedExtentsDirty)
	require.Len(t, freelist.releasedExtents, 1)
	require.EqualValues(t, 200, freelist.releasedExtents[0].start)
	require.EqualValues(t, 209, freelist.releasedExtents[0].end)
}

func TestFreelistWriteTailUpdateAvoidsFullRewrite(t *testing.T) {
	db, _ := CreateTestDB(t)
	firstCap, extraCap := calculateFreelistCapacity(BTreePageSize)
	allocateAndReleasePages(t, db, firstCap+extraCap+32)
	require.NoError(t, WriteFreelist(db.dal, db.dal.freelist))
	require.GreaterOrEqual(t, len(db.dal.freelist.freelistPages), 3)

	_, err := db.dal.AllocatePage()
	require.NoError(t, err)

	freelistWriteCount := 0
	db.dal.beforeSetPageHook = func(p *Page) error {
		if len(p.Data) > 0 && p.Data[freelistPageTypeOffset] == FreeListPage {
			freelistWriteCount++
		}
		return nil
	}
	require.NoError(t, WriteFreelist(db.dal, db.dal.freelist))
	db.dal.beforeSetPageHook = nil

	require.Greater(t, freelistWriteCount, 0)
	require.Less(t, freelistWriteCount, len(db.dal.freelist.freelistPages))
}

func TestFreelistWriteSortsReleasedPagesAndKeepsTailWrite(t *testing.T) {
	db, _ := CreateTestDB(t)
	firstCap, extraCap := calculateFreelistCapacity(BTreePageSize)
	allocateAndReleasePages(t, db, firstCap+2*extraCap+64)
	require.NoError(t, WriteFreelist(db.dal, db.dal.freelist))
	require.GreaterOrEqual(t, len(db.dal.freelist.freelistPages), 4)
	require.True(t, sort.SliceIsSorted(db.dal.freelist.releasedPages, func(i, j int) bool {
		return db.dal.freelist.releasedPages[i] < db.dal.freelist.releasedPages[j]
	}))

	for idx := 0; idx+1 < len(db.dal.freelist.releasedPages); idx += 2 {
		db.dal.freelist.releasedPages[idx], db.dal.freelist.releasedPages[idx+1] = db.dal.freelist.releasedPages[idx+1], db.dal.freelist.releasedPages[idx]
	}
	db.dal.freelist.releasedPageIndex = nil
	db.dal.freelist.releasedExtentsDirty = true
	db.dal.freelist.releasedPagesDirty = true
	db.dal.freelist.dirty = true

	_, err := db.dal.freelist.GetConsecutivePageNumbers(32)
	require.NoError(t, err)

	freelistWriteCount := 0
	db.dal.beforeSetPageHook = func(p *Page) error {
		if len(p.Data) > 0 && p.Data[freelistPageTypeOffset] == FreeListPage {
			freelistWriteCount++
		}
		return nil
	}
	require.NoError(t, WriteFreelist(db.dal, db.dal.freelist))
	db.dal.beforeSetPageHook = nil

	require.True(t, sort.SliceIsSorted(db.dal.freelist.releasedPages, func(i, j int) bool {
		return db.dal.freelist.releasedPages[i] < db.dal.freelist.releasedPages[j]
	}))
	require.Greater(t, freelistWriteCount, 0)
	require.LessOrEqual(t, freelistWriteCount, 2)
}
