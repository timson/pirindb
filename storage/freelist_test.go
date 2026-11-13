package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func allocateAndReleasePages(t *testing.T, db *DB, count int) {
	t.Helper()
	pages := make([]uint64, 0, count)
	for range count {
		page, err := db.dal.AllocatePage()
		require.NoError(t, err)
		pages = append(pages, page.PageNumber)
	}
	for _, pageNum := range pages {
		require.NoError(t, db.dal.ReleasePage(pageNum))
	}
}

func allocateFragmentedReleasedPages(t *testing.T, db *DB, extentCount int) {
	t.Helper()
	pages := make([]uint64, 0, extentCount*2)
	for range extentCount * 2 {
		page, err := db.dal.AllocatePage()
		require.NoError(t, err)
		pages = append(pages, page.PageNumber)
	}
	for idx := 0; idx < len(pages); idx += 2 {
		require.NoError(t, db.dal.ReleasePage(pages[idx]))
	}
}

func TestFreelistExtentWriteRead(t *testing.T) {
	db, filename := CreateTestDB(t)
	allocateAndReleasePages(t, db, 1800)
	require.Len(t, db.dal.freelist.releasedExtents, 1)
	require.EqualValues(t, 1800, db.dal.freelist.releasedPageN())
	require.NoError(t, WriteFreelist(db.dal, db.dal.freelist))

	expectedPages := append([]uint64(nil), db.dal.freelist.freelistPages...)
	expectedExtents := append([]pageExtent(nil), db.dal.freelist.releasedExtents...)
	CloseTestDB(t, db)

	db = OpenTestDB(t, filename, nil)
	require.Equal(t, expectedPages, db.dal.freelist.freelistPages)
	require.Equal(t, expectedExtents, db.dal.freelist.releasedExtents)
	require.EqualValues(t, 1800, db.dal.freelist.releasedPageN())
}

func TestFreelistBootstrappedOnFreshDB(t *testing.T) {
	db, filename := CreateTestDB(t)
	CloseTestDB(t, db)
	db = OpenTestDB(t, filename, nil)

	require.EqualValues(t, rootPageNumber, db.dal.freelist.currentPage)
	require.Equal(t, db.dal.maxPages, db.dal.freelist.maxPages)
	require.Equal(t, []uint64{freelistPageNumber}, db.dal.freelist.freelistPages)
	require.Zero(t, db.dal.freelist.releasedPageN())
	require.Empty(t, db.dal.freelist.releasedExtents)
	page, err := db.dal.GetPage(freelistPageNumber)
	require.NoError(t, err)
	require.Equal(t, byte(FreeListPage), page.Data[freelistPageTypeOffset])
}

func TestFreelistPersistsFragmentedExtentsAcrossMultiplePages(t *testing.T) {
	db, filename := CreateTestDB(t)
	firstCap, extraCap := calculateFreelistCapacity(BTreePageSize)
	extentCount := firstCap + extraCap + 17
	allocateFragmentedReleasedPages(t, db, extentCount)
	require.Len(t, db.dal.freelist.releasedExtents, extentCount)
	require.NoError(t, WriteFreelist(db.dal, db.dal.freelist))
	require.GreaterOrEqual(t, len(db.dal.freelist.freelistPages), 3)
	expected := append([]pageExtent(nil), db.dal.freelist.releasedExtents...)

	CloseTestDB(t, db)
	db = OpenTestDB(t, filename, nil)
	require.Equal(t, expected, db.dal.freelist.releasedExtents)
	require.EqualValues(t, extentCount, db.dal.freelist.releasedPageN())
}

func TestFreelistGetConsecutivePageNumbersFromReleasedRun(t *testing.T) {
	freelist := NewFreelist(BTreePageSize, 200)
	freelist.currentPage = 120
	for _, pageNum := range []uint64{10, 11, 12, 30, 31} {
		freelist.ReleasePage(pageNum)
	}

	pageNums, err := freelist.GetConsecutivePageNumbers(3)
	require.NoError(t, err)
	require.Equal(t, []uint64{10, 11, 12}, pageNums)
	require.False(t, freelist.containsReleasedPage(10))
	require.False(t, freelist.containsReleasedPage(11))
	require.False(t, freelist.containsReleasedPage(12))
	require.EqualValues(t, 2, freelist.releasedPageN())
}

func TestFreelistGetConsecutivePageNumbersFromHighWatermark(t *testing.T) {
	freelist := NewFreelist(BTreePageSize, 50)
	freelist.currentPage = 7
	pageNums, err := freelist.GetConsecutivePageNumbers(4)
	require.NoError(t, err)
	require.Equal(t, []uint64{8, 9, 10, 11}, pageNums)
	require.EqualValues(t, 11, freelist.currentPage)
}

func TestFreelistRepeatedAllocationShrinksExtentWithoutRebuild(t *testing.T) {
	freelist := NewFreelist(BTreePageSize, 400)
	freelist.currentPage = 250
	for pageNum := uint64(200); pageNum <= 220; pageNum++ {
		freelist.ReleasePage(pageNum)
	}
	require.Equal(t, []pageExtent{{start: 200, end: 220}}, freelist.releasedExtents)

	pageNums, err := freelist.GetConsecutivePageNumbers(5)
	require.NoError(t, err)
	require.Equal(t, []uint64{216, 217, 218, 219, 220}, pageNums)
	require.Equal(t, []pageExtent{{start: 200, end: 215}}, freelist.releasedExtents)

	pageNums, err = freelist.GetConsecutivePageNumbers(6)
	require.NoError(t, err)
	require.Equal(t, []uint64{210, 211, 212, 213, 214, 215}, pageNums)
	require.Equal(t, []pageExtent{{start: 200, end: 209}}, freelist.releasedExtents)
}

func TestFreelistTailExtentUpdateAvoidsFullRewrite(t *testing.T) {
	db, _ := CreateTestDB(t)
	firstCap, extraCap := calculateFreelistCapacity(BTreePageSize)
	allocateFragmentedReleasedPages(t, db, firstCap+2*extraCap+32)
	require.NoError(t, WriteFreelist(db.dal, db.dal.freelist))
	require.GreaterOrEqual(t, len(db.dal.freelist.freelistPages), 4)

	_, err := db.dal.AllocatePage()
	require.NoError(t, err)
	writes := 0
	db.dal.beforeSetPageHook = func(page *Page) error {
		if page.Data[freelistPageTypeOffset] == FreeListPage {
			writes++
		}
		return nil
	}
	require.NoError(t, WriteFreelist(db.dal, db.dal.freelist))
	db.dal.beforeSetPageHook = nil
	require.Greater(t, writes, 0)
	require.LessOrEqual(t, writes, 2)
}

func TestFreelistMiddleExtentIncrementalRewriteSurvivesReopen(t *testing.T) {
	db, filename := CreateTestDB(t)
	firstCap, extraCap := calculateFreelistCapacity(BTreePageSize)
	allocateFragmentedReleasedPages(t, db, firstCap+2*extraCap+32)
	require.NoError(t, WriteFreelist(db.dal, db.dal.freelist))
	require.NoError(t, db.dal.Sync())

	targetIndex := firstCap + extraCap/2
	target := db.dal.freelist.releasedExtents[targetIndex].start
	require.NoError(t, db.dal.freelist.removeReleasedRange(target, 1))
	require.NoError(t, WriteFreelist(db.dal, db.dal.freelist))
	require.NoError(t, db.dal.Sync())
	require.NoError(t, db.Close())

	db = OpenTestDB(t, filename, nil)
	require.False(t, db.dal.freelist.containsReleasedPage(target))
	require.NoError(t, db.Close())
}

func TestFreelistMillionPageRunUsesSingleExtent(t *testing.T) {
	freelist := NewFreelist(BTreePageSize, 1_100_000)
	freelist.currentPage = 1_000_100
	for pageNum := uint64(100); pageNum < 1_000_100; pageNum++ {
		freelist.ReleasePage(pageNum)
	}
	require.EqualValues(t, 1_000_000, freelist.releasedPageN())
	require.Equal(t, []pageExtent{{start: 100, end: 1_000_099}}, freelist.releasedExtents)
}

func TestFreelistChainGrowthDetachesCommitSnapshot(t *testing.T) {
	f := newFreelist(BTreePageSize, 4096, true)
	f.currentPage = 2048
	records := f.recordsPerFirst + 1
	f.releasedExtents = make([]pageExtent, records)
	for idx := range f.releasedExtents {
		pageNum := uint64(rootPageNumber + 2 + idx*2)
		f.releasedExtents[idx] = pageExtent{start: pageNum, end: pageNum}
	}
	f.releasedCount = uint64(records)
	f.freelistPages = []uint64{freelistPageNumber}
	f.rebuildFreelistPageSet()
	snapshot := cloneFreelist(f)

	dal := &Dal{meta: NewMeta(BTreePageSize), maxPages: 4096, pageChecksums: true}
	require.NoError(t, ensureFreelistPageCapacity(dal, f, records))
	require.Len(t, f.freelistPages, 2)
	require.Len(t, snapshot.freelistPages, 1)

	f.releasedExtents[0].start++
	require.NotEqual(t, f.releasedExtents[0], snapshot.releasedExtents[0])
	f.freelistPageSet[f.freelistPages[1]] = struct{}{}
	_, leaked := snapshot.freelistPageSet[f.freelistPages[1]]
	require.False(t, leaked)
}
