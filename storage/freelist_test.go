package storage

import (
	"reflect"
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
