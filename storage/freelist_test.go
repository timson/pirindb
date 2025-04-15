package storage

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFreelistWriteRead(t *testing.T) {
	db, filename := createTestDB(t)

	releasedPageSize := 10000
	// Allocate new freelist, add releasedPages
	freelist := NewFreelist(BTreePageSize, uint64(releasedPageSize))
	freelist.releasedPages = make([]uint64, releasedPageSize)
	for i := 0; i < releasedPageSize; i++ {
		freelist.releasedPages[i] = uint64(i)
	}
	// Flush it to the disk
	freelist.dirty = true
	err := WriteFreelist(db.dal, freelist)
	closeTestDB(t, db)

	db = openTestDB(t, filename, nil)
	// Truncated releasedPages
	freelist.releasedPages = db.dal.freelist.releasedPages[:3000]
	// Flush to the disk once again
	freelist.dirty = true
	err = WriteFreelist(db.dal, freelist)
	require.NoError(t, err)
	closeTestDB(t, db)

	db = openTestDB(t, filename, nil)
	require.NoError(t, err)
	require.True(t, reflect.DeepEqual(freelist.freelistPages, db.dal.freelist.freelistPages))
	require.True(t, reflect.DeepEqual(freelist.releasedPages, db.dal.freelist.releasedPages))
}
