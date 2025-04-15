package storage

import (
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"
)

func TestNewBlob(t *testing.T) {
	db, filename := createTestDB(t)

	data := make([]byte, 18000)
	for idx := 0; idx < len(data); idx++ {
		data[idx] = byte(idx % 256)
	}

	tx := db.Begin(true)
	blob, err := NewBlob(data)
	require.NoError(t, err)

	pageNum, errSave := blob.Save(tx)
	require.NoError(t, errSave)
	t.Logf("New blob at page %d", pageNum)

	err = tx.Commit()
	require.NoError(t, err)
	closeTestDB(t, db)

	// Now open created database
	db = openTestDB(t, filename, nil)
	tx = db.Begin(true)

	existingBlob, errRead := GetBlob(tx, pageNum)
	require.NoError(t, errRead)

	// check if blobs are equal
	require.True(t, reflect.DeepEqual(existingBlob.data, blob.data))

	// Node we want to delete blob and check if pages are release
	_, err = DeleteBlob(tx, pageNum)
	require.NoError(t, err)
	err = tx.Commit()
	require.NoError(t, err)

	require.Equal(t, len(db.dal.freelist.releasedPages), existingBlob.pageCount)
}
