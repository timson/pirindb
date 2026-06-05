package storage

import (
	"bytes"
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"
)

func TestNewBlob(t *testing.T) {
	db, filename := CreateTestDB(t)

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
	CloseTestDB(t, db)

	// Now open created database
	db = OpenTestDB(t, filename, nil)
	tx = db.Begin(true)

	existingBlob, errRead := GetBlob(tx, pageNum)
	require.NoError(t, errRead)

	// check if blobs are equal
	require.True(t, reflect.DeepEqual(existingBlob.data, blob.data))

	// Node we want to delete blob and check if pages are release
	deletedLen, err := DeleteBlob(tx, pageNum)
	require.NoError(t, err)
	require.Equal(t, len(data), deletedLen)
	err = tx.Commit()
	require.NoError(t, err)

	require.Equal(t, len(db.dal.freelist.releasedPages), existingBlob.pageCount)
}

func TestBlobStreamReadWrite(t *testing.T) {
	db, filename := CreateTestDB(t)

	data := bytes.Repeat([]byte("stream-blob-"), 4096)

	tx := db.Begin(true)
	pageNum, err := SaveBlobFromReader(tx, bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	CloseTestDB(t, db)

	db = OpenTestDB(t, filename, nil)
	tx = db.Begin(false)

	size, err := BlobSize(tx, pageNum)
	require.NoError(t, err)
	require.Equal(t, len(data), size)

	var buf bytes.Buffer
	written, err := WriteBlobTo(tx, pageNum, &buf)
	require.NoError(t, err)
	require.Equal(t, int64(len(data)), written)
	require.Equal(t, data, buf.Bytes())

	tx.Rollback()
}
