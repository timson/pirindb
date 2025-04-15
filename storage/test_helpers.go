package storage

import (
	"github.com/stretchr/testify/require"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
)

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func TempFileName(suffix string) string {
	return filepath.Join(os.TempDir(), uuid.New().String()+suffix)
}

func createTestDB(t *testing.T) (*DB, string) {
	tempFilename := TempFileName(".db")

	db, err := Open(tempFilename, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		err = db.Close()
		if err != nil {
			t.Fatal(err)
		}
		_ = os.Remove(tempFilename)
		_ = os.Remove(db.dal.opts.TxLogPath)
	})
	return db, tempFilename
}

func openTestDB(t *testing.T, filename string, opts *Options) *DB {
	db, err := Open(filename, opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func closeTestDB(t *testing.T, db *DB) {
	err := db.Close()
	require.NoError(t, err)
}

func (node *BNode) itemsToKeys() [][]byte {
	keys := make([][]byte, len(node.items))
	for i, item := range node.items {
		keys[i] = item.Key
	}
	return keys
}
