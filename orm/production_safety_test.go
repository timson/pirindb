package orm

import (
	"github.com/stretchr/testify/require"
	"github.com/timson/pirindb/storage"
	"path/filepath"
	"testing"
)

func TestAbortedSaveDoesNotPublishIdentity(t *testing.T) {
	db, e := storage.Open(filepath.Join(t.TempDir(), "orm.db"), nil)
	require.NoError(t, e)
	defer db.Close()
	o := New(db)
	a := User{Login: "a"}
	bad := struct{ NoPK string }{}
	require.Error(t, o.Save(&a, &bad))
	require.Zero(t, a.ID)
	b := User{Login: "b"}
	require.NoError(t, o.Save(&b))
	require.EqualValues(t, 1, b.ID)
	require.NoError(t, o.Save(&a))
	require.EqualValues(t, 2, a.ID)
	require.NoError(t, db.View(func(tx *storage.Tx) error {
		bucket, e := tx.GetBucket([]byte("model/User"))
		if e != nil {
			return e
		}
		require.EqualValues(t, 2, bucket.ItemCount())
		return nil
	}))
}
