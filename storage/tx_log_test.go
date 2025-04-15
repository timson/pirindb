package storage

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"
)

func Test_Recovery_AfterSimulatedCrash(t *testing.T) {
	pattern := []byte("1234")
	checkFunc := func(db *DB) error {
		return db.View(func(tx *Tx) error {
			usersBucket, err := tx.GetBucket([]byte("users"))
			if err != nil {
				return err
			}
			value, found := usersBucket.Get([]byte("id"))
			if !found {
				return fmt.Errorf("found id key, but should not be")
			}
			if !reflect.DeepEqual(value, pattern) {
				return fmt.Errorf("%v != %v", value, pattern)
			}
			t.Logf("read key from users bucket: %s", string(value))
			return nil
		})
	}

	db, filename := createTestDB(t)
	tx := db.Begin(true)
	bucket, err := tx.CreateBucketIfNotExists([]byte("users"))
	require.NoError(t, err)
	err = bucket.Put([]byte("id"), []byte("1234"))
	require.NoError(t, err)
	// Inject failure after txLog writes are done
	db.dal.beforeSetPageHook = func(p *Page) error {
		if !db.dal.txLog.active {
			return fmt.Errorf("unable to write pages to db")
		}
		return nil
	}

	err = tx.Commit() // This will crash
	require.Error(t, err)
	t.Logf("trying to fetch id key from users bucket")
	err = checkFunc(db)
	require.Error(t, err)
	db.Close()

	t.Log("reopen DB without recovery")
	db = openTestDB(t, filename, DefaultOptions().WithRecovery(false))
	err = checkFunc(db)
	t.Log(err)
	require.Error(t, err)
	db.Close()

	t.Log("reopen DB with recovery")
	db = openTestDB(t, filename, DefaultOptions().WithRecovery(true))
	err = checkFunc(db)
	require.NoError(t, err)
}
