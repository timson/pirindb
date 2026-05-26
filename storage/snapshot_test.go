package storage

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSnapshotExportImportDBRoundTrip(t *testing.T) {
	db, _ := CreateTestDB(t)
	err := db.Update(func(tx *Tx) error {
		mainBucket, err := tx.CreateBucketIfNotExists([]byte("main"))
		if err != nil {
			return err
		}
		if err = mainBucket.Put([]byte("alpha"), []byte("one")); err != nil {
			return err
		}
		if err = mainBucket.Put([]byte("blob"), bytes.Repeat([]byte("x"), MaxValueSize+256)); err != nil {
			return err
		}

		usersBucket, err := tx.CreateBucketIfNotExists([]byte("users"))
		if err != nil {
			return err
		}
		if err = usersBucket.Put([]byte("1"), []byte("alice")); err != nil {
			return err
		}
		return usersBucket.Put([]byte("2"), []byte("bob"))
	})
	require.NoError(t, err)

	snapshotPath := filepath.Join(t.TempDir(), "db.snapshot")
	exportStats, err := ExportDBToPath(db, snapshotPath, false)
	require.NoError(t, err)
	require.Equal(t, uint64(2), exportStats.BucketsExported)
	require.Equal(t, uint64(4), exportStats.KeysExported)
	require.Greater(t, exportStats.BytesWritten, int64(0))

	importDB, _ := CreateTestDB(t)
	err = importDB.Update(func(tx *Tx) error {
		stale, err := tx.CreateBucketIfNotExists([]byte("stale"))
		if err != nil {
			return err
		}
		return stale.Put([]byte("legacy"), []byte("value"))
	})
	require.NoError(t, err)

	importStats, err := ImportDBFromPath(importDB, snapshotPath)
	require.NoError(t, err)
	require.Equal(t, uint64(2), importStats.BucketsImported)
	require.Equal(t, uint64(4), importStats.KeysImported)
	require.Greater(t, importStats.BytesRead, int64(0))

	requireBucketValue(t, importDB, []byte("main"), []byte("alpha"), []byte("one"))
	requireBucketValue(t, importDB, []byte("main"), []byte("blob"), bytes.Repeat([]byte("x"), MaxValueSize+256))
	requireBucketValue(t, importDB, []byte("users"), []byte("1"), []byte("alice"))
	requireBucketValue(t, importDB, []byte("users"), []byte("2"), []byte("bob"))
	requireBucketNotFound(t, importDB, []byte("stale"))
}

func TestSnapshotImportBucketReplacesTargetOnly(t *testing.T) {
	sourceDB, _ := CreateTestDB(t)
	err := sourceDB.Update(func(tx *Tx) error {
		usersBucket, err := tx.CreateBucketIfNotExists([]byte("users"))
		if err != nil {
			return err
		}
		if err = usersBucket.Put([]byte("1"), []byte("alice")); err != nil {
			return err
		}
		return usersBucket.Put([]byte("2"), []byte("bob"))
	})
	require.NoError(t, err)

	snapshotPath := filepath.Join(t.TempDir(), "bucket.snapshot")
	exportStats, err := ExportBucketToPath(sourceDB, []byte("users"), snapshotPath, false)
	require.NoError(t, err)
	require.Equal(t, uint64(1), exportStats.BucketsExported)
	require.Equal(t, uint64(2), exportStats.KeysExported)

	targetDB, _ := CreateTestDB(t)
	err = targetDB.Update(func(tx *Tx) error {
		restored, err := tx.CreateBucketIfNotExists([]byte("restored"))
		if err != nil {
			return err
		}
		if err = restored.Put([]byte("stale"), []byte("legacy")); err != nil {
			return err
		}
		other, err := tx.CreateBucketIfNotExists([]byte("other"))
		if err != nil {
			return err
		}
		return other.Put([]byte("safe"), []byte("value"))
	})
	require.NoError(t, err)

	importStats, err := ImportBucketFromPath(targetDB, []byte("restored"), snapshotPath)
	require.NoError(t, err)
	require.Equal(t, uint64(1), importStats.BucketsImported)
	require.Equal(t, uint64(2), importStats.KeysImported)

	requireBucketValue(t, targetDB, []byte("restored"), []byte("1"), []byte("alice"))
	requireBucketValue(t, targetDB, []byte("restored"), []byte("2"), []byte("bob"))
	requireBucketValue(t, targetDB, []byte("other"), []byte("safe"), []byte("value"))

	err = targetDB.View(func(tx *Tx) error {
		bucket, err := tx.GetBucket([]byte("restored"))
		require.NoError(t, err)
		_, found := bucket.Get([]byte("stale"))
		require.False(t, found)
		return nil
	})
	require.NoError(t, err)
}

func TestImportInvalidSnapshotDoesNotMutateDB(t *testing.T) {
	db, _ := CreateTestDB(t)
	err := db.Update(func(tx *Tx) error {
		usersBucket, err := tx.CreateBucketIfNotExists([]byte("users"))
		if err != nil {
			return err
		}
		return usersBucket.Put([]byte("id"), []byte("original"))
	})
	require.NoError(t, err)

	snapshotPath := filepath.Join(t.TempDir(), "invalid.snapshot")
	err = os.WriteFile(snapshotPath, []byte("not-a-snapshot"), 0o644)
	require.NoError(t, err)

	_, err = ImportDBFromPath(db, snapshotPath)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrSnapshotInvalid))

	requireBucketValue(t, db, []byte("users"), []byte("id"), []byte("original"))
}
