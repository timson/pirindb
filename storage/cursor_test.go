package storage

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/require"
	"reflect"
	"slices"
	"testing"
)

func TestCursorFirstNext(t *testing.T) {
	db, _ := CreateTestDB(t)

	iterations := 5000
	sourceKeys := make(map[string][]byte)
	readKeys := make(map[string][]byte)
	err := db.Update(func(tx *Tx) error {
		bucket, _ := tx.CreateBucket([]byte("foo"))
		for idx := range iterations {
			k := fmt.Sprintf("%05d", idx)
			err := bucket.Put([]byte(k), []byte(k))
			if err != nil {
				return err
			}
			sourceKeys[k] = []byte(k)
		}
		return nil
	})
	require.NoError(t, err)

	err = db.View(func(tx *Tx) error {
		bucket, _ := tx.GetBucket([]byte("foo"))
		cursor := bucket.Cursor()
		cnt := 0
		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			readKeys[string(k)] = v
			cnt++
		}
		require.Equal(t, iterations, cnt)
		require.True(t, reflect.DeepEqual(sourceKeys, readKeys))
		return nil
	})
	require.NoError(t, err)
}

func TestCursorPrefixScan(t *testing.T) {
	db, _ := CreateTestDB(t)

	iterations := 5000
	err := db.Update(func(tx *Tx) error {
		bucket, _ := tx.CreateBucket([]byte("foo"))
		for idx := range iterations {
			k := fmt.Sprintf("%05d", idx)
			err := bucket.Put([]byte(k), []byte(k))
			if err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	err = db.View(func(tx *Tx) error {
		bucket, _ := tx.GetBucket([]byte("foo"))
		cursor := bucket.Cursor()
		prefix := []byte("03")
		cnt := 0
		for k, _ := cursor.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = cursor.Next() {
			cnt++
		}
		require.Equal(t, 1000, cnt)
		return nil
	})
}

func TestCursorLastPrev(t *testing.T) {
	db, _ := CreateTestDB(t)

	iterations := 500
	original := make([]string, iterations)
	err := db.Update(func(tx *Tx) error {
		bucket, _ := tx.CreateBucket([]byte("foo"))
		for idx := range iterations {
			k := fmt.Sprintf("%04d", idx)
			original[idx] = k
			err := bucket.Put([]byte(k), []byte(k))
			if err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	reversedOriginal := slices.Clone(original)
	slices.Reverse(reversedOriginal)

	err = db.View(func(tx *Tx) error {
		bucket, _ := tx.GetBucket([]byte("foo"))
		cursor := bucket.Cursor()
		testSlices := make([]string, 0)
		for k, _ := cursor.Last(); k != nil; k, _ = cursor.Prev() {
			testSlices = append(testSlices, string(k))
		}
		require.True(t, reflect.DeepEqual(reversedOriginal, testSlices))
		return nil
	})

	reversedOriginal = reversedOriginal[200:]

	err = db.View(func(tx *Tx) error {
		bucket, _ := tx.GetBucket([]byte("foo"))
		cursor := bucket.Cursor()
		prefix := []byte("0299")
		testSlices := make([]string, 0)
		for k, _ := cursor.Seek(prefix); k != nil; k, _ = cursor.Prev() {
			testSlices = append(testSlices, string(k))
		}
		require.True(t, reflect.DeepEqual(reversedOriginal, testSlices))
		return nil
	})
}

func TestCursorSeekPastEndReturnsNil(t *testing.T) {
	db, _ := CreateTestDB(t)

	err := db.Update(func(tx *Tx) error {
		bucket, _ := tx.CreateBucket([]byte("foo"))
		if err := bucket.Put([]byte("a"), []byte("1")); err != nil {
			return err
		}
		if err := bucket.Put([]byte("b"), []byte("2")); err != nil {
			return err
		}
		return nil
	})
	require.NoError(t, err)

	err = db.View(func(tx *Tx) error {
		bucket, _ := tx.GetBucket([]byte("foo"))
		cursor := bucket.Cursor()
		k, v := cursor.Seek([]byte("z"))
		require.Nil(t, k)
		require.Nil(t, v)
		return nil
	})
	require.NoError(t, err)
}

func TestCursorPrevReturnsDecodedValue(t *testing.T) {
	db, _ := CreateTestDB(t)

	err := db.Update(func(tx *Tx) error {
		bucket, _ := tx.CreateBucket([]byte("foo"))
		if err := bucket.Put([]byte("a"), []byte("v1")); err != nil {
			return err
		}
		if err := bucket.Put([]byte("b"), []byte("v2")); err != nil {
			return err
		}
		return nil
	})
	require.NoError(t, err)

	err = db.View(func(tx *Tx) error {
		bucket, _ := tx.GetBucket([]byte("foo"))
		cursor := bucket.Cursor()
		_, _ = cursor.Last()
		k, v := cursor.Prev()
		require.Equal(t, []byte("a"), k)
		require.Equal(t, []byte("v1"), v)
		return nil
	})
	require.NoError(t, err)
}

func TestCursorSeekResetsState(t *testing.T) {
	db, _ := CreateTestDB(t)

	err := db.Update(func(tx *Tx) error {
		bucket, _ := tx.CreateBucket([]byte("foo"))
		for idx := 0; idx < 100; idx++ {
			k := fmt.Sprintf("%04d", idx)
			if err := bucket.Put([]byte(k), []byte(k)); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	err = db.View(func(tx *Tx) error {
		bucket, _ := tx.GetBucket([]byte("foo"))
		cursor := bucket.Cursor()

		// Move cursor state around before Seek.
		for k, _ := cursor.Last(); k != nil; k, _ = cursor.Prev() {
			if string(k) == "0075" {
				break
			}
		}

		k, v := cursor.Seek([]byte("0020"))
		require.Equal(t, []byte("0020"), k)
		require.Equal(t, []byte("0020"), v)

		k, v = cursor.Next()
		require.Equal(t, []byte("0021"), k)
		require.Equal(t, []byte("0021"), v)
		return nil
	})
	require.NoError(t, err)
}
