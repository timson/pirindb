package storage

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func verifyBTreeModel(t *testing.T, db *DB, model map[string][]byte) {
	t.Helper()
	keys := make([]string, 0, len(model))
	for key := range model {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	require.NoError(t, db.View(func(tx *Tx) error {
		bucket, err := tx.GetBucket([]byte("model"))
		if err != nil {
			return err
		}
		cursor := bucket.Cursor()
		idx := 0
		for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
			if idx >= len(keys) {
				return fmt.Errorf("cursor returned unexpected key %q", key)
			}
			expectedKey := keys[idx]
			if string(key) != expectedKey || !bytes.Equal(value, model[expectedKey]) {
				return fmt.Errorf("cursor mismatch at %d: got %q", idx, key)
			}
			idx++
		}
		if err = cursor.Err(); err != nil {
			return err
		}
		if idx != len(keys) {
			return fmt.Errorf("cursor returned %d keys, expected %d", idx, len(keys))
		}

		idx = len(keys) - 1
		for key, value := cursor.Last(); key != nil; key, value = cursor.Prev() {
			if idx < 0 {
				return fmt.Errorf("reverse cursor returned unexpected key %q", key)
			}
			expectedKey := keys[idx]
			if string(key) != expectedKey || !bytes.Equal(value, model[expectedKey]) {
				return fmt.Errorf("reverse cursor mismatch at %d: got %q", idx, key)
			}
			idx--
		}
		if err = cursor.Err(); err != nil {
			return err
		}
		if idx != -1 {
			return fmt.Errorf("reverse cursor stopped with %d keys remaining", idx+1)
		}
		return nil
	}))
	report, err := db.Check()
	require.NoError(t, err)
	require.Zero(t, report.OrphanPageCount)
}

func TestBTreeRandomizedVariableSizeMutationsSurviveReopen(t *testing.T) {
	path := TempFileName(".db")
	db, err := Open(path, nil)
	require.NoError(t, err)
	tlogPath := db.dal.opts.TxLogPath
	t.Cleanup(func() {
		_ = db.Close()
		_ = os.Remove(path)
		_ = os.Remove(tlogPath)
	})
	require.NoError(t, db.Update(func(tx *Tx) error {
		_, createErr := tx.CreateBucket([]byte("model"))
		return createErr
	}))

	rng := rand.New(rand.NewSource(0x504952494e))
	model := make(map[string][]byte)
	valueSizes := []int{0, 1, 17, 255, 700, 1000, MaxValueSize, MaxValueSize + 1, 5000}
	for batch := 0; batch < 20; batch++ {
		err = db.Update(func(tx *Tx) error {
			bucket, getErr := tx.GetBucket([]byte("model"))
			if getErr != nil {
				return getErr
			}
			for range 80 {
				key := fmt.Sprintf("key-%03d", rng.Intn(300))
				if rng.Intn(100) < 38 {
					if _, exists := model[key]; !exists {
						continue
					}
					if removeErr := bucket.Remove([]byte(key)); removeErr != nil {
						return removeErr
					}
					delete(model, key)
					continue
				}
				size := valueSizes[rng.Intn(len(valueSizes))]
				value := bytes.Repeat([]byte{byte(rng.Intn(251) + 1)}, size)
				if putErr := bucket.Put([]byte(key), value); putErr != nil {
					return putErr
				}
				model[key] = append([]byte(nil), value...)
			}
			return nil
		})
		require.NoError(t, err, "batch %d", batch)
		verifyBTreeModel(t, db, model)
		if batch%5 == 4 {
			require.NoError(t, db.Close())
			db, err = Open(path, nil)
			require.NoError(t, err)
			verifyBTreeModel(t, db, model)
		}
	}
}
