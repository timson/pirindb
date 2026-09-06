package main

import (
	"bytes"
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/timson/pirindb/storage"
)

func openRedisListPropertyDB(t *testing.T) *storage.DB {
	t.Helper()
	path := storage.TempFileName(".db")
	opts := storage.DefaultOptions().
		WithSyncPolicy(storage.SyncPolicyJournal).
		WithCheckpointTxThreshold(1024)
	db, err := storage.Open(path, opts)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = db.Close()
		_ = os.Remove(path)
		_ = os.Remove(db.GetOptions().TxLogPath)
	})
	return db
}

func modelRedisListRange(values [][]byte, start, stop int64) [][]byte {
	length := int64(len(values))
	if length == 0 {
		return nil
	}
	if start < 0 {
		start += length
	}
	if stop < 0 {
		stop += length
	}
	if start < 0 {
		start = 0
	}
	if stop < 0 || start >= length {
		return nil
	}
	if stop >= length {
		stop = length - 1
	}
	if start > stop {
		return nil
	}
	result := make([][]byte, stop-start+1)
	copy(result, values[start:stop+1])
	return result
}

func modelRedisListRemove(values [][]byte, count int64, element []byte) ([][]byte, int64) {
	remove := make([]bool, len(values))
	limit := count
	reverse := count < 0
	if count == 0 {
		limit = int64(len(values))
	} else if reverse {
		limit = -count
	}
	var removed int64
	if reverse {
		for i := len(values) - 1; i >= 0 && removed < limit; i-- {
			if bytes.Equal(values[i], element) {
				remove[i] = true
				removed++
			}
		}
	} else {
		for i := 0; i < len(values) && removed < limit; i++ {
			if bytes.Equal(values[i], element) {
				remove[i] = true
				removed++
			}
		}
	}
	remaining := make([][]byte, 0, len(values)-int(removed))
	for i, value := range values {
		if !remove[i] {
			remaining = append(remaining, value)
		}
	}
	return remaining, removed
}

func requireRedisListMatchesModel(t *testing.T, db *storage.DB, ns redisNamespace, key []byte, model [][]byte) {
	t.Helper()
	require.NoError(t, db.View(func(tx *storage.Tx) error {
		length, err := redisListLenTx(tx, ns, key, 0)
		if err != nil {
			return err
		}
		require.Equal(t, int64(len(model)), length)
		values, err := redisListRangeTx(tx, ns, key, 0, -1, 0)
		if err != nil {
			return err
		}
		if len(values) == 0 && len(model) == 0 {
			return nil
		}
		require.Equal(t, model, values)

		meta, found, err := loadRedisListMetaTx(tx, ns, key)
		if err != nil || !found {
			return err
		}
		visited := make(map[uint64]struct{})
		var counted uint64
		previousID := uint64(0)
		segmentID := meta.HeadSegID
		for segmentID != 0 {
			require.NotContains(t, visited, segmentID)
			visited[segmentID] = struct{}{}
			segment, loadErr := loadRedisListSegmentTx(tx, ns, meta, segmentID)
			if loadErr != nil {
				return loadErr
			}
			require.Equal(t, previousID, segment.PrevID)
			require.NotEmpty(t, segment.Values)
			counted += uint64(len(segment.Values))
			previousID = segmentID
			segmentID = segment.NextID
		}
		require.Equal(t, meta.TailSegID, previousID)
		require.Equal(t, meta.Length, counted)
		return nil
	}))
}

func TestRedisListSegmentAlgorithmsAgainstSliceModel(t *testing.T) {
	db := openRedisListPropertyDB(t)
	ns := redisNamespaceForDB(0)
	key := []byte("property-list")
	rng := rand.New(rand.NewSource(0x5eed))
	model := make([][]byte, 0, 256)

	initial := make([][]byte, 150)
	for i := range initial {
		initial[i] = []byte{byte(i % 11), byte(i)}
		model = append(model, cloneBytes(initial[i]))
	}
	require.NoError(t, db.Update(func(tx *storage.Tx) error {
		_, err := redisPushTx(tx, ns, key, initial, false, 0)
		return err
	}))

	for step := 0; step < 500; step++ {
		switch rng.Intn(7) {
		case 0:
			value := []byte{byte(step), byte(rng.Intn(13))}
			require.NoError(t, db.Update(func(tx *storage.Tx) error {
				_, err := redisPushTx(tx, ns, key, [][]byte{value}, true, 0)
				return err
			}))
			model = append([][]byte{cloneBytes(value)}, model...)
		case 1:
			value := []byte{byte(step), byte(rng.Intn(17))}
			require.NoError(t, db.Update(func(tx *storage.Tx) error {
				_, err := redisPushTx(tx, ns, key, [][]byte{value}, false, 0)
				return err
			}))
			model = append(model, cloneBytes(value))
		case 2:
			left := rng.Intn(2) == 0
			var got []byte
			var found bool
			require.NoError(t, db.Update(func(tx *storage.Tx) error {
				var err error
				got, found, err = redisPopTx(tx, ns, key, left, 0)
				return err
			}))
			if len(model) == 0 {
				require.False(t, found)
			} else if left {
				require.True(t, found)
				require.Equal(t, model[0], got)
				model = model[1:]
			} else {
				require.True(t, found)
				require.Equal(t, model[len(model)-1], got)
				model = model[:len(model)-1]
			}
		case 3:
			if len(model) == 0 {
				continue
			}
			index := rng.Intn(len(model))
			value := []byte("replacement")
			if step%29 == 0 {
				value = bytes.Repeat([]byte("x"), redisListTargetSegmentPayloadSize+step+1)
			}
			require.NoError(t, db.Update(func(tx *storage.Tx) error {
				return redisListSetTx(tx, ns, key, int64(index), value, 0)
			}))
			model[index] = cloneBytes(value)
		case 4:
			if len(model) == 0 {
				continue
			}
			span := int64(len(model) + 10)
			start := int64(rng.Intn(int(span*2+1))) - span
			stop := int64(rng.Intn(int(span*2+1))) - span
			require.NoError(t, db.Update(func(tx *storage.Tx) error {
				return redisListTrimTx(tx, ns, key, start, stop, 0)
			}))
			model = modelRedisListRange(model, start, stop)
		case 5:
			if len(model) == 0 {
				continue
			}
			element := cloneBytes(model[rng.Intn(len(model))])
			count := int64(rng.Intn(5) - 2)
			expected, expectedRemoved := modelRedisListRemove(model, count, element)
			var removed int64
			require.NoError(t, db.Update(func(tx *storage.Tx) error {
				var err error
				removed, err = redisListRemTx(tx, ns, key, count, element, 0)
				return err
			}))
			require.Equal(t, expectedRemoved, removed)
			model = expected
		case 6:
			if len(model) == 0 {
				continue
			}
			index := rng.Intn(len(model))
			require.NoError(t, db.View(func(tx *storage.Tx) error {
				value, found, err := redisListIndexTx(tx, ns, key, int64(index), 0)
				require.True(t, found)
				require.Equal(t, model[index], value)
				return err
			}))
		}
		requireRedisListMatchesModel(t, db, ns, key, model)
	}
}
