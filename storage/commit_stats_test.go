package storage

import (
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCommitObserverLogsSingleAndBatchBreakdown(t *testing.T) {
	path := TempFileName(".db")
	var stats []CommitPhaseStats

	opts := DefaultOptions().WithCommitObserver(func(s CommitPhaseStats) {
		stats = append(stats, s)
	})

	db, err := Open(path, opts)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = db.Close()
		_ = os.Remove(path)
		_ = os.Remove(db.dal.opts.TxLogPath)
	})

	err = db.Update(func(tx *Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("bench"))
		if err != nil {
			return err
		}
		return bucket.Put([]byte("single"), []byte("value"))
	})
	require.NoError(t, err)

	value := bytes.Repeat([]byte("v"), 64)
	err = db.Update(func(tx *Tx) error {
		return writeBatch(tx, []byte("bench"), 0, 1000, value)
	})
	require.NoError(t, err)

	require.Len(t, stats, 2)

	single := stats[0]
	batch := stats[1]

	require.Positive(t, single.TotalDuration)
	require.Positive(t, single.TxLogWriteDuration)
	require.Positive(t, single.TxLogSyncDuration)
	require.Positive(t, single.DBWriteDuration)
	require.Positive(t, single.DBSyncDuration)
	require.Positive(t, single.JournalClearDuration)
	require.Positive(t, single.JournalPageCount)
	require.Positive(t, single.JournalBytes)

	require.Positive(t, batch.TotalDuration)
	require.Positive(t, batch.TxLogWriteDuration)
	require.Positive(t, batch.TxLogSyncDuration)
	require.Positive(t, batch.DBWriteDuration)
	require.Positive(t, batch.DBSyncDuration)
	require.Positive(t, batch.JournalClearDuration)
	require.Greater(t, batch.JournalPageCount, single.JournalPageCount)
	require.Greater(t, batch.JournalBytes, single.JournalBytes)

	t.Logf("single_write_tx: %s", single)
	t.Logf("batch_write_1000_tx: %s", batch)
}
