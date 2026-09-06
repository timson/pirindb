package storage

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/require"
	"os"
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

	db, filename := CreateTestDB(t)
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
	db = OpenTestDB(t, filename, DefaultOptions().WithRecovery(false))
	err = checkFunc(db)
	t.Log(err)
	require.Error(t, err)
	db.Close()

	t.Log("reopen DB with recovery")
	db = OpenTestDB(t, filename, DefaultOptions().WithRecovery(true))
	err = checkFunc(db)
	require.NoError(t, err)
}

func TestTxLogRecoverRejectsOffsetPageMismatch(t *testing.T) {
	logPath := TempFileName(".tlog")
	txLog := NewTxLog(logPath, 0600, BTreePageSize)
	require.NotNil(t, txLog.file)
	t.Cleanup(func() {
		_ = txLog.file.Close()
		_ = os.Remove(logPath)
	})

	page := &Page{
		PageNumber: 5,
		Data:       make([]byte, BTreePageSize),
	}

	err := txLog.With(func() error {
		// Offset points to page 4, while PageNumber is 5.
		return txLog.writePage(uint64(4*BTreePageSize), page)
	})
	require.NoError(t, err)

	err = txLog.Recover(func(offset uint64, page *Page) error {
		return nil
	})
	require.ErrorContains(t, err, "offset/page mismatch")
}

func TestRecovery_ReplaysStaleCommittedJournalIdempotently(t *testing.T) {
	db, filename := CreateTestDB(t)

	var staleJournal []byte
	captured := false
	db.dal.beforeSetPageHook = func(_ *Page) error {
		if db.dal.txLog.active || captured {
			return nil
		}
		data, err := os.ReadFile(db.dal.opts.TxLogPath)
		require.NoError(t, err)
		staleJournal = append([]byte(nil), data...)
		captured = true
		return nil
	}

	err := db.Update(func(tx *Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("users"))
		require.NoError(t, err)
		return bucket.Put([]byte("id"), []byte("committed"))
	})
	require.NoError(t, err)
	require.NotEmpty(t, staleJournal)

	journalFile, err := os.OpenFile(db.dal.opts.TxLogPath, os.O_WRONLY|os.O_TRUNC, 0600)
	require.NoError(t, err)
	_, err = journalFile.Write(staleJournal)
	require.NoError(t, err)
	require.NoError(t, journalFile.Sync())
	require.NoError(t, journalFile.Close())

	CloseTestDB(t, db)
	db = OpenTestDB(t, filename, DefaultOptions().WithRecovery(true))
	requireBucketValue(t, db, []byte("users"), []byte("id"), []byte("committed"))
	require.False(t, txLogActive(t, db))

	CloseTestDB(t, db)
	db = OpenTestDB(t, filename, DefaultOptions().WithRecovery(true))
	requireBucketValue(t, db, []byte("users"), []byte("id"), []byte("committed"))
	require.False(t, txLogActive(t, db))
}

func TestTxLogRecoversWithOneCorruptedMirroredHeader(t *testing.T) {
	logPath := TempFileName(".tlog")
	t.Cleanup(func() { _ = os.Remove(logPath) })
	txLog := NewTxLog(logPath, 0600, BTreePageSize)
	require.NoError(t, txLog.With(func() error {
		page := &Page{PageNumber: 4, Data: make([]byte, BTreePageSize)}
		page.Data[100] = 0x7a
		return txLog.writePage(uint64(4*BTreePageSize), page)
	}))
	require.NoError(t, txLog.Close())

	file, err := os.OpenFile(logPath, os.O_RDWR, 0)
	require.NoError(t, err)
	_, err = file.WriteAt([]byte("X"), txLogMagicOffset)
	require.NoError(t, err)
	require.NoError(t, file.Sync())
	require.NoError(t, file.Close())

	txLog = NewTxLog(logPath, 0600, BTreePageSize)
	defer txLog.Close()
	recovered := 0
	err = txLog.Recover(func(offset uint64, page *Page) error {
		require.EqualValues(t, 4*BTreePageSize, offset)
		require.EqualValues(t, 4, page.PageNumber)
		require.Equal(t, byte(0x7a), page.Data[100])
		recovered++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, recovered)
}

func TestTxLogRejectsWhenBothMirroredHeadersAreCorrupted(t *testing.T) {
	logPath := TempFileName(".tlog")
	t.Cleanup(func() { _ = os.Remove(logPath) })
	txLog := NewTxLog(logPath, 0600, BTreePageSize)
	require.NoError(t, txLog.With(func() error {
		return txLog.writePage(uint64(3*BTreePageSize), &Page{PageNumber: 3, Data: make([]byte, BTreePageSize)})
	}))
	require.NoError(t, txLog.Close())

	file, err := os.OpenFile(logPath, os.O_RDWR, 0)
	require.NoError(t, err)
	_, err = file.WriteAt([]byte("X"), txLogMagicOffset)
	require.NoError(t, err)
	_, err = file.WriteAt([]byte("Y"), int64(txLogHeaderSize+txLogMagicOffset))
	require.NoError(t, err)
	require.NoError(t, file.Sync())
	require.NoError(t, file.Close())

	txLog = NewTxLog(logPath, 0600, BTreePageSize)
	defer txLog.Close()
	err = txLog.Recover(func(uint64, *Page) error { return nil })
	require.Error(t, err)
}

func TestTxLogClearCompactsReusableSlots(t *testing.T) {
	logPath := TempFileName(".tlog")
	t.Cleanup(func() { _ = os.Remove(logPath) })
	txLog := NewTxLog(logPath, 0600, BTreePageSize)
	require.NoError(t, txLog.openErr)
	page := &Page{PageNumber: 7, Data: bytes.Repeat([]byte{0x5a}, BTreePageSize)}
	_, err := txLog.ReplaceWithPages([]*Page{page}, true)
	require.NoError(t, err)
	info, err := txLog.file.Stat()
	require.NoError(t, err)
	require.Greater(t, info.Size(), int64(txLogDataOffset))
	require.NoError(t, txLog.Clear())
	info, err = txLog.file.Stat()
	require.NoError(t, err)
	require.Equal(t, int64(txLogDataOffset), info.Size())
	active, err := txLog.HasActiveJournal()
	require.NoError(t, err)
	require.False(t, active)
	require.NoError(t, txLog.Close())
}

func TestTxLogIncreasingBodiesRepackInsteadOfGrowingWithoutBound(t *testing.T) {
	logPath := TempFileName(".tlog")
	t.Cleanup(func() { _ = os.Remove(logPath) })
	txLog := NewTxLog(logPath, 0600, BTreePageSize)
	require.NoError(t, txLog.openErr)
	defer txLog.Close()

	const maxPages = 24
	for pageCount := 1; pageCount <= maxPages; pageCount++ {
		pages := make([]*Page, pageCount)
		for idx := range pages {
			pages[idx] = &Page{PageNumber: uint64(idx + 3), Data: bytes.Repeat([]byte{byte(pageCount)}, BTreePageSize)}
		}
		_, err := txLog.ReplaceWithPages(pages, true)
		require.NoError(t, err)
		require.NoError(t, txLog.ClearFast())
	}
	info, err := txLog.file.Stat()
	require.NoError(t, err)
	liveBodySize := int64(maxPages * (txLogPageHeaderSize + BTreePageSize))
	require.LessOrEqual(t, info.Size(), int64(txLogDataOffset)+2*liveBodySize)
}
