package storage

import (
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestDAL(t *testing.T) {
	testFileName := "test_data.db"
	testPattern := "Hello, world!"

	opts := DefaultOptions()
	// Open or create the DAL
	dal, err := NewDal(testFileName, opts)
	require.NoError(t, err)
	defer func() {
		_ = os.Remove(testFileName)
		_ = os.Remove(dal.opts.TxLogPath)
	}()

	// Test writing to a page
	page, allocatePageErr := dal.AllocatePage()
	require.NoError(t, allocatePageErr)
	pageNumber := page.PageNumber

	copy(page.Data, []byte(testPattern))
	dal.SetPage(page)
	require.NoError(t, err)
	err = dal.Close()
	require.NoError(t, err)

	// Re-open the DAL and verify the page data
	// This is to ensure that the data was written to disk
	newDal, reOpenDalErr := NewDal(testFileName, opts)
	if reOpenDalErr != nil {
		t.Fatalf("Failed to re-open DAL: %v", reOpenDalErr)
	}

	// Re-read the pageNum and verify the modification
	newPage, getPageErr := newDal.GetPage(pageNumber)
	require.NoError(t, getPageErr)
	require.True(t, string(newPage.Data[:len(testPattern)]) == testPattern)
	_ = newDal.Close()
}

func TestDALMetadata(t *testing.T) {
	testFileName := "test_data.db"
	opts := DefaultOptions()
	dal, err := NewDal(testFileName, opts)
	require.NoError(t, err)
	defer func() {
		_ = os.Remove(testFileName)
		_ = os.Remove(dal.opts.TxLogPath)
	}()

	err = dal.Close()
	require.NoError(t, err)

	dal, err = NewDal(testFileName, opts)
	require.NoError(t, err)
	meta := dal.meta
	require.Equal(t, meta.GetDbName(), dbName)
	major, minor := meta.GetDbVersion()
	require.Equal(t, []byte{major, minor}, []byte{dbVersionMajor, dbVersionMinor})
	err = dal.Close()
	require.NoError(t, err)
}

func TestDALFreelist(t *testing.T) {
	testFileName := "test_data.db"
	opts := DefaultOptions()

	dal, err := NewDal(testFileName, opts)
	require.NoError(t, err)
	defer func() {
		_ = os.Remove(testFileName)
		_ = os.Remove(dal.opts.TxLogPath)
	}()

	pageNum, _ := dal.AllocatePage()
	err = dal.SetPage(pageNum)
	require.NoError(t, err)

	err = dal.ReleasePage(pageNum.PageNumber)
	require.NoError(t, err)

	releasedExtents := append([]pageExtent(nil), dal.freelist.releasedExtents...)
	err = WriteFreelist(dal, dal.freelist)
	require.NoError(t, err)
	err = dal.Close()
	require.NoError(t, err)

	dal, err = NewDal(testFileName, opts)
	require.NoError(t, err)
	require.Equal(t, releasedExtents, dal.freelist.releasedExtents)
	err = dal.Close()
	require.NoError(t, err)
}

func TestDALReleasePageValidation(t *testing.T) {
	testFileName := "test_data.db"
	opts := DefaultOptions()

	dal, err := NewDal(testFileName, opts)
	require.NoError(t, err)
	defer func() {
		_ = os.Remove(testFileName)
		_ = os.Remove(dal.opts.TxLogPath)
	}()

	require.Error(t, dal.ReleasePage(metaPageNumber))
	require.Error(t, dal.ReleasePage(freelistPageNumber))
	require.Error(t, dal.ReleasePage(dal.meta.root))

	page, err := dal.AllocatePage()
	require.NoError(t, err)
	require.NoError(t, dal.ReleasePage(page.PageNumber))
	require.NoError(t, dal.ReleasePage(page.PageNumber)) // idempotent duplicate release

	require.True(t, dal.freelist.containsReleasedPage(page.PageNumber))
	require.EqualValues(t, 1, dal.freelist.releasedPageN())
	require.Error(t, dal.ReleasePage(dal.freelist.currentPage+1))
}

func TestNewDatabaseRefusesExistingNonEmptyTransactionLog(t *testing.T) {
	path := TempFileName(".db")
	tlogPath := TempFileName(".tlog")
	require.NoError(t, os.WriteFile(tlogPath, []byte("existing journal"), 0600))
	t.Cleanup(func() {
		_ = os.Remove(path)
		_ = os.Remove(tlogPath)
	})
	_, err := Open(path, DefaultOptions().WithTxLogPath(tlogPath))
	require.ErrorIs(t, err, ErrRecoveryRequired)
	_, statErr := os.Stat(path)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestDifferentDatabasesCannotShareTransactionLog(t *testing.T) {
	firstPath := TempFileName(".db")
	secondPath := TempFileName(".db")
	tlogPath := TempFileName(".tlog")
	first, err := Open(firstPath, DefaultOptions().WithTxLogPath(tlogPath))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = first.Close()
		_ = os.Remove(firstPath)
		_ = os.Remove(secondPath)
		_ = os.Remove(tlogPath)
	})
	_, err = Open(secondPath, DefaultOptions().WithTxLogPath(tlogPath))
	require.ErrorContains(t, err, "transaction log")
	_, statErr := os.Stat(secondPath)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}
