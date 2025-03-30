package storage

import (
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestDAL(t *testing.T) {
	testFileName := "test_data.db"
	testPattern := "Hello, world!"

	// Open or create the DAL
	dal, err := NewDal(testFileName, 0644, 4096)
	require.NoError(t, err)
	defer func() {
		_ = os.Remove(testFileName)
	}()

	// Test writing to a pageNum
	pageNum, allocatePageErr := dal.AllocatePage()
	require.NoError(t, allocatePageErr)
	pageNumber := pageNum.PageNumber

	copy(pageNum.Data, []byte(testPattern))
	err = dal.Sync()
	require.NoError(t, err)
	err = dal.Close()
	require.NoError(t, err)

	// Re-open the DAL and verify the pageNum data
	// This is to ensure that the data was written to disk
	newDal, reOpenDalErr := NewDal(testFileName, 0644, 4096)
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

	dal, err := NewDal(testFileName, 0644, BTreePageSize)
	require.NoError(t, err)
	defer func() {
		_ = os.Remove(testFileName)
	}()

	err = dal.Close()
	require.NoError(t, err)

	dal, err = NewDal(testFileName, 0644, BTreePageSize)
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

	dal, err := NewDal(testFileName, 0644, BTreePageSize)
	require.NoError(t, err)
	defer func() {
		_ = os.Remove(testFileName)
	}()

	pageNum, _ := dal.AllocatePage()
	err = dal.SetPage(pageNum)
	require.NoError(t, err)

	err = dal.ReleasePage(pageNum.PageNumber)
	require.NoError(t, err)

	releasedPages := dal.freelist.releasedPages
	err = WriteFreelist(dal, dal.freelist)
	require.NoError(t, err)
	err = dal.Close()
	require.NoError(t, err)

	dal, err = NewDal(testFileName, 0644, BTreePageSize)
	require.NoError(t, err)
	require.Equal(t, releasedPages, dal.freelist.releasedPages)
	err = dal.Close()
	require.NoError(t, err)
}
