package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBNodeDeserializeRejectsMalformedLengthsWithoutPanic(t *testing.T) {
	data := make([]byte, BTreePageSize)
	data[NodePageTypeOffset] = NodePage
	data[NodeTypeOffset] = 1
	binary.LittleEndian.PutUint16(data[NodeNumItemsOffset:], 1)
	binary.LittleEndian.PutUint16(data[NodeHeaderSize:], 0)
	binary.LittleEndian.PutUint16(data[NodeHeaderSize+UInt16Size:], ^uint16(0))
	binary.LittleEndian.PutUint16(data[NodeHeaderSize+2*UInt16Size:], 1)
	require.NoError(t, sealPageData(data, true))
	require.ErrorIs(t, NewBNode().Deserialize(data), ErrCorruptedNode)
}

func TestBNodeDeserializeRejectsInvalidChildCountAndOrdering(t *testing.T) {
	node := &BNode{
		items: []*Item{
			{Key: []byte("b"), Value: []byte{ValueSimple, '1'}},
			{Key: []byte("a"), Value: []byte{ValueSimple, '2'}},
		},
	}
	data := make([]byte, BTreePageSize)
	require.ErrorIs(t, node.Serialize(data), ErrCorruptedNode)

	clear(data)
	data[NodePageTypeOffset] = NodePage
	data[NodeTypeOffset] = 0
	binary.LittleEndian.PutUint16(data[NodeNumItemsOffset:], 1)
	binary.LittleEndian.PutUint16(data[NodeHeaderSize:], 1)
	require.NoError(t, sealPageData(data, true))
	require.ErrorIs(t, NewBNode().Deserialize(data), ErrCorruptedNode)
}

func TestBlobHeaderRejectsImpossibleSizeAndPageCount(t *testing.T) {
	page := &Page{PageNumber: 3, Data: make([]byte, BTreePageSize)}
	page.Data[0] = BlobPage
	binary.LittleEndian.PutUint32(page.Data[blobFirstPageTotalPagesOffset:], 1)
	binary.LittleEndian.PutUint32(page.Data[blobFirstPageDataSizeOffset:], ^uint32(0))
	_, _, err := decodeBlobHeaderForSize(page, BTreePageSize-pageChecksumSize)
	require.ErrorIs(t, err, ErrCorruptedBlob)

	binary.LittleEndian.PutUint32(page.Data[blobFirstPageDataSizeOffset:], 10_000)
	_, _, err = decodeBlobHeaderForSize(page, BTreePageSize-pageChecksumSize)
	require.ErrorIs(t, err, ErrCorruptedBlob)
}

func flipFileByte(t *testing.T, path string, offset int64) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	defer file.Close()
	buffer := []byte{0}
	_, err = file.ReadAt(buffer, offset)
	require.NoError(t, err)
	buffer[0] ^= 0x80
	_, err = file.WriteAt(buffer, offset)
	require.NoError(t, err)
	require.NoError(t, file.Sync())
}

func TestPageChecksumDetectsBTreeCorruption(t *testing.T) {
	path := TempFileName(".db")
	db, err := Open(path, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *Tx) error {
		bucket, createErr := tx.CreateBucket([]byte("checksum"))
		if createErr != nil {
			return createErr
		}
		return bucket.Put([]byte("key"), []byte("value"))
	}))
	root := db.dal.meta.root
	tlogPath := db.dal.opts.TxLogPath
	require.NoError(t, db.Close())
	defer os.Remove(path)
	defer os.Remove(tlogPath)

	flipFileByte(t, path, int64(root*BTreePageSize+100))
	db, err = Open(path, nil)
	require.NoError(t, err)
	defer db.Close()
	_, err = db.Check()
	require.True(t, errors.Is(err, ErrChecksumMismatch), "unexpected error: %v", err)
}

func TestMetadataMirrorRepairsPrimaryCorruptionOnOpen(t *testing.T) {
	path := TempFileName(".db")
	db, err := Open(path, nil)
	require.NoError(t, err)
	tlogPath := db.dal.opts.TxLogPath
	require.NoError(t, db.Close())
	defer os.Remove(path)
	defer os.Remove(tlogPath)
	flipFileByte(t, path, 100)
	db, err = Open(path, nil)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	file, err := os.Open(path)
	require.NoError(t, err)
	primary := make([]byte, BTreePageSize)
	_, err = file.ReadAt(primary, 0)
	require.NoError(t, err)
	require.NoError(t, file.Close())
	require.NoError(t, verifyPageData(primary, true))
}

func TestMetadataMirrorRejectsCorruptionOfBothCopies(t *testing.T) {
	path := TempFileName(".db")
	db, err := Open(path, nil)
	require.NoError(t, err)
	tlogPath := db.dal.opts.TxLogPath
	require.NoError(t, db.Close())
	defer os.Remove(path)
	defer os.Remove(tlogPath)
	flipFileByte(t, path, 100)
	flipFileByte(t, path, int64(metaMirrorPageNumber*BTreePageSize+100))
	_, err = Open(path, nil)
	require.ErrorIs(t, err, ErrCorruptedMeta)
}

func TestMetadataDoesNotTreatDamagedChecksummedHeaderAsLegacy(t *testing.T) {
	path := TempFileName(".db")
	db, err := Open(path, nil)
	require.NoError(t, err)
	tlogPath := db.dal.opts.TxLogPath
	require.NoError(t, db.Close())
	defer os.Remove(path)
	defer os.Remove(tlogPath)

	file, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	_, err = file.WriteAt([]byte{2}, int64(metaDbVersionOffset))
	require.NoError(t, err)
	require.NoError(t, file.Sync())
	require.NoError(t, file.Close())
	flipFileByte(t, path, int64(metaMirrorPageNumber*BTreePageSize+100))

	_, err = Open(path, nil)
	require.ErrorIs(t, err, ErrCorruptedMeta)
}

func TestStoredPageSizeBootstrapsReopenAndRecovery(t *testing.T) {
	path := TempFileName(".db")
	opts := DefaultOptions().WithPageSize(8192)
	db, err := Open(path, opts)
	require.NoError(t, err)
	tlogPath := db.dal.opts.TxLogPath
	defer os.Remove(path)
	defer os.Remove(tlogPath)
	require.NoError(t, db.Update(func(tx *Tx) error {
		bucket, createErr := tx.CreateBucket([]byte("custom"))
		if createErr != nil {
			return createErr
		}
		return bucket.Put([]byte("before"), []byte("value"))
	}))
	require.NoError(t, db.Close())

	db, err = Open(path, nil)
	require.NoError(t, err)
	require.EqualValues(t, 8192, db.GetOptions().PageSize)
	tx := db.Begin(true)
	bucket, err := tx.GetBucket([]byte("custom"))
	require.NoError(t, err)
	require.NoError(t, bucket.Put([]byte("recovered"), []byte("yes")))
	failOnce := true
	db.dal.beforeSetPageHook = func(_ *Page) error {
		if !db.dal.txLog.active && failOnce {
			failOnce = false
			return fmt.Errorf("checkpoint failure")
		}
		return nil
	}
	err = tx.Commit()
	require.ErrorContains(t, err, "checkpoint failure")
	db.dal.beforeSetPageHook = nil
	require.NoError(t, db.Close())

	db, err = Open(path, nil)
	require.NoError(t, err)
	defer db.Close()
	require.EqualValues(t, 8192, db.GetOptions().PageSize)
	require.NoError(t, db.View(func(tx *Tx) error {
		bucket, getErr := tx.GetBucket([]byte("custom"))
		if getErr != nil {
			return getErr
		}
		value, found := bucket.Get([]byte("recovered"))
		require.True(t, found)
		require.Equal(t, []byte("yes"), value)
		return nil
	}))
}

func TestLegacyV02RootPageAndFreelistRemainWritable(t *testing.T) {
	path := TempFileName(".db")
	pageSize := uint64(BTreePageSize)
	pageCount := uint64(minFileSize) / pageSize
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	require.NoError(t, err)
	require.NoError(t, file.Truncate(int64(pageCount*pageSize)))

	legacyMeta := NewMeta(pageSize)
	legacyMeta.dbVersion = uint16(dbVersionMajor)<<8 | 2
	legacyMeta.root = legacyRootPageNumber
	metaData := make([]byte, pageSize)
	require.NoError(t, legacyMeta.Serialize(metaData))
	_, err = file.WriteAt(metaData, 0)
	require.NoError(t, err)

	freelistData := make([]byte, pageSize)
	freelistData[freelistPageTypeOffset] = FreeListPage
	binary.LittleEndian.PutUint64(freelistData[freelistCurrentPageOffset:], legacyRootPageNumber)
	binary.LittleEndian.PutUint64(freelistData[freelistMaxPagesOffset:], pageCount)
	_, err = file.WriteAt(freelistData, int64(pageSize*freelistPageNumber))
	require.NoError(t, err)

	rootData := make([]byte, pageSize)
	root := NewBNode()
	require.NoError(t, root.serializePage(rootData, false))
	_, err = file.WriteAt(rootData, int64(pageSize*legacyRootPageNumber))
	require.NoError(t, err)
	require.NoError(t, file.Sync())
	require.NoError(t, file.Close())

	db, err := Open(path, nil)
	require.NoError(t, err)
	tlogPath := db.dal.opts.TxLogPath
	t.Cleanup(func() {
		_ = db.Close()
		_ = os.Remove(path)
		_ = os.Remove(tlogPath)
	})
	require.False(t, db.dal.pageChecksums)
	require.EqualValues(t, legacyRootPageNumber, db.dal.meta.root)
	require.EqualValues(t, legacyRootPageNumber, db.dal.freelist.firstDataPage)
	require.NoError(t, db.Update(func(tx *Tx) error {
		bucket, createErr := tx.CreateBucket([]byte("legacy"))
		if createErr != nil {
			return createErr
		}
		return bucket.Put([]byte("key"), []byte("value"))
	}))
	require.NoError(t, db.Close())
	db, err = Open(path, nil)
	require.NoError(t, err)
	requireBucketValue(t, db, []byte("legacy"), []byte("key"), []byte("value"))
	_, err = db.Check()
	require.NoError(t, err)
}

func TestInvalidPageSizeOptionsAreRejected(t *testing.T) {
	for _, pageSize := range []uint64{0, 2048, 5000, 65536} {
		path := TempFileName(".db")
		_, err := Open(path, DefaultOptions().WithPageSize(pageSize))
		require.ErrorIs(t, err, ErrInvalidOptions)
		_ = os.Remove(path)
	}
}
