package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/stretchr/testify/require"
	"hash/crc32"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
)

func TestFreelistGrowthFromReleasedPageRoundTrips(t *testing.T) {
	for _, checksums := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy", true: "extents"}[checksums], func(t *testing.T) {
			db, _ := CreateTestDB(t)
			require.NoError(t, db.dal.allocateFile(4096*BTreePageSize))
			db.dal.pageChecksums = checksums
			f := newFreelist(BTreePageSize, 4096, checksums)
			f.currentPage = 4095
			f.freelistPages = []uint64{freelistPageNumber}
			f.rebuildFreelistPageSet()
			for i := 0; i < f.recordsPerFirst+1; i++ {
				f.addReleasedPage(uint64(rootPageNumber + 2 + i*2))
			}
			pages, err := BuildFreelistPages(db.dal, f)
			require.NoError(t, err)
			expected := len(f.releasedExtents)
			if !checksums {
				expected = int(f.releasedCount)
			}
			require.EqualValues(t, expected, binary.LittleEndian.Uint64(pages[0].Data[freelistNumPagesOffset:]))
			for _, p := range pages {
				require.NoError(t, db.dal.SetPage(p))
			}
			require.NoError(t, db.dal.Sync())
			read, err := ReadFreelist(db.dal)
			require.NoError(t, err)
			require.Equal(t, f.releasedExtents, read.releasedExtents)
			for _, p := range f.freelistPages {
				require.False(t, read.containsReleasedPage(p))
			}
		})
	}
}

func TestIntegrityAfterAllocationRollback(t *testing.T) {
	for _, policy := range []SyncPolicy{SyncPolicyStrict, SyncPolicyJournal, SyncPolicyGroup} {
		db, err := Open(filepath.Join(t.TempDir(), "rollback.db"), DefaultOptions().WithSyncPolicy(policy))
		require.NoError(t, err)
		defer db.Close()
		for range 3 {
			tx := db.Begin(true)
			_, err = tx.CreateBucket([]byte("aborted"))
			require.NoError(t, err)
			tx.Rollback()
			_, err = db.Check()
			require.NoError(t, err)
		}
	}
}

func TestSnapshotPreservesSequencesAndRejectsCorruption(t *testing.T) {
	db, _ := CreateTestDB(t)
	require.NoError(t, db.Update(func(tx *Tx) error {
		b, e := tx.CreateBucket([]byte("b"))
		if e != nil {
			return e
		}
		for range 4 {
			if _, e = b.NextSequence(); e != nil {
				return e
			}
		}
		return b.Put([]byte("key"), []byte("original"))
	}))
	path := filepath.Join(t.TempDir(), "backup.snapshot")
	_, err := ExportDBToPath(db, path, false)
	require.NoError(t, err)
	dst, _ := CreateTestDB(t)
	_, err = ImportDBFromPath(dst, path)
	require.NoError(t, err)
	require.NoError(t, dst.Update(func(tx *Tx) error {
		b, e := tx.GetBucket([]byte("b"))
		if e != nil {
			return e
		}
		n, e := b.NextSequence()
		require.EqualValues(t, 5, n)
		return e
	}))
	original, err := os.ReadFile(path)
	require.NoError(t, err)
	variants := [][]byte{append(append([]byte(nil), original...), 0), append([]byte(nil), original[:len(original)-1]...), append([]byte(nil), original...)}
	variants[2][len(original)-33] ^= 1
	for _, bad := range variants {
		require.NoError(t, os.WriteFile(path, bad, 0600))
		_, err = ImportDBFromPath(dst, path)
		require.ErrorIs(t, err, ErrSnapshotInvalid)
		requireBucketValue(t, dst, []byte("b"), []byte("key"), []byte("original"))
		_, err = dst.Check()
		require.NoError(t, err)
	}
}

func TestSnapshotCannotReplaceActiveFiles(t *testing.T) {
	db, path := CreateTestDB(t)
	alias := filepath.Join(t.TempDir(), "alias")
	require.NoError(t, os.Symlink(filepath.Dir(path), alias))
	for _, p := range []string{path, db.GetOptions().TxLogPath, filepath.Join(alias, filepath.Base(path))} {
		_, err := ExportDBToPath(db, p, true)
		require.Error(t, err)
	}
	_, err := db.Check()
	require.NoError(t, err)
}

func TestSnapshotConcurrentNoOverwrite(t *testing.T) {
	db, _ := CreateTestDB(t)
	path := filepath.Join(t.TempDir(), "backup")
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() { defer wg.Done(); _, err := ExportDBToPath(db, path, false); results <- err }()
	}
	wg.Wait()
	close(results)
	successes := 0
	for err := range results {
		if err == nil {
			successes++
		} else {
			require.ErrorIs(t, err, os.ErrExist)
		}
	}
	require.Equal(t, 1, successes)
	dst, _ := CreateTestDB(t)
	_, err := ImportDBFromPath(dst, path)
	require.NoError(t, err)
}

func TestWrongJournalCannotModifyDatabase(t *testing.T) {
	for _, active := range []bool{false, true} {
		t.Run(map[bool]string{false: "inactive", true: "active"}[active], func(t *testing.T) {
			dir := t.TempDir()
			a, err := Open(filepath.Join(dir, "a.db"), DefaultOptions().WithSyncPolicy(SyncPolicyJournal).WithCheckpointTxThreshold(999))
			require.NoError(t, err)
			bpath := filepath.Join(dir, "b.db")
			b, err := Open(bpath, nil)
			require.NoError(t, err)
			require.NoError(t, b.Close())
			if active {
				require.NoError(t, a.Update(func(tx *Tx) error {
					bucket, e := tx.CreateBucket([]byte("a"))
					if e != nil {
						return e
					}
					return bucket.Put([]byte("k"), []byte("wrong"))
				}))
			}
			walpath := a.GetOptions().TxLogPath
			require.NoError(t, a.dal.Close()) // Preserve the crash state, without checkpointing.
			beforeDB, err := os.ReadFile(bpath)
			require.NoError(t, err)
			beforeLog, err := os.ReadFile(walpath)
			require.NoError(t, err)
			_, err = Open(bpath, DefaultOptions().WithTxLogPath(walpath))
			require.ErrorIs(t, err, ErrJournalIdentity)
			after, err := os.ReadFile(bpath)
			require.NoError(t, err)
			require.Equal(t, beforeDB, after)
			after, err = os.ReadFile(walpath)
			require.NoError(t, err)
			require.Equal(t, beforeLog, after)
		})
	}
}

func TestMetadataRecoveryRequiresIndependentIdentity(t *testing.T) {
	for _, destroyIdentity := range []bool{false, true} {
		t.Run(map[bool]string{false: "surviving-anchor", true: "lost-anchor"}[destroyIdentity], func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "db.db")
			db, err := Open(path, nil)
			require.NoError(t, err)
			db.dal.beforeSetPageHook = func(_ *Page) error {
				if !db.dal.txLog.active {
					return os.ErrPermission
				}
				return nil
			}
			err = db.Update(func(tx *Tx) error { // Enough buckets to force the catalog root to change, journaling metadata images.
				for i := 0; i < 150; i++ {
					if _, e := tx.CreateBucket([]byte(fmt.Sprintf("bucket-%03d", i))); e != nil {
						return e
					}
				}
				return nil
			})
			require.ErrorIs(t, err, ErrCommitUncertain)
			require.NoError(t, db.dal.Close())
			file, err := os.OpenFile(path, os.O_RDWR, 0)
			require.NoError(t, err)
			for _, page := range []int64{metaPageNumber, metaMirrorPageNumber} {
				_, err = file.WriteAt([]byte("X"), page*BTreePageSize)
				require.NoError(t, err)
				if destroyIdentity {
					_, err = file.WriteAt([]byte("X"), page*BTreePageSize+metadataIdentityOffset)
					require.NoError(t, err)
				}
			}
			require.NoError(t, file.Sync())
			require.NoError(t, file.Close())
			recovered, err := Open(path, nil)
			if destroyIdentity {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			defer recovered.Close()
			_, err = recovered.Check()
			require.NoError(t, err)
		})
	}
}

func TestSameBasenameUsesDistinctJournals(t *testing.T) {
	dir := t.TempDir()
	a, err := Open(filepath.Join(dir, "foo.db"), nil)
	require.NoError(t, err)
	defer a.Close()
	b, err := Open(filepath.Join(dir, "foo.data"), nil)
	require.NoError(t, err)
	defer b.Close()
	require.NotEqual(t, a.GetOptions().TxLogPath, b.GetOptions().TxLogPath)
}

func TestBatchedRestoreExceedsTransactionBudget(t *testing.T) {
	source, _ := CreateTestDB(t)
	for batch := 0; batch < 10; batch++ {
		require.NoError(t, source.Update(func(tx *Tx) error {
			bucket, e := tx.CreateBucketIfNotExists([]byte("bulk"))
			if e != nil {
				return e
			}
			for i := 0; i < 30; i++ {
				if e = bucket.Put([]byte(fmt.Sprintf("key-%04d", batch*30+i)), bytes.Repeat([]byte("x"), 2048)); e != nil {
					return e
				}
			}
			bucket.counter = 999
			bucket.markDirty()
			return nil
		}))
	}
	snapshot := filepath.Join(t.TempDir(), "bulk.snapshot")
	_, err := ExportDBToPath(source, snapshot, false)
	require.NoError(t, err)
	dir := filepath.Join(t.TempDir(), "restored")
	opts := DefaultOptions().WithMaxTransactionBytes(64 * 1024)
	stats, err := RestoreDBToDirectory(snapshot, dir, opts)
	require.NoError(t, err)
	require.EqualValues(t, 300, stats.KeysImported)
	_, err = os.Stat(filepath.Join(dir, "READY"))
	require.NoError(t, err)
	dst, err := Open(filepath.Join(dir, "database.db"), opts)
	require.NoError(t, err)
	defer dst.Close()
	_, err = dst.Check()
	require.NoError(t, err)
	require.NoError(t, dst.View(func(tx *Tx) error {
		b, e := tx.GetBucket([]byte("bulk"))
		if e != nil {
			return e
		}
		require.EqualValues(t, 999, b.Sequence())
		require.EqualValues(t, 300, b.ItemCount())
		return nil
	}))
	_, err = RestoreDBToDirectory(snapshot, dir, opts)
	require.ErrorIs(t, err, os.ErrExist)
	bad, err := os.ReadFile(snapshot)
	require.NoError(t, err)
	bad[len(bad)-1] ^= 1
	require.NoError(t, os.WriteFile(snapshot, bad, 0600))
	failedDir := filepath.Join(t.TempDir(), "failed")
	_, err = RestoreDBToDirectory(snapshot, failedDir, opts)
	require.ErrorIs(t, err, ErrSnapshotInvalid)
	_, err = os.Stat(failedDir)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestCleanV03MigrationAndInterruptedMetadataUpgrade(t *testing.T) {
	for _, mixed := range []bool{false, true} {
		t.Run(fmt.Sprint(mixed), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "legacy.db")
			db, err := Open(path, nil)
			require.NoError(t, err)
			require.NoError(t, db.Update(func(tx *Tx) error {
				b, e := tx.CreateBucket([]byte("kept"))
				if e != nil {
					return e
				}
				return b.Put([]byte("key"), []byte("value"))
			}))
			legacy := *db.dal.meta
			legacy.dbVersion = 3
			legacy.databaseID = [16]byte{}
			legacy.commitSequence = 0
			for _, number := range []uint64{metaPageNumber, metaMirrorPageNumber} {
				m := &legacy
				if mixed && number == metaMirrorPageNumber {
					upgraded := legacy
					upgraded.dbVersion = 4
					upgraded.databaseID = newDatabaseID()
					m = &upgraded
				}
				p, e := buildMetaPageAt(number, legacy.pageSize, m)
				require.NoError(t, e)
				require.NoError(t, db.dal.SetPage(p))
			}
			require.NoError(t, db.dal.Sync())
			wal := db.GetOptions().TxLogPath
			require.NoError(t, db.dal.Close())
			require.NoError(t, os.Truncate(wal, 0))
			migrated, err := Open(path, nil)
			require.NoError(t, err)
			require.EqualValues(t, 4, migrated.dal.meta.dbVersion)
			require.NotEqual(t, [16]byte{}, migrated.dal.meta.databaseID)
			require.Equal(t, migrated.dal.meta.databaseID, migrated.dal.txLog.header.databaseID)
			requireBucketValue(t, migrated, []byte("kept"), []byte("key"), []byte("value"))
			require.NoError(t, migrated.Close())
			again, err := Open(path, nil)
			require.NoError(t, err)
			defer again.Close()
			_, err = again.Check()
			require.NoError(t, err)
		})
	}
}

func TestStaleSameDatabaseJournalIsRejected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stale.db")
	db, err := Open(path, DefaultOptions().WithSyncPolicy(SyncPolicyJournal).WithCheckpointTxThreshold(999))
	require.NoError(t, err)
	put := func(value string) {
		require.NoError(t, db.Update(func(tx *Tx) error {
			b, e := tx.CreateBucketIfNotExists([]byte("b"))
			if e != nil {
				return e
			}
			return b.Put([]byte("key"), []byte(value))
		}))
	}
	put("old")
	wal := db.GetOptions().TxLogPath
	stale, err := os.ReadFile(wal)
	require.NoError(t, err)
	put("new")
	require.NoError(t, db.Close())
	require.NoError(t, os.WriteFile(wal, stale, 0600))
	before, err := os.ReadFile(path)
	require.NoError(t, err)
	_, err = Open(path, nil)
	require.ErrorIs(t, err, ErrJournalIdentity)
	after, e := os.ReadFile(path)
	require.NoError(t, e)
	require.Equal(t, before, after)
}

func TestCrashRecoveryAcknowledgedWrites(t *testing.T) {
	if path := os.Getenv("PIRINDB_CRASH_TEST_PATH"); path != "" {
		policy, _ := strconv.Atoi(os.Getenv("PIRINDB_CRASH_TEST_POLICY"))
		db, e := Open(path, DefaultOptions().WithSyncPolicy(SyncPolicy(policy)).WithCheckpointTxThreshold(999))
		if e != nil {
			panic(e)
		}
		for i := 0; i < 12; i++ {
			e = db.Update(func(tx *Tx) error {
				b, e := tx.CreateBucketIfNotExists([]byte("ack"))
				if e != nil {
					return e
				}
				return b.Put([]byte(fmt.Sprint(i)), []byte("committed"))
			})
			if e != nil {
				panic(e)
			}
			fmt.Printf("ACK %d\n", i)
		}
		os.Exit(0) // Deliberately omit all close/checkpoint cleanup.
	}
	for _, policy := range []SyncPolicy{SyncPolicyStrict, SyncPolicyJournal, SyncPolicyGroup} {
		t.Run(fmt.Sprint(policy), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "crash.db")
			child := exec.Command(os.Args[0], "-test.run=^TestCrashRecoveryAcknowledgedWrites$")
			child.Env = append(os.Environ(), "PIRINDB_CRASH_TEST_PATH="+path, "PIRINDB_CRASH_TEST_POLICY="+fmt.Sprint(policy))
			out, err := child.CombinedOutput()
			require.NoError(t, err, string(out))
			require.Equal(t, 12, strings.Count(string(out), "ACK "))
			db, err := Open(path, nil)
			require.NoError(t, err)
			defer db.Close()
			for i := 0; i < 12; i++ {
				requireBucketValue(t, db, []byte("ack"), []byte(fmt.Sprint(i)), []byte("committed"))
			}
			_, err = db.Check()
			require.NoError(t, err)
		})
	}
}

func TestSnapshotEmptyKeysRoundTrip(t *testing.T) {
	db, _ := CreateTestDB(t)
	require.NoError(t, db.Update(func(tx *Tx) error {
		b, e := tx.CreateBucket([]byte("b"))
		if e != nil {
			return e
		}
		if e = b.Put([]byte{}, []byte("empty")); e != nil {
			return e
		}
		return b.Put([]byte("x"), []byte("other"))
	}))
	snapshot := filepath.Join(t.TempDir(), "empty.snapshot")
	_, err := ExportDBToPath(db, snapshot, false)
	require.NoError(t, err)
	dst, _ := CreateTestDB(t)
	_, err = ImportDBFromPath(dst, snapshot)
	require.NoError(t, err)
	requireBucketValue(t, dst, []byte("b"), []byte{}, []byte("empty"))
	dir := filepath.Join(t.TempDir(), "restored")
	_, err = RestoreDBToDirectory(snapshot, dir, nil)
	require.NoError(t, err)
	restored, err := Open(filepath.Join(dir, "database.db"), nil)
	require.NoError(t, err)
	defer restored.Close()
	requireBucketValue(t, restored, []byte("b"), []byte{}, []byte("empty"))
}

func TestLegacyWALClearProducesRecoverableNewHeader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.tlog")
	h := newTxLogHeader(BTreePageSize)
	h.slotOffsets = [2]uint64{2 * txLogV3HeaderSize, 2 * txLogV3HeaderSize}
	current := marshalTxLogHeader(h)
	legacy := append([]byte(nil), current[:txLogV3HeaderSize]...)
	binary.LittleEndian.PutUint16(legacy[txLogVersionOffset:], txLogVersionV3)
	binary.LittleEndian.PutUint32(legacy[txLogV3ChecksumOffset:], crc32.ChecksumIEEE(legacy[:txLogV3ChecksumOffset]))
	require.NoError(t, os.WriteFile(path, append(legacy, legacy...), 0600))
	log := NewTxLog(path, 0600, BTreePageSize)
	require.NoError(t, log.ensureHeaderLoaded())
	require.EqualValues(t, txLogVersionV3, log.headerVersion)
	require.NoError(t, log.Clear())
	require.NoError(t, log.Close())
	// Reopen at the historical Clear-before-binding interruption point.
	log = NewTxLog(path, 0600, BTreePageSize)
	defer log.Close()
	active, err := log.HasActiveJournal()
	require.NoError(t, err)
	require.False(t, active)
	require.NoError(t, log.bindInactiveDatabase(newDatabaseID()))
}

func TestTornWALUpgradeRetainsInactiveLegacyFallback(t *testing.T) {
	for _, size := range []int{8, txLogHeaderSize} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "legacy.tlog")
			h := newTxLogHeader(BTreePageSize)
			h.slotOffsets = [2]uint64{2 * txLogV3HeaderSize, 2 * txLogV3HeaderSize}
			modern := marshalTxLogHeader(h)
			legacy := append([]byte(nil), modern[:txLogV3HeaderSize]...)
			binary.LittleEndian.PutUint16(legacy[txLogVersionOffset:], txLogVersionV3)
			binary.LittleEndian.PutUint32(legacy[txLogV3ChecksumOffset:], crc32.ChecksumIEEE(legacy[:txLogV3ChecksumOffset]))
			require.NoError(t, os.WriteFile(path, append(legacy, legacy...), 0600))
			torn := marshalTxLogHeader(newTxLogHeader(BTreePageSize))
			torn[len(torn)-1] ^= 1
			file, err := os.OpenFile(path, os.O_RDWR, 0)
			require.NoError(t, err)
			_, err = file.WriteAt(torn[:size], txLogHeaderStride)
			require.NoError(t, err)
			require.NoError(t, file.Close())
			log := NewTxLog(path, 0600, BTreePageSize)
			defer log.Close()
			active, err := log.HasActiveJournal()
			require.NoError(t, err)
			require.False(t, active)
			require.NoError(t, log.Clear())
			require.NoError(t, log.bindInactiveDatabase(newDatabaseID()))
		})
	}
}

func TestMirrorIdentityEvidenceBlocksWrongRecovery(t *testing.T) {
	for _, conflict := range []bool{false, true} {
		t.Run(fmt.Sprint(conflict), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "identity.db")
			db, err := Open(path, DefaultOptions().WithSyncPolicy(SyncPolicyJournal).WithCheckpointTxThreshold(999))
			require.NoError(t, err)
			put := func(v string) {
				require.NoError(t, db.Update(func(tx *Tx) error {
					b, e := tx.CreateBucketIfNotExists([]byte("b"))
					if e != nil {
						return e
					}
					return b.Put([]byte("k"), []byte(v))
				}))
			}
			put("old")
			oldMeta, err := db.dal.GetPage(metaPageNumber)
			require.NoError(t, err)
			oldLog, err := os.ReadFile(db.GetOptions().TxLogPath)
			require.NoError(t, err)
			put("new")
			if conflict {
				changed := *db.dal.meta
				changed.databaseID = newDatabaseID()
				page, e := buildMetaPageAt(metaMirrorPageNumber, changed.pageSize, &changed)
				require.NoError(t, e)
				require.NoError(t, db.dal.SetPage(page))
			} else {
				require.NoError(t, db.dal.SetPage(oldMeta))
			}
			require.NoError(t, db.dal.Sync())
			wal := db.GetOptions().TxLogPath
			require.NoError(t, db.dal.Close())
			if !conflict {
				require.NoError(t, os.WriteFile(wal, oldLog, 0600))
			}
			before, err := os.ReadFile(path)
			require.NoError(t, err)
			beforeLog, err := os.ReadFile(wal)
			require.NoError(t, err)
			_, err = Open(path, nil)
			require.ErrorIs(t, err, ErrJournalIdentity)
			after, e := os.ReadFile(path)
			require.NoError(t, e)
			require.Equal(t, before, after)
			after, e = os.ReadFile(wal)
			require.NoError(t, e)
			require.Equal(t, beforeLog, after)
		})
	}
}

func TestExplicitLegacyV3RecoveryRetiresBeforeUpgrade(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v3.db")
	db, err := Open(path, nil)
	require.NoError(t, err)
	legacy := *db.dal.meta
	legacy.dbVersion = 3
	legacy.databaseID = [16]byte{}
	legacy.commitSequence = 0
	for _, number := range []uint64{metaPageNumber, metaMirrorPageNumber} {
		page, e := buildMetaPageAt(number, legacy.pageSize, &legacy)
		require.NoError(t, e)
		require.NoError(t, db.dal.SetPage(page))
	}
	root, err := db.dal.GetPage(rootPageNumber)
	require.NoError(t, err)
	require.NoError(t, db.dal.Sync())
	wal := db.GetOptions().TxLogPath
	require.NoError(t, db.dal.Close())
	body := make([]byte, txLogPageHeaderSize+BTreePageSize)
	binary.LittleEndian.PutUint64(body, uint64(rootPageNumber*BTreePageSize))
	binary.LittleEndian.PutUint64(body[txLogPageNumber:], rootPageNumber)
	copy(body[txLogPageHeaderSize:], root.Data)
	h := newTxLogHeader(BTreePageSize)
	h.activeSlot = 0
	h.numPages = 1
	h.crc = crc32.ChecksumIEEE(body)
	h.slotOffsets = [2]uint64{2 * txLogV3HeaderSize, uint64(2*txLogV3HeaderSize + len(body))}
	h.slotCapacities = [2]uint64{uint64(len(body)), 0}
	raw := marshalTxLogHeader(h)
	header := append([]byte(nil), raw[:txLogV3HeaderSize]...)
	binary.LittleEndian.PutUint16(header[txLogVersionOffset:], txLogVersionV3)
	binary.LittleEndian.PutUint32(header[txLogV3ChecksumOffset:], crc32.ChecksumIEEE(header[:txLogV3ChecksumOffset]))
	data := append(append(append([]byte(nil), header...), header...), body...)
	require.NoError(t, os.WriteFile(wal, data, 0600))
	_, err = Open(path, nil)
	require.ErrorIs(t, err, ErrLegacyRecovery)
	unchanged, e := os.ReadFile(wal)
	require.NoError(t, e)
	require.Equal(t, data, unchanged)
	migrated, err := Open(path, DefaultOptions().WithLegacyRecovery(true))
	require.NoError(t, err)
	defer migrated.Close()
	_, err = migrated.Check()
	require.NoError(t, err)
	require.NotEqual(t, [16]byte{}, migrated.dal.meta.databaseID)
}

func TestJournalSyncFaultPreservesEarlierAcknowledgement(t *testing.T) {
	for _, policy := range []SyncPolicy{SyncPolicyStrict, SyncPolicyJournal, SyncPolicyGroup} {
		for _, failure := range []string{"body", "header"} {
			t.Run(fmt.Sprintf("%d-%s", policy, failure), func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "fault.db")
				db, err := Open(path, DefaultOptions().WithSyncPolicy(policy).WithCheckpointTxThreshold(999))
				require.NoError(t, err)
				put := func(value string) error {
					return db.Update(func(tx *Tx) error {
						b, e := tx.CreateBucketIfNotExists([]byte("b"))
						if e != nil {
							return e
						}
						return b.Put([]byte("key"), []byte(value))
					})
				}
				require.NoError(t, put("acknowledged"))
				injected := false
				db.dal.txLog.beforeSyncHook = func(stage string) error {
					if !injected && stage == failure {
						injected = true
						return os.ErrPermission
					}
					return nil
				}
				require.Error(t, put("unacknowledged"))
				require.True(t, injected)
				require.NoError(t, db.dal.Close())
				recovered, err := Open(path, nil)
				require.NoError(t, err)
				defer recovered.Close()
				requireBucketValue(t, recovered, []byte("b"), []byte("key"), []byte("acknowledged"))
				_, err = recovered.Check()
				require.NoError(t, err)
			})
		}
	}
}

func TestWALRetirementIsDurableBeforeCompaction(t *testing.T) {
	path := filepath.Join(t.TempDir(), "compact.tlog")
	log := NewTxLog(path, 0600, BTreePageSize)
	defer log.Close()
	_, err := log.ReplaceWithPages([]*Page{{PageNumber: 3, Data: make([]byte, BTreePageSize)}}, true)
	require.NoError(t, err)
	require.NoError(t, log.ClearFast())
	stages := []string{}
	log.beforeSyncHook = func(stage string) error { stages = append(stages, stage); return nil }
	log.beforeTruncateHook = func() error { require.Equal(t, []string{"header", "mirror"}, stages); return os.ErrPermission }
	require.ErrorIs(t, log.compact(), os.ErrPermission)
	reader := NewTxLog(path, 0600, BTreePageSize)
	defer reader.Close()
	active, err := reader.HasActiveJournal()
	require.NoError(t, err)
	require.False(t, active)
}
