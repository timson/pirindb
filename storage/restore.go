package storage

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// RestoreDBToDirectory builds a new database in bounded transactions without
// replacing any active files. A directory is usable only after READY exists.
// An interrupted restore can leave an incomplete directory; it is never reused.
func RestoreDBToDirectory(snapshotPath, directory string, opts *Options) (stats SnapshotImportStats, err error) {
	return restoreWithFS(snapshotPath, directory, opts, osStorageFS{})
}
func restoreWithFS(snapshotPath, directory string, opts *Options, fs storageFS) (stats SnapshotImportStats, err error) {
	source, err := openStorageFile(fs, snapshotPath)
	if err != nil {
		return stats, err
	}
	defer source.Close()
	if err = fs.Mkdir(directory, 0700); err != nil {
		return stats, err
	}
	complete := false
	defer func() {
		if !complete {
			_ = fs.RemoveAll(directory)
		}
	}()
	effective := cloneOptions(opts)
	effective.TxLogPath = filepath.Join(directory, "database.db.tlog")
	effective.AllowLegacyRecovery = false
	effective.EnableRecovery = true
	db, err := openWithFS(filepath.Join(directory, "database.db"), effective, fs)
	if err != nil {
		return stats, err
	}
	defer db.Close()
	counter := &countingReader{r: bufio.NewReader(source)}
	digest := sha256.New()
	r := io.TeeReader(counter, digest)
	header, err := readSnapshotHeader(r)
	if err != nil {
		return stats, err
	}
	if header.Scope != snapshotScopeDB {
		return stats, ErrSnapshotScopeMismatch
	}
	seen := make(map[string]struct{})
	for sectionIndex := uint32(0); sectionIndex < header.SectionCount; sectionIndex++ {
		section, e := readSnapshotBucketSection(r)
		if e != nil {
			return stats, e
		}
		if _, exists := seen[string(section.Name)]; exists {
			return stats, fmt.Errorf("%w: duplicate bucket", ErrSnapshotInvalid)
		}
		seen[string(section.Name)] = struct{}{}
		tx, e := db.BeginE(true)
		if e != nil {
			return stats, e
		}
		// Only one transaction is active at a time; every error aborts it.
		processErr := func() error {
			defer func() { tx.Rollback() }()
			bucket, e := tx.CreateBucket(section.Name)
			if e != nil {
				return e
			}
			bucket.counter = section.Sequence
			bucket.markDirty()
			var previous []byte
			batch := 0
			for index := uint64(0); index < section.ItemCount; index++ {
				key, e := readSnapshotBytes(r, uint64(MaxKeySize-1))
				if e != nil {
					return e
				}
				if index > 0 && bytes.Compare(previous, key) >= 0 {
					return fmt.Errorf("%w: unordered/duplicate key", ErrSnapshotInvalid)
				}
				previous = key
				size, e := readSnapshotValueLength(r, uint64(OneGigabyte-1))
				if e != nil {
					return e
				}
				if batch > 0 && (batch >= 128 || tx.dirtyMemoryEstimate()+int64(size)+int64(8*effective.PageSize) > effective.MaxTransactionBytes*3/4) {
					if e = tx.Commit(); e != nil {
						return e
					}
					tx, e = db.BeginE(true)
					if e != nil {
						return e
					}
					bucket, e = tx.GetBucket(section.Name)
					if e != nil {
						return e
					}
					batch = 0
				}
				if e = bucket.PutReader(key, r, int64(size)); e != nil {
					return e
				}
				batch++
				stats.KeysImported++
			}
			return tx.Commit()
		}()
		if processErr != nil {
			return stats, processErr
		}
		stats.BucketsImported++
	}
	if err = verifySnapshotDigest(counter, digest.Sum(nil)); err != nil {
		return stats, err
	}
	if _, err = db.Check(); err != nil {
		return stats, err
	}
	identity := db.dal.meta.databaseID
	if err = db.Close(); err != nil {
		return stats, err
	}
	stats.BytesRead = counter.n
	manifest, err := json.Marshal(struct {
		Format         int                 `json:"format"`
		Database       string              `json:"database"`
		Journal        string              `json:"journal"`
		Identity       string              `json:"identity"`
		SnapshotSHA256 string              `json:"snapshot_sha256"`
		Stats          SnapshotImportStats `json:"stats"`
	}{1, "database.db", "database.db.tlog", hex.EncodeToString(identity[:]), hex.EncodeToString(digest.Sum(nil)), stats})
	if err != nil {
		return stats, err
	}
	file, err := fs.OpenFile(filepath.Join(directory, "READY.tmp"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return stats, err
	}
	_, writeErr := file.Write(manifest)
	syncErr := file.Sync()
	closeErr := file.Close()
	if writeErr != nil {
		return stats, writeErr
	}
	if syncErr != nil {
		return stats, syncErr
	}
	if closeErr != nil {
		return stats, closeErr
	}
	if err = fs.Rename(filepath.Join(directory, "READY.tmp"), filepath.Join(directory, "READY")); err != nil {
		return stats, err
	}
	if err = syncDirectories(fs, filepath.Join(directory, "READY"), directory); err != nil {
		return stats, err
	}
	complete = true
	return stats, nil
}
