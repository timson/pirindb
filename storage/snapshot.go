package storage

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

var (
	ErrSnapshotInvalid       = errors.New("invalid snapshot")
	ErrSnapshotScopeMismatch = errors.New("snapshot scope mismatch")
)

const (
	snapshotVersion uint16 = 2

	snapshotScopeDB     snapshotScope = 1
	snapshotScopeBucket snapshotScope = 2
)

var snapshotMagic = [8]byte{'P', 'I', 'R', 'I', 'N', 'S', 'N', 'P'}

type snapshotScope uint8

type SnapshotExportStats struct {
	BucketsExported uint64
	KeysExported    uint64
	BytesWritten    int64
}

type SnapshotImportStats struct {
	BucketsImported uint64
	KeysImported    uint64
	BytesRead       int64
}

type snapshotHeader struct {
	Version           uint16
	Scope             snapshotScope
	SectionCount      uint32
	SourceBucketName  []byte
	SourceDatabaseID  [16]byte
	CreatedAtUnixNano int64
}

type snapshotBucketSection struct {
	Name      []byte
	ItemCount uint64
	Sequence  uint64
}

type countingWriter struct {
	w io.Writer
	n int64
}

func (w *countingWriter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	w.n += int64(n)
	return n, err
}

type countingReader struct {
	r io.Reader
	n int64
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	r.n += int64(n)
	return n, err
}

func snapshotCloneBytes(src []byte) []byte {
	if src == nil {
		return nil
	}
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

func ExportDBToPath(db *DB, path string, overwrite bool) (SnapshotExportStats, error) {
	return exportSnapshotToPath(db, path, overwrite, snapshotScopeDB, nil)
}

func ExportBucketToPath(db *DB, bucketName []byte, path string, overwrite bool) (SnapshotExportStats, error) {
	if len(bucketName) == 0 {
		return SnapshotExportStats{}, fmt.Errorf("%w: bucket name is required", ErrSnapshotInvalid)
	}
	return exportSnapshotToPath(db, path, overwrite, snapshotScopeBucket, snapshotCloneBytes(bucketName))
}

func ImportDBFromPath(db *DB, path string) (SnapshotImportStats, error) {
	return importSnapshotFromPath(db, path, snapshotScopeDB, nil)
}

func ImportBucketFromPath(db *DB, bucketName []byte, path string) (SnapshotImportStats, error) {
	if len(bucketName) == 0 {
		return SnapshotImportStats{}, fmt.Errorf("%w: target bucket name is required", ErrSnapshotInvalid)
	}
	return importSnapshotFromPath(db, path, snapshotScopeBucket, snapshotCloneBytes(bucketName))
}

func exportSnapshotToPath(db *DB, path string, overwrite bool, scope snapshotScope, bucketName []byte) (SnapshotExportStats, error) {
	if err := db.validateSnapshotDestination(path); err != nil {
		return SnapshotExportStats{}, err
	}

	if !overwrite {
		if _, err := db.dal.fs.Stat(path); err == nil {
			return SnapshotExportStats{}, os.ErrExist
		} else if err != nil && !errors.Is(err, os.ErrNotExist) {
			return SnapshotExportStats{}, err
		}
	}

	dir := filepath.Dir(path)
	tmpFile, err := db.dal.fs.CreateTemp(dir, "."+filepath.Base(path)+".snapshot-*")
	if err != nil {
		return SnapshotExportStats{}, err
	}
	tmpPath := tmpFile.Name()
	defer func() {
		_ = tmpFile.Close()
		_ = db.dal.fs.Remove(tmpPath)
	}()

	bufferedWriter := bufio.NewWriter(tmpFile)
	counter := &countingWriter{w: bufferedWriter}
	stats := SnapshotExportStats{}
	err = db.View(func(tx *Tx) error {
		return writeSnapshotTx(tx, counter, scope, bucketName, &stats)
	})
	if err != nil {
		return SnapshotExportStats{}, err
	}

	if err = bufferedWriter.Flush(); err != nil {
		return SnapshotExportStats{}, err
	}
	if err = tmpFile.Sync(); err != nil {
		return SnapshotExportStats{}, err
	}
	if err = tmpFile.Close(); err != nil {
		return SnapshotExportStats{}, err
	}

	if err = db.validateSnapshotDestination(path); err != nil {
		return SnapshotExportStats{}, err
	}
	if overwrite {
		err = db.dal.fs.Rename(tmpPath, path)
	} else {
		err = db.dal.fs.Link(tmpPath, path)
	}
	if err != nil {
		return SnapshotExportStats{}, err
	}
	if err = syncDirectories(db.dal.fs, path); err != nil {
		return SnapshotExportStats{}, err
	}

	stats.BytesWritten = counter.n
	return stats, nil
}

func importSnapshotFromPath(db *DB, path string, targetScope snapshotScope, targetBucketName []byte) (SnapshotImportStats, error) {
	file, err := openStorageFile(db.dal.fs, path)
	if err != nil {
		return SnapshotImportStats{}, err
	}
	defer func() {
		_ = file.Close()
	}()

	bufferedReader := bufio.NewReader(file)
	counter := &countingReader{r: bufferedReader}
	stats := SnapshotImportStats{}
	err = db.Update(func(tx *Tx) error {
		return readSnapshotTx(tx, counter, targetScope, targetBucketName, &stats)
	})
	if err != nil {
		return SnapshotImportStats{}, err
	}

	stats.BytesRead = counter.n
	return stats, nil
}

func writeSnapshotTx(tx *Tx, w io.Writer, scope snapshotScope, bucketName []byte, stats *SnapshotExportStats) error {
	digest := sha256.New()
	if err := writeSnapshotPayload(tx, io.MultiWriter(w, digest), scope, bucketName, stats); err != nil {
		return err
	}
	_, err := w.Write(digest.Sum(nil))
	return err
}

func writeSnapshotPayload(tx *Tx, w io.Writer, scope snapshotScope, bucketName []byte, stats *SnapshotExportStats) error {
	switch scope {
	case snapshotScopeDB:
		buckets, err := tx.BucketsE()
		if err != nil {
			return err
		}
		header := snapshotHeader{
			Version:          snapshotVersion,
			SourceDatabaseID: tx.db.dal.meta.databaseID, CreatedAtUnixNano: time.Now().UnixNano(),
			Scope:        snapshotScopeDB,
			SectionCount: uint32(len(buckets)),
		}
		if err := writeSnapshotHeader(w, header); err != nil {
			return err
		}
		for _, name := range buckets {
			if err := writeSnapshotBucketTx(tx, w, name, stats); err != nil {
				return err
			}
		}
		return nil
	case snapshotScopeBucket:
		bucket, err := tx.GetBucket(bucketName)
		if err != nil {
			return err
		}
		header := snapshotHeader{
			Version:          snapshotVersion,
			SourceDatabaseID: tx.db.dal.meta.databaseID, CreatedAtUnixNano: time.Now().UnixNano(),
			Scope:            snapshotScopeBucket,
			SectionCount:     1,
			SourceBucketName: snapshotCloneBytes(bucketName),
		}
		if err = writeSnapshotHeader(w, header); err != nil {
			return err
		}
		return writeSnapshotBucket(w, bucketName, bucket, stats)
	default:
		return fmt.Errorf("%w: unsupported snapshot scope", ErrSnapshotInvalid)
	}
}

func writeSnapshotBucketTx(tx *Tx, w io.Writer, bucketName []byte, stats *SnapshotExportStats) error {
	bucket, err := tx.GetBucket(bucketName)
	if err != nil {
		return err
	}
	return writeSnapshotBucket(w, bucketName, bucket, stats)
}

func writeSnapshotBucket(w io.Writer, bucketName []byte, bucket *Bucket, stats *SnapshotExportStats) error {
	section := snapshotBucketSection{
		Name:      snapshotCloneBytes(bucketName),
		ItemCount: bucket.ItemCount(),
		Sequence:  bucket.Sequence(),
	}
	if err := writeSnapshotBucketSection(w, section); err != nil {
		return err
	}

	cursor := bucket.Cursor()
	for item := cursor.FirstItem(); item != nil; item = cursor.NextItem() {
		if err := writeSnapshotBytes(w, item.Key); err != nil {
			return err
		}
		if err := writeSnapshotItemValue(bucket.tx, w, item); err != nil {
			return err
		}
		stats.KeysExported++
	}
	if err := cursor.Err(); err != nil {
		return err
	}
	stats.BucketsExported++
	return nil
}

func readSnapshotTx(tx *Tx, r io.Reader, targetScope snapshotScope, targetBucketName []byte, stats *SnapshotImportStats) error {
	digest := sha256.New()
	if err := readSnapshotPayload(tx, io.TeeReader(r, digest), targetScope, targetBucketName, stats); err != nil {
		return err
	}
	return verifySnapshotDigest(r, digest.Sum(nil))
}

func verifySnapshotDigest(r io.Reader, actual []byte) error {
	var expected [sha256.Size]byte
	if _, err := io.ReadFull(r, expected[:]); err != nil {
		return fmt.Errorf("%w: missing content digest", ErrSnapshotInvalid)
	}
	if !bytes.Equal(expected[:], actual) {
		return fmt.Errorf("%w: content digest mismatch", ErrSnapshotInvalid)
	}
	var tail [1]byte
	if n, err := r.Read(tail[:]); n != 0 || err != io.EOF {
		return fmt.Errorf("%w: trailing snapshot data", ErrSnapshotInvalid)
	}
	return nil
}

func readSnapshotPayload(tx *Tx, r io.Reader, targetScope snapshotScope, targetBucketName []byte, stats *SnapshotImportStats) error {
	header, err := readSnapshotHeader(r)
	if err != nil {
		return err
	}
	if header.Scope != targetScope {
		return fmt.Errorf("%w: file scope %d does not match target scope %d", ErrSnapshotScopeMismatch, header.Scope, targetScope)
	}

	switch targetScope {
	case snapshotScopeDB:
		if err := replaceAllBucketsTx(tx); err != nil {
			return err
		}
		seenBuckets := make(map[string]struct{})
		for i := uint32(0); i < header.SectionCount; i++ {
			section, sectionErr := readSnapshotBucketSection(r)
			if sectionErr != nil {
				return sectionErr
			}
			if _, exists := seenBuckets[string(section.Name)]; exists {
				return fmt.Errorf("%w: duplicate bucket %q", ErrSnapshotInvalid, string(section.Name))
			}
			seenBuckets[string(section.Name)] = struct{}{}
			if err = importSnapshotBucketRecordsTx(tx, section.Name, section.ItemCount, section.Sequence, r, stats); err != nil {
				return err
			}
		}
		return nil
	case snapshotScopeBucket:
		if header.SectionCount != 1 {
			return fmt.Errorf("%w: bucket snapshot must contain exactly one bucket section", ErrSnapshotInvalid)
		}
		section, sectionErr := readSnapshotBucketSection(r)
		if sectionErr != nil {
			return sectionErr
		}
		if len(header.SourceBucketName) > 0 && string(header.SourceBucketName) != string(section.Name) {
			return fmt.Errorf("%w: bucket snapshot header does not match bucket section", ErrSnapshotInvalid)
		}
		if err := replaceBucketTx(tx, targetBucketName); err != nil {
			return err
		}
		return importSnapshotBucketRecordsTx(tx, targetBucketName, section.ItemCount, section.Sequence, r, stats)
	default:
		return fmt.Errorf("%w: unsupported import scope", ErrSnapshotInvalid)
	}
}

func replaceAllBucketsTx(tx *Tx) error {
	buckets, err := tx.BucketsE()
	if err != nil {
		return err
	}
	for _, bucketName := range buckets {
		if err := tx.DeleteBucket(bucketName); err != nil && !errors.Is(err, ErrBucketNotFound) {
			return err
		}
	}
	return nil
}

func replaceBucketTx(tx *Tx, bucketName []byte) error {
	if err := tx.DeleteBucket(bucketName); err != nil && !errors.Is(err, ErrBucketNotFound) {
		return err
	}
	return nil
}

func importSnapshotBucketRecordsTx(tx *Tx, bucketName []byte, itemCount uint64, sequence uint64, r io.Reader, stats *SnapshotImportStats) error {
	bucket, err := tx.CreateBucket(bucketName)
	if errors.Is(err, ErrBucketExists) {
		return fmt.Errorf("%w: bucket %q already exists during import", ErrSnapshotInvalid, string(bucketName))
	}
	if err != nil {
		return err
	}

	bucket.counter = sequence
	bucket.markDirty()
	var previous []byte
	for i := uint64(0); i < itemCount; i++ {
		key, keyErr := readSnapshotBytes(r, uint64(MaxKeySize-1))
		if keyErr != nil {
			return keyErr
		}
		if i > 0 && bytes.Compare(previous, key) >= 0 {
			return fmt.Errorf("%w: keys must be unique and strictly ordered", ErrSnapshotInvalid)
		}
		previous = key
		valueLen, valueErr := readSnapshotValueLength(r, uint64(OneGigabyte-1))
		if valueErr != nil {
			return valueErr
		}
		if err = bucket.PutReader(key, r, int64(valueLen)); err != nil {
			return err
		}
		stats.KeysImported++
	}
	stats.BucketsImported++
	return nil
}

func writeSnapshotHeader(w io.Writer, header snapshotHeader) error {
	if _, err := w.Write(snapshotMagic[:]); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, header.Version); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, uint8(header.Scope)); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, uint8(0)); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, header.SectionCount); err != nil {
		return err
	}
	if _, err := w.Write(header.SourceDatabaseID[:]); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, header.CreatedAtUnixNano); err != nil {
		return err
	}
	return writeSnapshotBytes32(w, header.SourceBucketName)
}

func readSnapshotHeader(r io.Reader) (snapshotHeader, error) {
	var magic [8]byte
	if _, err := io.ReadFull(r, magic[:]); err != nil {
		return snapshotHeader{}, fmt.Errorf("%w: unable to read snapshot magic", ErrSnapshotInvalid)
	}
	if magic != snapshotMagic {
		return snapshotHeader{}, fmt.Errorf("%w: unknown snapshot magic", ErrSnapshotInvalid)
	}

	var version uint16
	if err := binary.Read(r, binary.LittleEndian, &version); err != nil {
		return snapshotHeader{}, fmt.Errorf("%w: unable to read snapshot version", ErrSnapshotInvalid)
	}
	if version != snapshotVersion {
		return snapshotHeader{}, fmt.Errorf("%w: unsupported snapshot version %d (v2 with digest and sequences required)", ErrSnapshotInvalid, version)
	}

	var scope uint8
	if err := binary.Read(r, binary.LittleEndian, &scope); err != nil {
		return snapshotHeader{}, fmt.Errorf("%w: unable to read snapshot scope", ErrSnapshotInvalid)
	}
	var reserved uint8
	if err := binary.Read(r, binary.LittleEndian, &reserved); err != nil {
		return snapshotHeader{}, fmt.Errorf("%w: unable to read snapshot reserved byte", ErrSnapshotInvalid)
	}

	if reserved != 0 {
		return snapshotHeader{}, fmt.Errorf("%w: nonzero reserved byte", ErrSnapshotInvalid)
	}
	var sectionCount uint32
	if err := binary.Read(r, binary.LittleEndian, &sectionCount); err != nil {
		return snapshotHeader{}, fmt.Errorf("%w: unable to read snapshot section count", ErrSnapshotInvalid)
	}

	var sourceID [16]byte
	if _, err := io.ReadFull(r, sourceID[:]); err != nil {
		return snapshotHeader{}, fmt.Errorf("%w: missing source identity", ErrSnapshotInvalid)
	}
	var createdAt int64
	if err := binary.Read(r, binary.LittleEndian, &createdAt); err != nil {
		return snapshotHeader{}, fmt.Errorf("%w: missing creation time", ErrSnapshotInvalid)
	}
	sourceBucketName, err := readSnapshotBytes32(r, uint32(MaxKeySize-1))
	if err != nil {
		return snapshotHeader{}, err
	}

	header := snapshotHeader{
		Version:          version,
		SourceDatabaseID: sourceID, CreatedAtUnixNano: createdAt,
		Scope:            snapshotScope(scope),
		SectionCount:     sectionCount,
		SourceBucketName: sourceBucketName,
	}
	switch header.Scope {
	case snapshotScopeDB:
		if len(header.SourceBucketName) != 0 {
			return snapshotHeader{}, fmt.Errorf("%w: db snapshot must not include a source bucket name", ErrSnapshotInvalid)
		}
	case snapshotScopeBucket:
		if len(header.SourceBucketName) == 0 {
			return snapshotHeader{}, fmt.Errorf("%w: bucket snapshot must include a source bucket name", ErrSnapshotInvalid)
		}
	default:
		return snapshotHeader{}, fmt.Errorf("%w: unknown snapshot scope %d", ErrSnapshotInvalid, header.Scope)
	}

	return header, nil
}

func writeSnapshotBucketSection(w io.Writer, section snapshotBucketSection) error {
	if len(section.Name) == 0 {
		return fmt.Errorf("%w: bucket name is required", ErrSnapshotInvalid)
	}
	if err := writeSnapshotBytes32(w, section.Name); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, section.ItemCount); err != nil {
		return err
	}
	return binary.Write(w, binary.LittleEndian, section.Sequence)
}

func readSnapshotBucketSection(r io.Reader) (snapshotBucketSection, error) {
	name, err := readSnapshotBytes32(r, uint32(MaxKeySize-1))
	if err != nil {
		return snapshotBucketSection{}, err
	}
	if len(name) == 0 {
		return snapshotBucketSection{}, fmt.Errorf("%w: empty bucket name", ErrSnapshotInvalid)
	}

	var itemCount uint64
	if err := binary.Read(r, binary.LittleEndian, &itemCount); err != nil {
		return snapshotBucketSection{}, fmt.Errorf("%w: unable to read bucket item count", ErrSnapshotInvalid)
	}
	var sequence uint64
	if err := binary.Read(r, binary.LittleEndian, &sequence); err != nil {
		return snapshotBucketSection{}, fmt.Errorf("%w: missing bucket sequence", ErrSnapshotInvalid)
	}
	return snapshotBucketSection{Name: name, ItemCount: itemCount, Sequence: sequence}, nil
}

func writeSnapshotBytes(w io.Writer, data []byte) error {
	if err := binary.Write(w, binary.LittleEndian, uint64(len(data))); err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	_, err := w.Write(data)
	return err
}

func writeSnapshotItemValue(tx *Tx, w io.Writer, item *Item) error {
	valueLen, err := item.valueLen(tx)
	if err != nil {
		return err
	}
	if err = binary.Write(w, binary.LittleEndian, uint64(valueLen)); err != nil {
		return err
	}
	if valueLen == 0 {
		return nil
	}
	_, err = item.writeValueTo(tx, w)
	return err
}

func writeSnapshotBytes32(w io.Writer, data []byte) error {
	if err := binary.Write(w, binary.LittleEndian, uint32(len(data))); err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	_, err := w.Write(data)
	return err
}

func readSnapshotBytes(r io.Reader, maxLen uint64) ([]byte, error) {
	length, err := readSnapshotValueLength(r, maxLen)
	if err != nil {
		return nil, err
	}
	data := make([]byte, int(length))
	if length == 0 {
		return data, nil
	}
	if _, err = io.ReadFull(r, data); err != nil {
		return nil, fmt.Errorf("%w: unable to read value bytes", ErrSnapshotInvalid)
	}
	return data, nil
}

func readSnapshotValueLength(r io.Reader, maxLen uint64) (uint64, error) {
	var length uint64
	if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
		return 0, fmt.Errorf("%w: unable to read value length", ErrSnapshotInvalid)
	}
	if length > maxLen {
		return 0, fmt.Errorf("%w: value length %d exceeds maximum %d", ErrSnapshotInvalid, length, maxLen)
	}
	return length, nil
}

func readSnapshotBytes32(r io.Reader, maxLen uint32) ([]byte, error) {
	var length uint32
	if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
		return nil, fmt.Errorf("%w: unable to read byte slice length", ErrSnapshotInvalid)
	}
	if length > maxLen {
		return nil, fmt.Errorf("%w: byte slice length %d exceeds maximum %d", ErrSnapshotInvalid, length, maxLen)
	}
	data := make([]byte, int(length))
	if length == 0 {
		return data, nil
	}
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, fmt.Errorf("%w: unable to read byte slice", ErrSnapshotInvalid)
	}
	return data, nil
}

// Resolve the parent as well as the final component: replacing a symlink itself
// is harmless to its target, but accepting active-file aliases is confusing.
func canonicalPathWithFS(fs storageFS, path string) (string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	parent, err := fs.EvalSymlinks(filepath.Dir(absolute))
	if err != nil {
		return "", err
	}
	resolved := filepath.Join(parent, filepath.Base(absolute))
	if target, err := fs.EvalSymlinks(resolved); err == nil {
		return target, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	return resolved, nil
}
func (db *DB) validateSnapshotDestination(path string) error {
	if db == nil || db.dal == nil {
		return ErrDatabaseClosed
	}
	candidate, err := canonicalPathWithFS(db.dal.fs, path)
	if err != nil {
		return err
	}
	for _, active := range []string{db.dal.path, db.dal.opts.TxLogPath} {
		protected, err := canonicalPathWithFS(db.dal.fs, active)
		if err != nil {
			return err
		}
		if protected == candidate {
			return fmt.Errorf("snapshot destination is an active database or journal")
		}
		candidateInfo, candidateErr := db.dal.fs.Stat(candidate)
		protectedInfo, protectedErr := db.dal.fs.Stat(protected)
		if candidateErr == nil && protectedErr == nil && os.SameFile(candidateInfo, protectedInfo) {
			return fmt.Errorf("snapshot destination aliases active storage")
		}
	}
	return nil
}
