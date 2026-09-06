package storage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

var (
	ErrSnapshotInvalid       = errors.New("invalid snapshot")
	ErrSnapshotScopeMismatch = errors.New("snapshot scope mismatch")
)

const (
	snapshotVersion uint16 = 1

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
	Version          uint16
	Scope            snapshotScope
	SectionCount     uint32
	SourceBucketName []byte
}

type snapshotBucketSection struct {
	Name      []byte
	ItemCount uint64
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
	if !overwrite {
		if _, err := os.Stat(path); err == nil {
			return SnapshotExportStats{}, os.ErrExist
		} else if err != nil && !errors.Is(err, os.ErrNotExist) {
			return SnapshotExportStats{}, err
		}
	}

	dir := filepath.Dir(path)
	tmpFile, err := os.CreateTemp(dir, "."+filepath.Base(path)+".snapshot-*")
	if err != nil {
		return SnapshotExportStats{}, err
	}
	tmpPath := tmpFile.Name()
	defer func() {
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath)
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

	if overwrite {
		if removeErr := os.Remove(path); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			return SnapshotExportStats{}, removeErr
		}
	}
	if err = os.Rename(tmpPath, path); err != nil {
		return SnapshotExportStats{}, err
	}

	stats.BytesWritten = counter.n
	return stats, nil
}

func importSnapshotFromPath(db *DB, path string, targetScope snapshotScope, targetBucketName []byte) (SnapshotImportStats, error) {
	file, err := os.Open(path)
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
	switch scope {
	case snapshotScopeDB:
		buckets, err := tx.BucketsE()
		if err != nil {
			return err
		}
		header := snapshotHeader{
			Version:      snapshotVersion,
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
		seenBuckets := make(map[string]struct{}, header.SectionCount)
		for i := uint32(0); i < header.SectionCount; i++ {
			section, sectionErr := readSnapshotBucketSection(r)
			if sectionErr != nil {
				return sectionErr
			}
			if _, exists := seenBuckets[string(section.Name)]; exists {
				return fmt.Errorf("%w: duplicate bucket %q", ErrSnapshotInvalid, string(section.Name))
			}
			seenBuckets[string(section.Name)] = struct{}{}
			if err = importSnapshotBucketRecordsTx(tx, section.Name, section.ItemCount, r, stats); err != nil {
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
		return importSnapshotBucketRecordsTx(tx, targetBucketName, section.ItemCount, r, stats)
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

func importSnapshotBucketRecordsTx(tx *Tx, bucketName []byte, itemCount uint64, r io.Reader, stats *SnapshotImportStats) error {
	bucket, err := tx.CreateBucket(bucketName)
	if errors.Is(err, ErrBucketExists) {
		return fmt.Errorf("%w: bucket %q already exists during import", ErrSnapshotInvalid, string(bucketName))
	}
	if err != nil {
		return err
	}

	for i := uint64(0); i < itemCount; i++ {
		key, keyErr := readSnapshotBytes(r, uint64(MaxKeySize-1))
		if keyErr != nil {
			return keyErr
		}
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
		return snapshotHeader{}, fmt.Errorf("%w: unsupported snapshot version %d", ErrSnapshotInvalid, version)
	}

	var scope uint8
	if err := binary.Read(r, binary.LittleEndian, &scope); err != nil {
		return snapshotHeader{}, fmt.Errorf("%w: unable to read snapshot scope", ErrSnapshotInvalid)
	}
	var reserved uint8
	if err := binary.Read(r, binary.LittleEndian, &reserved); err != nil {
		return snapshotHeader{}, fmt.Errorf("%w: unable to read snapshot reserved byte", ErrSnapshotInvalid)
	}

	var sectionCount uint32
	if err := binary.Read(r, binary.LittleEndian, &sectionCount); err != nil {
		return snapshotHeader{}, fmt.Errorf("%w: unable to read snapshot section count", ErrSnapshotInvalid)
	}

	sourceBucketName, err := readSnapshotBytes32(r, uint32(MaxKeySize-1))
	if err != nil {
		return snapshotHeader{}, err
	}

	header := snapshotHeader{
		Version:          version,
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
	return binary.Write(w, binary.LittleEndian, section.ItemCount)
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
	return snapshotBucketSection{Name: name, ItemCount: itemCount}, nil
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
