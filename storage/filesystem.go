package storage

import (
	"io"
	"os"
	"path/filepath"

	"github.com/gofrs/flock"
)

type storageFile interface {
	io.Reader
	io.Writer
	io.ReaderAt
	io.WriterAt
	Stat() (os.FileInfo, error)
	Sync() error
	Truncate(int64) error
	Close() error
	Name() string
}
type storageLock interface {
	TryLock() (bool, error)
	Unlock() error
}
type storageFS interface {
	OpenFile(string, int, os.FileMode) (storageFile, error)
	Stat(string) (os.FileInfo, error)
	Remove(string) error
	RemoveAll(string) error
	Mkdir(string, os.FileMode) error
	CreateTemp(string, string) (storageFile, error)
	Rename(string, string) error
	Link(string, string) error
	EvalSymlinks(string) (string, error)
	Lock(string) storageLock
}
type osStorageFS struct{}

func (osStorageFS) OpenFile(p string, f int, m os.FileMode) (storageFile, error) {
	return os.OpenFile(p, f, m)
}
func (osStorageFS) Stat(p string) (os.FileInfo, error)          { return os.Stat(p) }
func (osStorageFS) Remove(p string) error                       { return os.Remove(p) }
func (osStorageFS) RemoveAll(p string) error                    { return os.RemoveAll(p) }
func (osStorageFS) Mkdir(p string, m os.FileMode) error         { return os.Mkdir(p, m) }
func (osStorageFS) CreateTemp(d, p string) (storageFile, error) { return os.CreateTemp(d, p) }
func (osStorageFS) Rename(a, b string) error                    { return os.Rename(a, b) }
func (osStorageFS) Link(a, b string) error                      { return os.Link(a, b) }
func (osStorageFS) EvalSymlinks(p string) (string, error)       { return filepath.EvalSymlinks(p) }
func (osStorageFS) Lock(p string) storageLock                   { return flock.New(p) }
func openStorageFile(fs storageFS, p string) (storageFile, error) {
	return fs.OpenFile(p, os.O_RDONLY, 0)
}
func syncDirectories(fs storageFS, paths ...string) error {
	seen := map[string]bool{}
	for _, p := range paths {
		d, err := filepath.Abs(filepath.Dir(p))
		if err != nil {
			return err
		}
		if seen[d] {
			continue
		}
		seen[d] = true
		f, err := openStorageFile(fs, d)
		if err != nil {
			return err
		}
		syncErr := f.Sync()
		closeErr := f.Close()
		if syncErr != nil {
			return syncErr
		}
		if closeErr != nil {
			return closeErr
		}
	}
	return nil
}
