package storage

import (
	"errors"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
)

// modelDisk uses real files for volatile reads, with an independent durable
// inode/namespace model. Crash reconstructs ONLY modeled persistent state.
type diskImage struct {
	data []byte
	dir  bool
}
type diskOp struct {
	id       int
	offset   int64
	data     []byte
	truncate bool
	size     int64
}
type diskEvent struct {
	Offset   int64
	Data     []byte
	Number   int
	Op, Path string
	Size     int64
}
type modelDisk struct {
	locks map[string]bool
	osStorageFS
	mu                  sync.Mutex
	root                string
	names, durableNames map[string]int
	stable              map[int]diskImage
	pending             []diskOp
	handles             []*modelFile
	trace               []diskEvent
	next, cut           int
	off                 bool
	fault               string
}
type modelFile struct {
	storageFile
	disk *modelDisk
	id   int
	dir  bool
}
type modelLock struct {
	disk *modelDisk
	path string
	held bool
}

func (l *modelLock) TryLock() (bool, error) {
	l.disk.mu.Lock()
	defer l.disk.mu.Unlock()
	if l.disk.locks[l.path] {
		return false, nil
	}
	l.disk.locks[l.path] = true
	l.held = true
	return true, nil
}
func (l *modelLock) Unlock() error {
	l.disk.mu.Lock()
	defer l.disk.mu.Unlock()
	if l.held {
		delete(l.disk.locks, l.path)
		l.held = false
	}
	return nil
}
func newModelDisk(root string) *modelDisk {
	return &modelDisk{locks: map[string]bool{}, root: root, names: map[string]int{}, durableNames: map[string]int{}, stable: map[int]diskImage{}}
}
func (d *modelDisk) Lock(p string) storageLock { return &modelLock{disk: d, path: p} }
func (d *modelDisk) event(op, p string, n int64) {
	d.trace = append(d.trace, diskEvent{Number: len(d.trace) + 1, Op: op, Path: p, Size: n})
	if d.cut > 0 && len(d.trace) == d.cut {
		d.off = true
		return
	}
}
func (d *modelDisk) check() error {
	if d.off {
		return syscall.EIO
	}
	return nil
}
func (d *modelDisk) register(p string, dir bool) int {
	if id, ok := d.names[p]; ok {
		return id
	}
	d.next++
	d.names[p] = d.next
	d.stable[d.next] = diskImage{dir: dir}
	return d.next
}
func (d *modelDisk) OpenFile(p string, flags int, mode os.FileMode) (storageFile, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if e := d.check(); e != nil {
		return nil, e
	}
	f, e := d.osStorageFS.OpenFile(p, flags, mode)
	if e != nil {
		return nil, e
	}
	info, e := f.Stat()
	if e != nil {
		return nil, e
	}
	id := d.register(p, info.IsDir())
	m := &modelFile{f, d, id, info.IsDir()}
	d.handles = append(d.handles, m)
	d.event("open", p, 0)
	return m, nil
}
func (d *modelDisk) CreateTemp(dir, pattern string) (storageFile, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if e := d.check(); e != nil {
		return nil, e
	}
	f, e := d.osStorageFS.CreateTemp(dir, pattern)
	if e != nil {
		return nil, e
	}
	id := d.register(f.Name(), false)
	m := &modelFile{f, d, id, false}
	d.handles = append(d.handles, m)
	d.event("create", f.Name(), 0)
	return m, nil
}
func (d *modelDisk) Mkdir(p string, m os.FileMode) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if e := d.check(); e != nil {
		return e
	}
	if e := os.Mkdir(p, m); e != nil {
		return e
	}
	d.register(p, true)
	d.event("mkdir", p, 0)
	return nil
}
func (d *modelDisk) Remove(p string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if e := d.check(); e != nil {
		return e
	}
	if e := os.Remove(p); e != nil {
		return e
	}
	delete(d.names, p)
	d.event("remove", p, 0)
	return nil
}
func (d *modelDisk) RemoveAll(p string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if e := d.check(); e != nil {
		return e
	}
	if e := os.RemoveAll(p); e != nil {
		return e
	}
	for name := range d.names {
		rel, e := filepath.Rel(p, name)
		if e == nil && rel != ".." && !filepath.IsAbs(rel) && (rel == "." || len(rel) < 3 || rel[:3] != "../") {
			delete(d.names, name)
		}
	}
	d.event("remove-all", p, 0)
	return nil
}
func (d *modelDisk) Rename(a, b string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if e := d.check(); e != nil {
		return e
	}
	if e := os.Rename(a, b); e != nil {
		return e
	}
	d.names[b] = d.names[a]
	delete(d.names, a)
	d.event("rename", b, 0)
	return nil
}
func (d *modelDisk) Link(a, b string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if e := d.check(); e != nil {
		return e
	}
	if e := os.Link(a, b); e != nil {
		return e
	}
	d.names[b] = d.names[a]
	d.event("link", b, 0)
	return nil
}
func (f *modelFile) WriteAt(p []byte, o int64) (int, error) {
	d := f.disk
	d.mu.Lock()
	defer d.mu.Unlock()
	if e := d.check(); e != nil {
		return 0, e
	}
	if d.fault == "write" {
		d.fault = ""
		return 0, syscall.ENOSPC
	}
	short := d.fault == "short"
	if short {
		d.fault = ""
		p = p[:len(p)/2]
	}
	n, e := f.storageFile.WriteAt(p, o)
	if n > 0 {
		d.pending = append(d.pending, diskOp{id: f.id, offset: o, data: append([]byte(nil), p[:n]...)})
	}
	d.event("write", f.Name(), int64(n))
	d.trace[len(d.trace)-1].Offset = o
	d.trace[len(d.trace)-1].Data = append([]byte(nil), p[:n]...)
	if short {
		return n, io.ErrShortWrite
	}
	if d.off {
		return n, syscall.EIO
	}
	return n, e
}
func (f *modelFile) Write(p []byte) (int, error) { // Snapshot writes are sequential.
	info, e := f.Stat()
	if e != nil {
		return 0, e
	}
	return f.WriteAt(p, info.Size())
}
func (f *modelFile) Truncate(n int64) error {
	d := f.disk
	d.mu.Lock()
	defer d.mu.Unlock()
	if e := d.check(); e != nil {
		return e
	}
	if e := f.storageFile.Truncate(n); e != nil {
		return e
	}
	d.pending = append(d.pending, diskOp{id: f.id, truncate: true, size: n})
	d.event("truncate", f.Name(), n)
	return d.check()
}
func (f *modelFile) Sync() error {
	d := f.disk
	d.mu.Lock()
	defer d.mu.Unlock()
	if e := d.check(); e != nil {
		return e
	}
	if d.fault == "sync" {
		d.fault = ""
		return syscall.EIO
	}
	if f.dir {
		for p := range d.durableNames {
			if filepath.Dir(p) == f.Name() {
				delete(d.durableNames, p)
			}
		}
		for p, id := range d.names {
			if filepath.Dir(p) == f.Name() {
				d.durableNames[p] = id
			}
		}
	} else {
		info, e := f.Stat()
		if e != nil {
			return e
		}
		data := make([]byte, info.Size())
		if _, e = f.ReadAt(data, 0); e != nil && !errors.Is(e, io.EOF) {
			return e
		}
		d.stable[f.id] = diskImage{data: data}
		remaining := d.pending[:0]
		for _, op := range d.pending {
			if op.id != f.id {
				remaining = append(remaining, op)
			}
		}
		d.pending = remaining
	}
	d.event("sync", f.Name(), 0)
	return d.check()
}
func applyDiskOp(images map[int]diskImage, op diskOp) {
	im := images[op.id]
	if op.truncate {
		if op.size < int64(len(im.data)) {
			im.data = im.data[:op.size]
		} else {
			im.data = append(im.data, make([]byte, int(op.size)-len(im.data))...)
		}
	} else {
		end := int(op.offset) + len(op.data)
		if end > len(im.data) {
			im.data = append(im.data, make([]byte, end-len(im.data))...)
		}
		copy(im.data[int(op.offset):], op.data)
	}
	images[op.id] = im
}
func (d *modelDisk) crash(seed int64, unit int, persist bool) error {
	for _, f := range d.handles {
		_ = f.storageFile.Close()
	}
	d.handles = nil
	if persist {
		// Namespace publication is atomic at operation boundaries in this
		// model; sample the volatile directory image or its synced image.
		namespaceRandom := rand.New(rand.NewSource(seed + 71))
		dirs := map[string]bool{}
		for p := range d.names {
			dirs[filepath.Dir(p)] = true
		}
		for p := range d.durableNames {
			dirs[filepath.Dir(p)] = true
		}
		ordered := []string{}
		for dir := range dirs {
			ordered = append(ordered, dir)
		}
		sort.Strings(ordered)
		for _, dir := range ordered {
			if namespaceRandom.Intn(2) == 0 {
				for p := range d.durableNames {
					if filepath.Dir(p) == dir {
						delete(d.durableNames, p)
					}
				}
				for p, id := range d.names {
					if filepath.Dir(p) == dir {
						d.durableNames[p] = id
					}
				}
			}
		}

		r := rand.New(rand.NewSource(seed))
		ops := []diskOp{}
		for _, op := range d.pending {
			if op.truncate {
				ops = append(ops, op)
				continue
			}
			for start := 0; start < len(op.data); start += unit {
				end := min(start+unit, len(op.data))
				part := op
				part.offset += int64(start)
				part.data = op.data[start:end]
				ops = append(ops, part)
			}
		}
		r.Shuffle(len(ops), func(i, j int) { ops[i], ops[j] = ops[j], ops[i] })
		for _, op := range ops {
			if r.Intn(2) == 0 {
				applyDiskOp(d.stable, op)
			}
		}
	}
	entries, e := os.ReadDir(d.root)
	if e != nil {
		return e
	}
	for _, entry := range entries {
		if e = os.RemoveAll(filepath.Join(d.root, entry.Name())); e != nil {
			return e
		}
	}
	paths := []string{}
	for p := range d.durableNames {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	aliases := map[int]string{}
	reachable := map[string]bool{d.root: true}
	for _, p := range paths {
		if !reachable[filepath.Dir(p)] {
			continue
		}
		reachable[p] = true
		id := d.durableNames[p]
		im := d.stable[id]
		if im.dir {
			if e = os.Mkdir(p, 0700); e != nil {
				return e
			}
			continue
		}

		if a, ok := aliases[id]; ok {
			e = os.Link(a, p)
		} else {
			e = os.WriteFile(p, im.data, 0600)
			aliases[id] = p
		}
		if e != nil {
			return e
		}
	}
	d.names = map[string]int{}
	for p, id := range d.durableNames {
		if reachable[p] {
			d.names[p] = id
		}
	}
	d.pending = nil
	d.locks = map[string]bool{}
	d.off = false
	d.cut = 0
	d.fault = ""
	return nil
}
