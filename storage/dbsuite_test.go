package storage

import (
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/require"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type suiteCase struct {
	Persist   bool
	Seed      int64
	Cut, Unit int
	Policy    SyncPolicy
}

func suiteEnabled(t *testing.T) {
	t.Helper()
	if os.Getenv("PIRIN_DBTEST") == "" {
		t.Skip("run make test-db")
	}
}
func suiteSeed() int64 { n, _ := strconv.ParseInt(os.Getenv("PIRIN_DBTEST_SEED"), 10, 64); return n }
func suiteRoot(t *testing.T) string {
	t.Helper()
	parent := os.Getenv("PIRIN_DBTEST_ARTIFACTS")
	if parent == "" {
		return t.TempDir()
	}
	root, e := os.MkdirTemp(parent, "case-")
	require.NoError(t, e)
	meta, _ := json.Marshal(struct {
		Test                               string
		Seed                               int64
		Profile, Duration, Policy, Workers string
	}{t.Name(), suiteSeed(), os.Getenv("PIRIN_DBTEST_PROFILE"), os.Getenv("PIRIN_DBTEST_DURATION"), os.Getenv("PIRIN_DBTEST_POLICY"), os.Getenv("PIRIN_DBTEST_WORKERS")})
	require.NoError(t, os.WriteFile(filepath.Join(root, "scenario.json"), meta, 0600))
	t.Cleanup(func() {
		if !t.Failed() && os.Getenv("PIRIN_DBTEST_KEEP") != "1" {
			_ = os.RemoveAll(root)
		}
	})
	return root
}
func bankUpdate(db *DB, n int) error {
	return db.Update(func(tx *Tx) error {
		b, e := tx.CreateBucketIfNotExists([]byte("bank"))
		if e != nil {
			return e
		}
		if e = b.Put([]byte("left"), []byte(strconv.Itoa(n))); e != nil {
			return e
		}
		if e = b.Put([]byte("right"), []byte(strconv.Itoa(100-n))); e != nil {
			return e
		}
		if n%2 == 0 {
			_, e = b.NextSequence()
		}
		return e
	})
}
func checkBank(t *testing.T, db *DB, minimum, maximum int) {
	t.Helper()
	require.NoError(t, db.View(func(tx *Tx) error {
		b, e := tx.GetBucket([]byte("bank"))
		if e != nil {
			return e
		}
		a, ok, e := b.GetE([]byte("left"))
		if e != nil {
			return e
		}
		require.True(t, ok)
		z, ok, e := b.GetE([]byte("right"))
		if e != nil {
			return e
		}
		require.True(t, ok)
		left, e := strconv.Atoi(string(a))
		if e != nil {
			return e
		}
		right, e := strconv.Atoi(string(z))
		if e != nil {
			return e
		}
		require.Equal(t, 100, left+right)
		require.GreaterOrEqual(t, left, minimum)
		require.LessOrEqual(t, left, maximum)
		return nil
	}))
	report, e := db.Check()
	require.NoError(t, e)
	require.Zero(t, report.OrphanPageCount)
}
func runDiskCase(t *testing.T, c suiteCase) int {
	root := suiteRoot(t)
	diskRoot := filepath.Join(root, "disk")
	require.NoError(t, os.Mkdir(diskRoot, 0700))
	d := newModelDisk(diskRoot)
	opts := DefaultOptions().WithSyncPolicy(c.Policy).WithCheckpointTxThreshold(2)
	db, e := openWithFS(filepath.Join(diskRoot, "bank.db"), opts, d)
	require.NoError(t, e)
	require.NoError(t, bankUpdate(db, 0))
	d.trace = nil
	d.cut = c.Cut
	ack, attempt := 0, 0
	for i := 1; i <= 3; i++ {
		attempt = i
		if e = bankUpdate(db, i); e != nil {
			break
		}
		ack = i
	}
	count := len(d.trace)
	if c.Cut > 0 {
		require.True(t, d.off, "requested cut was not reached")
	}
	if replay := os.Getenv("PIRIN_DBTEST_REPLAY"); replay != "" {
		raw, err := os.ReadFile(filepath.Join(replay, "trace.json"))
		require.NoError(t, err)
		var expected []diskEvent
		require.NoError(t, json.Unmarshal(raw, &expected))
		require.GreaterOrEqual(t, len(expected), c.Cut)
		for i := 0; i < c.Cut; i++ {
			a, b := expected[i], d.trace[i]
			require.Equal(t, a.Op, b.Op)
			require.Equal(t, filepath.Base(a.Path), filepath.Base(b.Path))
			require.Equal(t, a.Offset, b.Offset)
			require.Equal(t, a.Size, b.Size)
		}
	}
	config, _ := json.MarshalIndent(c, "", "  ")
	require.NoError(t, os.WriteFile(filepath.Join(root, "case.json"), config, 0600))
	trace, _ := json.MarshalIndent(d.trace, "", "  ")
	require.NoError(t, os.WriteFile(filepath.Join(root, "trace.json"), trace, 0600))
	require.NoError(t, d.crash(c.Seed, c.Unit, c.Persist))
	recovered, e := openWithFS(filepath.Join(diskRoot, "bank.db"), opts, d)
	require.NoError(t, e, "case %+v artifacts %s", c, root)
	defer recovered.Close()
	checkBank(t, recovered, ack, attempt)
	return count
}
func TestDBSuitePersistence(t *testing.T) {
	suiteEnabled(t)
	if replay := os.Getenv("PIRIN_DBTEST_REPLAY"); replay != "" {
		raw, e := os.ReadFile(filepath.Join(replay, "case.json"))
		require.NoError(t, e)
		var c suiteCase
		require.NoError(t, json.Unmarshal(raw, &c))
		require.Greater(t, c.Cut, 0)
		require.Contains(t, []int{512, 4096}, c.Unit)
		require.Contains(t, []SyncPolicy{SyncPolicyStrict, SyncPolicyJournal, SyncPolicyGroup}, c.Policy)
		runDiskCase(t, c)
		return
	}
	rounds := 1
	if os.Getenv("PIRIN_DBTEST_PROFILE") == "extended" {
		rounds = 8
	}
	for _, policy := range []SyncPolicy{SyncPolicyStrict, SyncPolicyJournal, SyncPolicyGroup} {
		total := runDiskCase(t, suiteCase{Seed: suiteSeed(), Cut: 0, Unit: 512, Policy: policy})
		for round := 0; round < rounds; round++ {
			for _, unit := range []int{512, 4096} {
				for cut := 1; cut <= total; cut++ {
					for _, persist := range []bool{false, true} {
						c := suiteCase{Seed: suiteSeed() + int64(round), Cut: cut, Unit: unit, Policy: policy, Persist: persist}
						t.Run(fmt.Sprintf("%d/%d/%d/%d/%v", policy, round, unit, cut, persist), func(t *testing.T) { runDiskCase(t, c) })
					}
				}
			}
		}
	}
}
func TestDBSuiteDiskModel(t *testing.T) {
	suiteEnabled(t)
	root := suiteRoot(t)
	d := newModelDisk(root)
	file, e := d.OpenFile(filepath.Join(root, "f"), os.O_CREATE|os.O_RDWR, 0600)
	require.NoError(t, e)
	_, e = file.Write([]byte("durable"))
	require.NoError(t, e)
	require.NoError(t, file.Sync())
	require.NoError(t, syncDirectories(d, file.Name()))
	_, e = file.WriteAt([]byte("lost"), 0)
	require.NoError(t, e)
	require.NoError(t, d.crash(1, 512, false))
	got, e := os.ReadFile(filepath.Join(root, "f"))
	require.NoError(t, e)
	require.Equal(t, "durable", string(got))
	// A file sync cannot substitute for durable directory publication.
	file, e = d.OpenFile(filepath.Join(root, "unpublished"), os.O_CREATE|os.O_RDWR, 0600)
	require.NoError(t, e)
	_, e = file.Write([]byte("x"))
	require.NoError(t, e)
	require.NoError(t, file.Sync())
	require.NoError(t, d.crash(1, 512, false))
	_, e = os.Stat(filepath.Join(root, "unpublished"))
	require.ErrorIs(t, e, os.ErrNotExist)
}
func TestDBSuiteWorkload(t *testing.T) {
	suiteEnabled(t)
	root := suiteRoot(t)
	db, e := Open(filepath.Join(root, "work.db"), suiteOptions())
	require.NoError(t, e)
	defer func() { _ = db.Close() }()
	r := rand.New(rand.NewSource(suiteSeed()))
	model := map[string]string{}
	duration := time.Second * 2
	if os.Getenv("PIRIN_DBTEST_PROFILE") == "extended" {
		duration = 10 * time.Second
	}
	if os.Getenv("PIRIN_DBTEST_PROFILE") == "soak" {
		duration, _ = time.ParseDuration(os.Getenv("PIRIN_DBTEST_DURATION"))
	}
	deadline := time.Now().Add(duration)
	iteration := 0
	for time.Now().Before(deadline) {
		iteration++
		require.NoError(t, db.Update(func(tx *Tx) error {
			b, e := tx.CreateBucketIfNotExists([]byte("model"))
			if e != nil {
				return e
			}
			for i := 0; i < 10; i++ {
				k := fmt.Sprint(r.Intn(1000))
				if r.Intn(3) == 0 {
					if _, exists := model[k]; !exists {
						continue
					}
					if e = b.Remove([]byte(k)); e != nil {
						return e
					}
					delete(model, k)
				} else {
					v := fmt.Sprintf("%d:%d", iteration, r.Int())
					if e = b.Put([]byte(k), []byte(v)); e != nil {
						return e
					}
					model[k] = v
				}
			}
			return nil
		}))
		if iteration%20 == 0 {
			require.NoError(t, db.View(func(tx *Tx) error {
				b, e := tx.GetBucket([]byte("model"))
				if e != nil {
					return e
				}
				actual := map[string]string{}
				e = b.ForEach(func(k, v []byte) error { actual[string(k)] = string(v); return nil })
				require.Equal(t, model, actual)
				return e
			}))
			_, e = db.Check()
			require.NoError(t, e)
		}
		if iteration%100 == 0 {
			snap := filepath.Join(root, "backup.snapshot")
			_, e = ExportDBToPath(db, snap, true)
			require.NoError(t, e)
			dir := filepath.Join(root, "restore")
			_ = os.RemoveAll(dir)
			_, e = RestoreDBToDirectory(snap, dir, nil)
			require.NoError(t, e)
			restored, e := Open(filepath.Join(dir, "database.db"), nil)
			require.NoError(t, e)
			require.NoError(t, restored.View(func(tx *Tx) error {
				b, e := tx.GetBucket([]byte("model"))
				if e != nil {
					return e
				}
				actual := map[string]string{}
				e = b.ForEach(func(k, v []byte) error { actual[string(k)] = string(v); return nil })
				require.Equal(t, model, actual)
				return e
			}))
			require.NoError(t, restored.Close())
			require.NoError(t, db.Close())
			db, e = Open(filepath.Join(root, "work.db"), suiteOptions())
			require.NoError(t, e)
		}
	}
	require.Positive(t, iteration)
	snapshot := filepath.Join(root, "final.snapshot")
	_, e = ExportDBToPath(db, snapshot, false)
	require.NoError(t, e)
	finalDir := filepath.Join(root, "final-restore")
	_, e = RestoreDBToDirectory(snapshot, finalDir, nil)
	require.NoError(t, e)
	restored, e := Open(filepath.Join(finalDir, "database.db"), nil)
	require.NoError(t, e)
	defer restored.Close()
	for _, checked := range []*DB{db, restored} {
		require.NoError(t, checked.View(func(tx *Tx) error {
			b, e := tx.GetBucket([]byte("model"))
			if e != nil {
				return e
			}
			actual := map[string]string{}
			e = b.ForEach(func(k, v []byte) error { actual[string(k)] = string(v); return nil })
			require.Equal(t, model, actual)
			return e
		}))
		_, e = checked.Check()
		require.NoError(t, e)
	}
	t.Logf("completed %d transactions seed=%d", iteration, suiteSeed())
}

type historyOp struct {
	Start, End int64
	Write      bool
	Result     int
}

func serialHistory(history []historyOp, state int, used uint64) bool {
	if used == (uint64(1)<<len(history))-1 {
		return true
	}
	for i, op := range history {
		if used&(1<<i) != 0 {
			continue
		}
		blocked := false
		for j, earlier := range history {
			if used&(1<<j) == 0 && earlier.End < op.Start {
				blocked = true
				break
			}
		}
		if blocked {
			continue
		}
		next := state
		if op.Write {
			next++
		}
		if op.Result != next {
			continue
		}
		if serialHistory(history, next, used|(1<<i)) {
			return true
		}
	}
	return false
}
func TestDBSuiteIsolation(t *testing.T) {
	suiteEnabled(t)
	require.False(t, serialHistory([]historyOp{{1, 2, true, 1}, {3, 4, false, 0}}, 0, 0), "checker must detect stale reads")
	for _, policy := range []SyncPolicy{SyncPolicyStrict, SyncPolicyJournal, SyncPolicyGroup} {
		db, e := Open(filepath.Join(suiteRoot(t), "concurrent.db"), DefaultOptions().WithSyncPolicy(policy))
		require.NoError(t, e)
		require.NoError(t, db.Update(func(tx *Tx) error {
			b, e := tx.CreateBucket([]byte("counter"))
			if e != nil {
				return e
			}
			return b.Put([]byte("n"), []byte("0"))
		}))
		var clock atomic.Int64
		var mu sync.Mutex
		history := []historyOp{}
		var wg sync.WaitGroup
		errs := make(chan error, 4)
		for worker := 0; worker < 4; worker++ {
			wg.Add(1)
			go func(worker int) {
				defer wg.Done()
				for step := 0; step < 2; step++ {
					op := historyOp{Start: clock.Add(1), Write: (worker+step)%2 == 0}
					action := func(tx *Tx) error {
						b, e := tx.GetBucket([]byte("counter"))
						if e != nil {
							return e
						}
						raw, _, e := b.GetE([]byte("n"))
						if e != nil {
							return e
						}
						n, e := strconv.Atoi(string(raw))
						if e != nil {
							return e
						}
						if op.Write {
							n++
							if e = b.Put([]byte("n"), []byte(strconv.Itoa(n))); e != nil {
								return e
							}
						}
						op.Result = n
						return nil
					}
					var e error
					if op.Write {
						e = db.Update(action)
					} else {
						e = db.View(action)
					}
					if e != nil {
						errs <- e
						return
					}
					op.End = clock.Add(1)
					mu.Lock()
					history = append(history, op)
					mu.Unlock()
				}
			}(worker)
		}
		wg.Wait()
		close(errs)
		for e := range errs {
			require.NoError(t, e)
		}
		require.True(t, serialHistory(history, 0, 0), "nonserial history: %+v", history)
		require.NoError(t, db.Close())
	}
}

func suiteOptions() *Options {
	p := SyncPolicyStrict
	switch os.Getenv("PIRIN_DBTEST_POLICY") {
	case "journal":
		p = SyncPolicyJournal
	case "group":
		p = SyncPolicyGroup
	}
	return DefaultOptions().WithSyncPolicy(p)
}
func TestDBSuiteIOFaults(t *testing.T) {
	suiteEnabled(t)
	for _, policy := range []SyncPolicy{SyncPolicyStrict, SyncPolicyJournal, SyncPolicyGroup} {
		for _, fault := range []string{"write", "short", "sync"} {
			t.Run(fmt.Sprintf("%d/%s", policy, fault), func(t *testing.T) {
				root := suiteRoot(t)
				d := newModelDisk(root)
				opts := DefaultOptions().WithSyncPolicy(policy)
				path := filepath.Join(root, "db")
				db, e := openWithFS(path, opts, d)
				require.NoError(t, e)
				require.NoError(t, bankUpdate(db, 0))
				d.fault = fault
				require.Error(t, bankUpdate(db, 1))
				require.Empty(t, d.fault, "fault was not reached")
				require.NoError(t, d.crash(1, 512, false))
				db, e = openWithFS(path, opts, d)
				require.NoError(t, e)
				defer db.Close()
				checkBank(t, db, 0, 0)
			})
		}
	}
}
func TestDBSuiteChurn(t *testing.T) {
	suiteEnabled(t)
	root := suiteRoot(t)
	path := filepath.Join(root, "churn.db")
	opts := suiteOptions()
	db, e := Open(path, opts)
	require.NoError(t, e)
	defer func() { _ = db.Close() }()
	require.NoError(t, bankUpdate(db, 0))
	duration := 2 * time.Second
	if profile := os.Getenv("PIRIN_DBTEST_PROFILE"); profile == "extended" || profile == "soak" {
		duration, e = time.ParseDuration(os.Getenv("PIRIN_DBTEST_DURATION"))
		require.NoError(t, e)
	}
	workers, _ := strconv.Atoi(os.Getenv("PIRIN_DBTEST_WORKERS"))
	if workers < 1 {
		workers = 4
	}
	var gate sync.RWMutex
	var commits atomic.Int64
	var latencyTotal, latencyMax, reopenNanos atomic.Int64
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	deadline := time.Now().Add(duration)
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(suiteSeed() + int64(worker)))
			for time.Now().Before(deadline) {
				n := r.Intn(101)
				gate.RLock()
				started := time.Now()
				e := bankUpdate(db, n)
				elapsed := time.Since(started).Nanoseconds()
				latencyTotal.Add(elapsed)
				for old := latencyMax.Load(); elapsed > old; old = latencyMax.Load() {
					if latencyMax.CompareAndSwap(old, elapsed) {
						break
					}
				}
				if e == nil {
					commits.Add(1)
				}
				gate.RUnlock()
				if e != nil {
					errs <- e
					return
				}
				if worker == 0 && commits.Load()%50 == 0 {
					gate.Lock()
					e = db.View(func(tx *Tx) error {
						b, e := tx.GetBucket([]byte("bank"))
						if e != nil {
							return e
						}
						a, _, e := b.GetE([]byte("left"))
						if e != nil {
							return e
						}
						z, _, e := b.GetE([]byte("right"))
						if e != nil {
							return e
						}
						l, e := strconv.Atoi(string(a))
						if e != nil {
							return e
						}
						rr, e := strconv.Atoi(string(z))
						if e != nil {
							return e
						}
						if l+rr != 100 {
							return fmt.Errorf("bank invariant violated")
						}
						return nil
					})
					if e == nil {
						_, e = db.Check()
					}
					if e == nil {
						e = db.Close()
					}
					if e == nil {
						started := time.Now()
						db, e = Open(path, opts)
						reopenNanos.Add(time.Since(started).Nanoseconds())
					}
					gate.Unlock()
					if e != nil {
						errs <- e
						return
					}
				}
			}
		}(worker)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		require.NoError(t, e)
	}
	require.Positive(t, commits.Load())
	t.Logf("commit_mean_ns=%d commit_max_ns=%d reopen_total_ns=%d", latencyTotal.Load()/commits.Load(), latencyMax.Load(), reopenNanos.Load())
	checkBank(t, db, 0, 100)
	info, e := os.Stat(path)
	require.NoError(t, e)
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	t.Logf("policy=%v commits=%d duration=%s heap_bytes=%d database_bytes=%d", opts.SyncPolicy, commits.Load(), duration, memory.HeapAlloc, info.Size())
}

func TestDBSuitePublication(t *testing.T) {
	suiteEnabled(t)
	run := func(t *testing.T, cut int, persist bool) int {
		root := suiteRoot(t)
		d := newModelDisk(root)
		db, e := openWithFS(filepath.Join(root, "db"), nil, d)
		require.NoError(t, e)
		require.NoError(t, bankUpdate(db, 1))
		snapshot := filepath.Join(root, "backup.snapshot")
		_, e = ExportDBToPath(db, snapshot, true)
		require.NoError(t, e)
		require.NoError(t, bankUpdate(db, 2))
		d.trace = nil
		d.cut = cut
		_, exportErr := ExportDBToPath(db, snapshot, true)
		count := len(d.trace)
		require.NoError(t, d.crash(suiteSeed(), 512, persist))
		db, e = openWithFS(filepath.Join(root, "db"), nil, d)
		require.NoError(t, e)
		defer db.Close()
		_, e = ImportDBFromPath(db, snapshot)
		require.NoError(t, e)
		if exportErr == nil {
			checkBank(t, db, 2, 2)
		} else {
			checkBank(t, db, 1, 2)
		}
		return count
	}
	total := run(t, 0, false)
	for cut := 1; cut <= total; cut++ {
		for _, persist := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/%v", cut, persist), func(t *testing.T) { run(t, cut, persist) })
		}
	}
}
func TestDBSuiteDirectoryDurability(t *testing.T) {
	suiteEnabled(t)
	root := suiteRoot(t)
	d := newModelDisk(root)
	child := filepath.Join(root, "unpublished-dir")
	require.NoError(t, d.Mkdir(child, 0700))
	file, e := d.OpenFile(filepath.Join(child, "READY"), os.O_CREATE|os.O_RDWR, 0600)
	require.NoError(t, e)
	_, e = file.Write([]byte("ready"))
	require.NoError(t, e)
	require.NoError(t, file.Sync())
	require.NoError(t, syncDirectories(d, file.Name()))
	require.NoError(t, d.crash(1, 512, false))
	_, e = os.Stat(child)
	require.ErrorIs(t, e, os.ErrNotExist)
}

func TestDBSuiteRestorePublication(t *testing.T) {
	suiteEnabled(t)
	run := func(t *testing.T, cut int, persist bool) int {
		root := suiteRoot(t)
		d := newModelDisk(root)
		db, e := openWithFS(filepath.Join(root, "source.db"), nil, d)
		require.NoError(t, e)
		require.NoError(t, bankUpdate(db, 1))
		snapshot := filepath.Join(root, "backup.snapshot")
		_, e = ExportDBToPath(db, snapshot, false)
		require.NoError(t, e)
		require.NoError(t, db.Close())
		d.trace = nil
		d.cut = cut
		target := filepath.Join(root, "restore")
		_, restoreErr := restoreWithFS(snapshot, target, nil, d)
		count := len(d.trace)
		require.NoError(t, d.crash(suiteSeed(), 512, persist))
		_, readyErr := os.Stat(filepath.Join(target, "READY"))
		if restoreErr == nil {
			require.NoError(t, readyErr)
		}
		if readyErr == nil {
			restored, e := Open(filepath.Join(target, "database.db"), nil)
			require.NoError(t, e)
			defer restored.Close()
			checkBank(t, restored, 1, 1)
		}
		return count
	}
	total := run(t, 0, false)
	for cut := 1; cut <= total; cut++ {
		for _, persist := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/%v", cut, persist), func(t *testing.T) { run(t, cut, persist) })
		}
	}
}

func FuzzDBNode(f *testing.F) {
	f.Add(make([]byte, BTreePageSize))
	f.Add([]byte{NodePage, 1, 0, 0})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > MaxPageSize {
			return
		}
		_ = NewBNode().Deserialize(data)
	})
}

func TestDBSuiteRecoveryInterrupted(t *testing.T) {
	suiteEnabled(t)
	run := func(t *testing.T, policy SyncPolicy, cut int) int {
		root := suiteRoot(t)
		disk := newModelDisk(root)
		path := filepath.Join(root, "recovery.db")
		opts := DefaultOptions().WithSyncPolicy(policy).WithCheckpointTxThreshold(999)
		db, err := openWithFS(path, opts, disk)
		require.NoError(t, err)
		require.NoError(t, bankUpdate(db, 0))
		require.NoError(t, bankUpdate(db, 3))
		require.NoError(t, disk.crash(suiteSeed(), 512, false))
		disk.trace = nil
		disk.cut = cut
		_, _ = openWithFS(path, opts, disk)
		count := len(disk.trace)
		if cut > 0 {
			require.True(t, disk.off, "recovery cut not reached")
		}
		require.NoError(t, disk.crash(suiteSeed(), 512, false))
		recovered, err := openWithFS(path, opts, disk)
		require.NoError(t, err)
		defer recovered.Close()
		checkBank(t, recovered, 3, 3)
		return count
	}
	for _, policy := range []SyncPolicy{SyncPolicyStrict, SyncPolicyJournal, SyncPolicyGroup} {
		total := run(t, policy, 0)
		for cut := 1; cut <= total; cut++ {
			t.Run(fmt.Sprintf("%d/%d", policy, cut), func(t *testing.T) { run(t, policy, cut) })
		}
	}
}

func TestDBSuiteBackupConcurrent(t *testing.T) {
	suiteEnabled(t)
	root := suiteRoot(t)
	db, err := Open(filepath.Join(root, "source.db"), nil)
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, bankUpdate(db, 0))
	stop := make(chan struct{})
	writerDone := make(chan error, 1)
	go func() {
		for index := 1; ; index++ {
			select {
			case <-stop:
				writerDone <- nil
				return
			default:
			}
			if err := bankUpdate(db, index%101); err != nil {
				writerDone <- err
				return
			}
		}
	}()
	defer func() { close(stop); require.NoError(t, <-writerDone) }()
	for index := 0; index < 3; index++ {
		snapshot := filepath.Join(root, fmt.Sprintf("backup-%d.snapshot", index))
		_, err = ExportDBToPath(db, snapshot, false)
		require.NoError(t, err)
		directory := filepath.Join(root, fmt.Sprintf("restore-%d", index))
		_, err = RestoreDBToDirectory(snapshot, directory, nil)
		require.NoError(t, err)
		restored, err := Open(filepath.Join(directory, "database.db"), nil)
		require.NoError(t, err)
		checkBank(t, restored, 0, 100)
		require.NoError(t, restored.Close())
	}
}
