package storage

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type DB struct {
	lock        sync.RWMutex
	readBarrier sync.Mutex
	groupGate   sync.RWMutex
	closeMu     sync.Mutex
	lifecycleMu sync.RWMutex
	activeTx    sync.WaitGroup
	closing     bool
	closed      bool
	dal         *Dal
	TxN         atomic.Int32
	groupMu     sync.Mutex
	groupCond   *sync.Cond
	activeBatch *groupCommitBatch
	nextBatchID uint64
	fatalState  atomic.Pointer[dbFatalState]
}

type dbFatalState struct {
	err error
}

type groupCommitBatch struct {
	id             uint64
	waiters        int
	flushRequested bool
	err            error
}

type BucketStat struct {
	ItemsN     uint64
	BlobsN     uint64
	BytesInUse uint64
}

type DBStat struct {
	TotalPageNum  int                    // total number pages
	FreePageN     int                    // total number of free pages
	UsedPageN     int                    // total number of used pages
	ReleasedPageN int                    // total number of released pages
	FreeListPageN int                    // total number of pages allocated for freelist
	TotalDBSize   uint64                 // amount of pages * page size
	AvailDBSize   uint64                 // amount of free pages * page size
	UsedDBSize    uint64                 // amount of used pages * page size
	Buckets       map[string]*BucketStat //
	TxN           int                    // total number of started read transactions
}

func Open(path string, opts *Options) (*DB, error) {
	if opts == nil {
		opts = DefaultOptions()
	}
	dal, err := NewDal(path, opts)
	if err != nil {
		return nil, err
	}
	db := &DB{
		lock:        sync.RWMutex{},
		readBarrier: sync.Mutex{},
		groupGate:   sync.RWMutex{},
		dal:         dal,
	}
	db.groupCond = sync.NewCond(&db.groupMu)
	return db, nil
}

// Health reports lifecycle and fatal durability failures without waiting for a
// transaction lock. It does not perform a disk scan or an integrity check.
func (db *DB) Health() error {
	if db == nil {
		return ErrDatabaseClosed
	}
	db.lifecycleMu.RLock()
	defer db.lifecycleMu.RUnlock()
	if db.closing || db.closed {
		return ErrDatabaseClosed
	}
	if err := db.currentFatalErr(); err != nil {
		return fmt.Errorf("%w: %v", ErrDatabaseFatal, err)
	}
	return nil
}

func (db *DB) Close() error {
	if db == nil {
		return nil
	}
	db.closeMu.Lock()
	defer db.closeMu.Unlock()

	db.lifecycleMu.Lock()
	if db.closed {
		db.lifecycleMu.Unlock()
		return nil
	}
	db.closing = true
	db.lifecycleMu.Unlock()

	db.activeTx.Wait()

	var checkpointErr error
	if db.currentFatalErr() == nil && !db.dal.recoveryPending {
		if err := db.dal.Sync(); err != nil {
			checkpointErr = fmt.Errorf("sync database during close: %w", err)
		} else if err := db.dal.txLog.Clear(); err != nil {
			checkpointErr = fmt.Errorf("clear transaction log during close: %w", err)
		} else {
			db.dal.resetPendingJournal()
			db.dal.resetOverlay()
		}
	}
	closeErr := db.dal.Close()

	db.lifecycleMu.Lock()
	db.closed = true
	db.closing = false
	db.lifecycleMu.Unlock()

	return errors.Join(checkpointErr, closeErr)
}

func (db *DB) Begin(write bool) *Tx {
	tx, err := db.BeginE(write)
	if err != nil {
		return &Tx{db: db, closed: true, beginErr: err}
	}
	return tx
}

func (db *DB) BeginE(write bool) (*Tx, error) {
	if db == nil || db.dal == nil {
		return nil, ErrDatabaseClosed
	}
	db.lifecycleMu.RLock()
	if db.closed || db.closing {
		db.lifecycleMu.RUnlock()
		return nil, ErrDatabaseClosed
	}
	if db.dal.recoveryPending {
		db.lifecycleMu.RUnlock()
		return nil, ErrRecoveryRequired
	}
	if err := db.currentFatalErr(); err != nil {
		db.lifecycleMu.RUnlock()
		return nil, fmt.Errorf("%w: %v", ErrDatabaseFatal, err)
	}
	db.activeTx.Add(1)
	db.lifecycleMu.RUnlock()

	if write {
		if db.dal.opts.SyncPolicy == SyncPolicyGroup {
			db.groupGate.RLock()
		}
		db.lock.Lock()
	} else {
		if db.dal.opts.SyncPolicy == SyncPolicyGroup {
			// Never wait for the database lock while holding readBarrier: a
			// group writer holds the database lock while acquiring that barrier
			// to publish a batch. Try the pair atomically and yield on conflict.
			for {
				db.readBarrier.Lock()
				if db.lock.TryRLock() {
					db.readBarrier.Unlock()
					break
				}
				db.readBarrier.Unlock()
				runtime.Gosched()
			}
		} else {
			db.lock.RLock()
		}
		db.TxN.Add(1)
	}
	if err := db.currentFatalErr(); err != nil {
		if write {
			db.lock.Unlock()
			if db.dal.opts.SyncPolicy == SyncPolicyGroup {
				db.groupGate.RUnlock()
			}
		} else {
			db.lock.RUnlock()
			db.TxN.Add(-1)
		}
		db.activeTx.Done()
		return nil, fmt.Errorf("%w: %v", ErrDatabaseFatal, err)
	}
	tx := newTx(db, write)
	tx.tracksLifecycle = true
	if write && db.dal.opts.SyncPolicy == SyncPolicyGroup {
		tx.holdsGroupGate = true
	}
	return tx, nil
}

func (db *DB) View(fn func(tx *Tx) error) error {
	if db == nil {
		return ErrDatabaseClosed
	}
	if fn == nil {
		return fmt.Errorf("view callback is nil")
	}
	tx, err := db.BeginE(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func (db *DB) Update(fn func(tx *Tx) error) error {
	if db == nil {
		return ErrDatabaseClosed
	}
	if fn == nil {
		return fmt.Errorf("update callback is nil")
	}
	tx, err := db.BeginE(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func (db *DB) Stat() *DBStat {
	stat, _ := db.StatE()
	return stat
}

func (db *DB) StatE() (*DBStat, error) {
	stat := &DBStat{Buckets: make(map[string]*BucketStat)}
	if db == nil {
		return stat, ErrDatabaseClosed
	}
	err := db.View(func(tx *Tx) error {
		freePages := db.dal.freelist.availablePageN()
		totalPages := uint64ToIntSaturated(db.dal.freelist.maxPages)
		usedPages := totalPages - freePages
		if usedPages < 0 {
			usedPages = 0
		}
		stat.TotalPageNum = totalPages
		stat.FreePageN = freePages
		stat.UsedPageN = usedPages
		stat.ReleasedPageN = uint64ToIntSaturated(db.dal.freelist.releasedPageN())
		stat.FreeListPageN = len(db.dal.freelist.freelistPages)
		stat.TotalDBSize = uint64(totalPages) * db.dal.meta.pageSize
		stat.AvailDBSize = uint64(freePages) * db.dal.meta.pageSize
		stat.UsedDBSize = uint64(usedPages) * db.dal.meta.pageSize
		buckets, bucketsErr := tx.BucketsE()
		if bucketsErr != nil {
			return bucketsErr
		}
		for _, bucketName := range buckets {
			bucket, err := tx.GetBucket(bucketName)
			if err != nil {
				return err
			}
			stat.Buckets[string(bucketName)] = &BucketStat{
				ItemsN:     bucket.itemsN,
				BlobsN:     bucket.blobsN,
				BytesInUse: bucket.bytesInUse,
			}
		}
		return nil
	})
	stat.TxN = int(db.TxN.Load())
	return stat, err
}

func (db *DB) GetOptions() *Options {
	if db == nil || db.dal == nil {
		return nil
	}
	return cloneOptions(db.dal.opts)
}

func (db *DB) GroupCommitBatchCount() uint64 {
	if db == nil {
		return 0
	}
	db.groupMu.Lock()
	defer db.groupMu.Unlock()
	return db.nextBatchID
}

func (db *DB) currentFatalErr() error {
	if db == nil {
		return ErrDatabaseClosed
	}
	state := db.fatalState.Load()
	if state == nil {
		return nil
	}
	return state.err
}

func (db *DB) setFatalErr(err error) {
	if db == nil || err == nil {
		return
	}
	db.fatalState.CompareAndSwap(nil, &dbFatalState{err: err})
}

func (db *DB) registerGroupBatch(forceFlush bool) *groupCommitBatch {
	db.groupMu.Lock()
	defer db.groupMu.Unlock()

	if db.activeBatch == nil {
		db.nextBatchID++
		db.readBarrier.Lock()
		db.activeBatch = &groupCommitBatch{
			id:      db.nextBatchID,
			waiters: 1,
		}
		delay := db.dal.opts.GroupCommitWindow
		if forceFlush {
			db.activeBatch.flushRequested = true
			delay = 0
		}
		go db.flushGroupBatchAfter(db.activeBatch.id, delay)
		return db.activeBatch
	}

	db.activeBatch.waiters++
	if !db.activeBatch.flushRequested && (forceFlush || db.activeBatch.waiters >= db.dal.opts.GroupCommitTxThreshold) {
		db.activeBatch.flushRequested = true
		go db.flushGroupBatchAfter(db.activeBatch.id, 0)
	}
	return db.activeBatch
}

func (db *DB) waitForGroupBatch(batch *groupCommitBatch) error {
	db.groupMu.Lock()
	defer db.groupMu.Unlock()

	for db.activeBatch != nil && db.activeBatch.id == batch.id {
		db.groupCond.Wait()
	}

	if batch.err != nil {
		return batch.err
	}
	if fatalErr := db.currentFatalErr(); fatalErr != nil {
		return fatalErr
	}
	return nil
}

func (db *DB) flushGroupBatchAfter(batchID uint64, delay time.Duration) {
	if delay > 0 {
		time.Sleep(delay)
	}

	db.groupGate.Lock()
	defer db.groupGate.Unlock()

	db.groupMu.Lock()
	batch := db.activeBatch
	if batch == nil || batch.id != batchID {
		db.groupMu.Unlock()
		return
	}
	batch.flushRequested = true
	db.groupMu.Unlock()

	pendingPages := db.dal.pendingJournalPages()
	overlayPages := db.dal.overlayPages()

	_, err := db.dal.txLog.ReplaceWithPages(pendingPages, true)
	if err == nil {
		for _, page := range overlayPages {
			if writeErr := db.dal.SetPage(page); writeErr != nil {
				err = writeErr
				break
			}
		}
	}
	if err == nil {
		err = db.dal.Sync()
	}
	if err == nil {
		err = db.dal.txLog.ClearFast()
	}
	if err == nil {
		db.dal.resetPendingJournal()
		db.dal.resetOverlay()
	}

	db.groupMu.Lock()
	if db.activeBatch != nil && db.activeBatch.id == batchID {
		db.activeBatch.err = err
		if err != nil {
			db.setFatalErr(fmt.Errorf("group commit flush failed: %w", err))
		}
		db.activeBatch = nil
	}
	db.groupCond.Broadcast()
	db.groupMu.Unlock()
	db.readBarrier.Unlock()
}
