package storage

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type DB struct {
	lock        sync.RWMutex
	readBarrier sync.Mutex
	groupGate   sync.RWMutex
	dal         *Dal
	TxN         atomic.Int32
	groupMu     sync.Mutex
	groupCond   *sync.Cond
	activeBatch *groupCommitBatch
	nextBatchID uint64
	fatalErr    error
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

func (db *DB) Close() error {
	return db.dal.Close()
}

func (db *DB) Begin(write bool) *Tx {
	if err := db.currentFatalErr(); err != nil {
		panic(err)
	}
	if write {
		if db.dal.opts.SyncPolicy == SyncPolicyGroup {
			db.groupGate.RLock()
		}
		db.lock.Lock()
	} else {
		db.readBarrier.Lock()
		db.lock.RLock()
		db.readBarrier.Unlock()
		db.TxN.Add(1)
	}
	tx := newTx(db, write)
	if write && db.dal.opts.SyncPolicy == SyncPolicyGroup {
		tx.holdsGroupGate = true
	}
	return tx
}

func (db *DB) View(fn func(tx *Tx) error) error {
	if err := db.currentFatalErr(); err != nil {
		return err
	}
	tx := db.Begin(false)
	defer tx.Rollback()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func (db *DB) Update(fn func(tx *Tx) error) error {
	if err := db.currentFatalErr(); err != nil {
		return err
	}
	tx := db.Begin(true)
	defer tx.Rollback()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func (db *DB) Stat() *DBStat {
	freePages := db.dal.freelist.availablePageN()
	freelistPages := len(db.dal.freelist.freelistPages)
	releasedPages := len(db.dal.freelist.releasedPages)
	totalPages := int(db.dal.freelist.maxPages)
	usedPages := totalPages - freePages
	if usedPages < 0 {
		usedPages = 0
	}

	bucketStats := make(map[string]*BucketStat)

	_ = db.View(func(tx *Tx) error {
		buckets := tx.Buckets()
		for _, bucketName := range buckets {
			bucket, err := tx.GetBucket(bucketName)
			if err != nil {
				continue
			}
			bucketStats[string(bucketName)] = &BucketStat{
				ItemsN:     bucket.itemsN,
				BlobsN:     bucket.blobsN,
				BytesInUse: bucket.bytesInUse,
			}
		}
		return nil
	})

	stat := &DBStat{
		TotalPageNum:  totalPages,
		FreePageN:     freePages,
		UsedPageN:     usedPages,
		ReleasedPageN: releasedPages,
		FreeListPageN: freelistPages,
		TotalDBSize:   uint64(totalPages) * db.dal.meta.pageSize,
		AvailDBSize:   uint64(freePages) * db.dal.meta.pageSize,
		UsedDBSize:    uint64(usedPages) * db.dal.meta.pageSize,
		Buckets:       bucketStats,
		TxN:           int(db.TxN.Load()),
	}
	return stat
}

func (db *DB) GetOptions() *Options {
	return db.dal.opts
}

func (db *DB) currentFatalErr() error {
	db.groupMu.Lock()
	defer db.groupMu.Unlock()
	return db.fatalErr
}

func (db *DB) setFatalErr(err error) {
	db.groupMu.Lock()
	defer db.groupMu.Unlock()
	if db.fatalErr == nil {
		db.fatalErr = err
	}
}

func (db *DB) registerGroupBatch() *groupCommitBatch {
	db.groupMu.Lock()
	defer db.groupMu.Unlock()

	if db.activeBatch == nil {
		db.nextBatchID++
		db.readBarrier.Lock()
		db.activeBatch = &groupCommitBatch{
			id:      db.nextBatchID,
			waiters: 1,
		}
		go db.flushGroupBatchAfter(db.activeBatch.id, db.dal.opts.GroupCommitWindow)
		return db.activeBatch
	}

	db.activeBatch.waiters++
	if !db.activeBatch.flushRequested && db.activeBatch.waiters >= db.dal.opts.GroupCommitTxThreshold {
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
	if db.fatalErr != nil {
		return db.fatalErr
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
		if err != nil && db.fatalErr == nil {
			db.fatalErr = fmt.Errorf("group commit flush failed: %w", err)
		}
		db.activeBatch = nil
	}
	db.groupCond.Broadcast()
	db.groupMu.Unlock()
	db.readBarrier.Unlock()
}
