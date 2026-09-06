package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

const (
	readNodeCacheLimit = 16384
	readPageCacheLimit = 16384
	readBlobCacheLimit = 512
)

type Tx struct {
	dirtyNodes        map[uint64]*BNode
	dirtyPages        map[uint64]*Page
	dirtyBuckets      map[string]*Bucket
	readNodes         map[uint64]*BNode
	readPages         map[uint64]*Page
	readBlobs         map[uint64]*Blob
	readBlobBytes     int64
	pagesToDelete     []uint64
	pagesToDeleteSet  map[uint64]struct{}
	allocatedPageNums []uint64
	originalMetaRoot  uint64
	write             bool
	holdsGroupGate    bool
	closed            bool
	once              sync.Once
	finishOnce        sync.Once
	asyncPublishOnce  sync.Once
	asyncPublished    chan struct{}
	tracksLifecycle   bool
	beginErr          error
	errMu             sync.Mutex
	stickyErr         error
	bypassNodeCache   bool
	btreePathIndexes  []int
	btreePathNodes    []*BNode
	db                *DB
	buckets           map[string]*Bucket
}

// CommitFuture resolves when an asynchronously submitted transaction is
// durable (or has failed). The transaction's changes are published in commit
// order before CommitAsync returns.
type CommitFuture struct {
	done chan struct{}
	err  error
}

func (future *CommitFuture) Wait() error {
	if future == nil || future.done == nil {
		return nil
	}
	<-future.done
	return future.err
}

func (tx *Tx) signalAsyncPublished() {
	if tx == nil || tx.asyncPublished == nil {
		return
	}
	tx.asyncPublishOnce.Do(func() { close(tx.asyncPublished) })
}

func (tx *Tx) CommitAsync() *CommitFuture {
	future := &CommitFuture{done: make(chan struct{})}
	if tx == nil {
		future.err = ErrTxClosed
		close(future.done)
		return future
	}
	tx.asyncPublished = make(chan struct{})
	go func() {
		// Commit has a few valid early-return paths (for example a read-only or
		// already-closed transaction) that do not pass through the writable
		// commit cleanup. Always publish those outcomes so CommitAsync can never
		// strand its caller waiting for a signal.
		defer tx.signalAsyncPublished()
		future.err = tx.Commit()
		close(future.done)
	}()
	<-tx.asyncPublished
	return future
}

func newTx(db *DB, write bool) *Tx {
	return &Tx{
		pagesToDelete:     make([]uint64, 0),
		allocatedPageNums: make([]uint64, 0),
		originalMetaRoot:  db.dal.meta.root,
		write:             write,
		holdsGroupGate:    false,
		closed:            false,
		once:              sync.Once{},
		finishOnce:        sync.Once{},
		db:                db,
	}
}

func (tx *Tx) finishLifecycle() {
	if tx == nil || tx.db == nil || !tx.tracksLifecycle {
		return
	}
	tx.finishOnce.Do(func() {
		tx.db.activeTx.Done()
	})
}

func (tx *Tx) recordError(err error) {
	if tx == nil || err == nil {
		return
	}
	tx.errMu.Lock()
	if tx.stickyErr == nil {
		tx.stickyErr = err
	}
	tx.errMu.Unlock()
}

func (tx *Tx) currentError() error {
	if tx == nil {
		return ErrTxClosed
	}
	tx.errMu.Lock()
	defer tx.errMu.Unlock()
	return tx.stickyErr
}

func (tx *Tx) allocatePage() (*Page, error) {
	pageNums, err := tx.allocatePageNumbers(1)
	if err != nil {
		return nil, err
	}
	return &Page{
		PageNumber: pageNums[0],
		Data:       make([]byte, tx.db.dal.meta.pageSize),
	}, nil
}

func (tx *Tx) allocatePageNumbers(count int) ([]uint64, error) {
	if err := tx.ensureWritable(); err != nil {
		return nil, err
	}
	if err := tx.ensureAdditionalDirtyPages(count); err != nil {
		return nil, err
	}
	pageNums, err := tx.db.dal.AllocateConsecutivePageNumbers(count)
	if err != nil {
		return nil, err
	}
	tx.allocatedPageNums = append(tx.allocatedPageNums, pageNums...)
	return pageNums, nil
}

func (tx *Tx) newNode(items []*Item, childNodes []uint64) (*BNode, error) {
	node := NewBNode()
	node.items = make([]*Item, len(items))
	copy(node.items, items)
	node.childNodes = append([]uint64{}, childNodes...)
	pageNumbers, err := tx.allocatePageNumbers(1)
	if err != nil {
		return nil, err
	}
	// B-tree nodes are serialized only during commit. Allocating a full page
	// buffer here would immediately discard it and cost one page-sized
	// allocation for every split.
	node.PageNum = pageNumbers[0]
	return node, nil
}

func (tx *Tx) getNode(page uint64) (*BNode, error) {
	if node, ok := tx.dirtyNodes[page]; ok {
		return node, nil
	}
	if node, ok := tx.readNodes[page]; ok {
		return node, nil
	}

	var node *BNode
	var err error
	if tx.bypassNodeCache {
		node, err = tx.db.dal.getNodeUncached(page)
	} else {
		node, err = tx.db.dal.getNode(page)
	}
	if err == nil {
		if tx.write {
			node = cloneNodeForBalance(node)
		}
		if _, exists := tx.readNodes[page]; exists || len(tx.readNodes) < readNodeCacheLimit {
			if tx.readNodes == nil {
				tx.readNodes = make(map[uint64]*BNode)
			}
			tx.readNodes[page] = node
		}
	} else if tx.write {
		tx.recordError(err)
	}
	return node, err
}

func (tx *Tx) setNode(node *BNode) {
	if tx.dirtyNodes == nil {
		tx.dirtyNodes = make(map[uint64]*BNode)
	}
	tx.dirtyNodes[node.PageNum] = node
}

func (tx *Tx) setPage(page *Page) error {
	if page == nil {
		return fmt.Errorf("cannot dirty a nil page")
	}
	if tx.dirtyPages == nil {
		tx.dirtyPages = make(map[uint64]*Page)
	}
	tx.dirtyPages[page.PageNumber] = page
	return tx.ensureDirtyLimit()
}

func (tx *Tx) ensureDirtyLimit() error {
	if tx == nil || tx.db == nil || tx.db.dal == nil {
		return ErrTxClosed
	}
	estimate := tx.dirtyMemoryEstimate()
	if estimate > tx.db.dal.opts.MaxTransactionBytes {
		return fmt.Errorf("%w: transaction state requires at least %d bytes (limit %d)", ErrTransactionTooLarge,
			estimate, tx.db.dal.opts.MaxTransactionBytes)
	}
	return nil
}

func (tx *Tx) dirtyMemoryEstimate() int64 {
	if tx == nil || tx.db == nil || tx.db.dal == nil || tx.db.dal.meta == nil {
		return 0
	}
	pageCount := int64(len(tx.dirtyNodes) + len(tx.dirtyPages))
	indexCount := int64(len(tx.pagesToDelete))
	return pageCount*int64(tx.db.dal.meta.pageSize) + indexCount*int64(UInt64Size)
}

func (tx *Tx) ensureAdditionalDirtyPages(count int) error {
	if count < 0 {
		return fmt.Errorf("%w: invalid dirty page count %d", ErrTransactionTooLarge, count)
	}
	if tx == nil || tx.db == nil || tx.db.dal == nil || tx.db.dal.meta == nil || tx.db.dal.meta.pageSize == 0 {
		return ErrTxClosed
	}
	pageBytes := int64(tx.db.dal.meta.pageSize)
	if int64(count) > (tx.db.dal.opts.MaxTransactionBytes-tx.dirtyMemoryEstimate())/pageBytes {
		return fmt.Errorf("%w: %d additional dirty pages exceed the remaining transaction memory", ErrTransactionTooLarge, count)
	}
	return nil
}

func (tx *Tx) getPage(pageNum uint64) (*Page, error) {
	if page, ok := tx.dirtyPages[pageNum]; ok {
		return page, nil
	}
	if page, ok := tx.readPages[pageNum]; ok {
		return page, nil
	}
	page, err := tx.db.dal.GetPage(pageNum)
	if err == nil {
		if _, exists := tx.readPages[pageNum]; exists || len(tx.readPages) < readPageCacheLimit {
			if tx.readPages == nil {
				tx.readPages = make(map[uint64]*Page)
			}
			tx.readPages[pageNum] = page
		}
	} else if tx.write {
		tx.recordError(err)
	}
	return page, err
}

func (tx *Tx) writeNodes(nodes ...*BNode) {
	for _, n := range nodes {
		tx.setNode(n)
	}
}

func (tx *Tx) deletePage(pageNum uint64) {
	if _, exists := tx.pagesToDeleteSet[pageNum]; exists {
		return
	}
	delete(tx.dirtyNodes, pageNum)
	delete(tx.dirtyPages, pageNum)
	delete(tx.readNodes, pageNum)
	delete(tx.readPages, pageNum)
	if tx.pagesToDeleteSet == nil {
		tx.pagesToDeleteSet = make(map[uint64]struct{})
	}
	tx.pagesToDeleteSet[pageNum] = struct{}{}
	tx.pagesToDelete = append(tx.pagesToDelete, pageNum)
}

func (tx *Tx) releaseAllocatedPages() {
	if err := tx.db.dal.ReleasePages(tx.allocatedPageNums); err != nil {
		logger.Error("failed to release allocated pages", "page_count", len(tx.allocatedPageNums), "error", err)
	}
}

func cloneFreelist(src *Freelist) *Freelist {
	if src == nil {
		return nil
	}
	// Commit rollback needs a snapshot, but copying every released extent on
	// every successful transaction makes commit cost O(freelist fragmentation).
	// Commit-time extent replacement is already copy-on-write; the rare
	// freelist-chain growth path explicitly detaches its mutable collections.
	clone := *src
	return &clone
}

func (tx *Tx) Rollback() {
	if tx == nil {
		return
	}
	if tx.closed {
		return
	}
	if !tx.write {
		tx.once.Do(func() {
			tx.db.lock.RUnlock()
			tx.db.TxN.Add(-1)
		})
		tx.readNodes = nil
		tx.readPages = nil
		tx.readBlobs = nil
		tx.readBlobBytes = 0
		tx.closed = true
		tx.finishLifecycle()
		return
	}
	defer func() {
		tx.allocatedPageNums = nil
		tx.once.Do(func() {
			tx.db.lock.Unlock()
			if tx.holdsGroupGate {
				tx.db.groupGate.RUnlock()
				tx.holdsGroupGate = false
			}
		})
		tx.closed = true
		tx.finishLifecycle()
		tx.signalAsyncPublished()
	}()

	tx.db.dal.meta.root = tx.originalMetaRoot
	tx.dirtyNodes = nil
	tx.dirtyPages = nil
	tx.dirtyBuckets = nil
	tx.readNodes = nil
	tx.readPages = nil
	tx.readBlobs = nil
	tx.readBlobBytes = 0
	tx.pagesToDelete = nil
	tx.pagesToDeleteSet = nil
	tx.releaseAllocatedPages()
}

func (tx *Tx) Commit() (err error) {
	if err = tx.ensureOpen(); err != nil {
		return err
	}
	if err = tx.currentError(); err != nil {
		tx.Rollback()
		return err
	}
	if tx.write {
		if err = tx.ensureDirtyLimit(); err != nil {
			tx.Rollback()
			return err
		}
	}
	if tx.write && len(tx.dirtyNodes) == 0 && len(tx.dirtyPages) == 0 && len(tx.dirtyBuckets) == 0 &&
		len(tx.pagesToDelete) == 0 && len(tx.allocatedPageNums) == 0 && tx.db.dal.meta.root == tx.originalMetaRoot && !tx.db.dal.freelist.dirty {
		tx.Rollback()
		return nil
	}
	if !tx.write {
		tx.once.Do(func() {
			tx.db.lock.RUnlock()
			tx.db.TxN.Add(-1)
		})
		tx.closed = true
		tx.finishLifecycle()
		return nil
	}

	var freelistSnapshot *Freelist
	checkpointDurable := false
	observer := tx.db.dal.opts.CommitObserver
	commitStarted := time.Time{}
	commitStats := CommitPhaseStats{}
	if observer != nil {
		commitStarted = time.Now()
		commitStats = CommitPhaseStats{
			DirtyNodeCount:   len(tx.dirtyNodes),
			DirtyPageCount:   len(tx.dirtyPages),
			DeletedPageCount: len(tx.pagesToDelete),
			UsedFastClear:    true,
		}
	}
	defer func() {
		notifyObserver := err == nil && observer != nil
		if notifyObserver {
			commitStats.TotalDuration = time.Since(commitStarted)
		}
		if err != nil {
			if checkpointDurable {
				tx.db.setFatalErr(fmt.Errorf("durable commit requires recovery: %w", err))
			} else {
				tx.db.dal.meta.root = tx.originalMetaRoot
				if freelistSnapshot != nil {
					tx.db.dal.freelist = freelistSnapshot
				}
				tx.releaseAllocatedPages()
			}
		}
		tx.once.Do(func() {
			tx.db.lock.Unlock()
			if tx.holdsGroupGate {
				tx.db.groupGate.RUnlock()
				tx.holdsGroupGate = false
			}
		})
		tx.dirtyNodes = nil
		tx.dirtyPages = nil
		tx.dirtyBuckets = nil
		tx.readNodes = nil
		tx.readPages = nil
		tx.readBlobs = nil
		tx.readBlobBytes = 0
		tx.pagesToDelete = nil
		tx.pagesToDeleteSet = nil
		tx.allocatedPageNums = nil
		tx.closed = true
		tx.finishLifecycle()
		tx.signalAsyncPublished()
		if notifyObserver {
			func() {
				defer func() {
					if recovered := recover(); recovered != nil {
						logger.Error("commit observer panicked", "panic", recovered)
					}
				}()
				observer(commitStats)
			}()
		}
	}()

	root := tx.getRootBucket()
	for _, bucket := range tx.dirtyBuckets {
		data := bucket.serialize()
		if err = root.Put(bucket.name, data.Value); err != nil {
			return err
		}
	}
	// Snapshot after catalog updates: those may allocate B-tree pages, and all
	// such allocations are already tracked for rollback. From here onward the
	// freelist mutation paths use copy-on-write collections.
	freelistSnapshot = cloneFreelist(tx.db.dal.freelist)

	if tx.db.dal.opts.SyncPolicy == SyncPolicyJournal {
		checkpointDurable, err = tx.commitJournalPolicy(&commitStats, observer != nil)
		return err
	}
	if tx.db.dal.opts.SyncPolicy == SyncPolicyGroup {
		checkpointDurable, err = tx.commitGroupPolicy(&commitStats, observer != nil)
		return err
	}

	checkpointDurable, err = tx.commitStrictPolicy(&commitStats, observer != nil)
	return err
}

func (tx *Tx) commitStrictPolicy(commitStats *CommitPhaseStats, collectStats bool) (bool, error) {
	commitPages, err := tx.materializeCommitPages()
	if err != nil {
		return false, err
	}

	// First write to physical log
	txLogStats, txLogErr := tx.db.dal.txLog.withStats(func() error {
		for _, page := range commitPages {
			if setPageErr := tx.db.dal.setPage(page, false); setPageErr != nil {
				return setPageErr
			}
		}
		return nil
	})
	if collectStats {
		commitStats.TxLogWriteDuration = txLogStats.writeDuration
		commitStats.TxLogSyncDuration = txLogStats.syncDuration
		commitStats.JournalPageCount = txLogStats.bufferedPages
		commitStats.JournalBytes = txLogStats.bufferedBytes
	}
	if txLogErr != nil {
		return errors.Is(txLogErr, ErrJournalRestoreFailed), txLogErr
	}
	checkpointDurable := true

	// Second write to the Database storage
	dbWriteStarted := time.Now()
	for _, page := range commitPages {
		if err := tx.db.dal.SetPage(page); err != nil {
			return checkpointDurable, err
		}
	}
	markFreelistPersisted(tx.db.dal.freelist)
	if collectStats {
		commitStats.DBWriteDuration = time.Since(dbWriteStarted)
	}

	dbSyncStarted := time.Now()
	if err := tx.db.dal.Sync(); err != nil {
		return checkpointDurable, err
	}
	if collectStats {
		commitStats.DBSyncDuration = time.Since(dbSyncStarted)
	}

	clearStarted := time.Now()
	if err := tx.db.dal.txLog.ClearFast(); err != nil {
		return checkpointDurable, err
	}
	if collectStats {
		commitStats.JournalClearDuration = time.Since(clearStarted)
	}

	return checkpointDurable, nil
}

func (tx *Tx) commitJournalPolicy(commitStats *CommitPhaseStats, collectStats bool) (bool, error) {
	commitPages, err := tx.materializeCommitPages()
	if err != nil {
		return false, err
	}
	if len(tx.db.dal.pendingJournal) > 0 && tx.db.dal.projectedPendingJournalPages(commitPages) > tx.db.dal.maxJournalPages() {
		syncStarted := time.Now()
		if err = tx.db.dal.Sync(); err != nil {
			return false, fmt.Errorf("checkpoint pending journal before memory limit: %w", err)
		}
		if collectStats {
			commitStats.DBSyncDuration += time.Since(syncStarted)
		}
		if err = tx.db.dal.txLog.ClearFast(); err != nil {
			return false, fmt.Errorf("clear checkpointed journal before memory limit: %w", err)
		}
		tx.db.dal.resetPendingJournal()
	}

	candidatePending, candidateTxN, pendingPages := tx.db.dal.stagePendingJournalPages(commitPages)

	txLogStats, err := tx.db.dal.txLog.ReplaceWithPages(pendingPages, true)
	if collectStats {
		commitStats.TxLogWriteDuration = txLogStats.writeDuration
		commitStats.TxLogSyncDuration = txLogStats.syncDuration
		commitStats.JournalPageCount = txLogStats.bufferedPages
		commitStats.JournalBytes = txLogStats.bufferedBytes
	}
	if err != nil {
		return errors.Is(err, ErrJournalRestoreFailed), err
	}
	tx.db.dal.installPendingJournal(candidatePending, candidateTxN)
	checkpointDurable := true

	dbWriteStarted := time.Now()
	for _, page := range commitPages {
		if err = tx.db.dal.SetPage(page); err != nil {
			return checkpointDurable, err
		}
	}
	markFreelistPersisted(tx.db.dal.freelist)
	if collectStats {
		commitStats.DBWriteDuration = time.Since(dbWriteStarted)
	}

	if !tx.db.dal.checkpointDue() {
		return checkpointDurable, nil
	}

	dbSyncStarted := time.Now()
	if err = tx.db.dal.Sync(); err != nil {
		return checkpointDurable, err
	}
	if collectStats {
		commitStats.DBSyncDuration = time.Since(dbSyncStarted)
	}

	clearStarted := time.Now()
	tx.db.dal.resetPendingJournal()
	if err = tx.db.dal.txLog.ClearFast(); err != nil {
		return checkpointDurable, err
	}
	if collectStats {
		commitStats.JournalClearDuration = time.Since(clearStarted)
	}

	return checkpointDurable, nil
}

func (tx *Tx) commitGroupPolicy(commitStats *CommitPhaseStats, collectStats bool) (bool, error) {
	commitPages, err := tx.materializeCommitPages()
	if err != nil {
		return false, err
	}

	tx.db.dal.mergePendingJournalPages(commitPages)
	tx.db.dal.mergeOverlayPages(commitPages)
	markFreelistPersisted(tx.db.dal.freelist)

	if collectStats {
		commitStats.JournalPageCount = len(tx.db.dal.pendingJournal)
	}

	forceFlush := len(tx.db.dal.pendingJournal) >= tx.db.dal.maxJournalPages()
	batch := tx.db.registerGroupBatch(forceFlush)

	// Release the writer lock early so following writers can join the same flush batch.
	tx.once.Do(func() {
		tx.db.lock.Unlock()
		if tx.holdsGroupGate {
			tx.db.groupGate.RUnlock()
			tx.holdsGroupGate = false
		}
	})
	tx.signalAsyncPublished()

	if err = tx.db.waitForGroupBatch(batch); err != nil {
		// This transaction is already part of shared batch state. It cannot
		// safely restore its freelist snapshot independently of other waiters.
		// The flusher marks the database fatal, and recovery retains the last
		// durable journal if one exists.
		return true, err
	}
	return true, nil
}

func (tx *Tx) materializeCommitPages() ([]*Page, error) {
	if err := tx.db.dal.ReleasePages(tx.pagesToDelete); err != nil {
		return nil, err
	}

	pagesByNum := make(map[uint64]*Page, len(tx.dirtyNodes)+len(tx.dirtyPages)+4)

	for _, node := range tx.dirtyNodes {
		page, err := tx.db.dal.marshalNodePage(node)
		if err != nil {
			return nil, err
		}
		// marshalNodePage returns a transaction-owned buffer, so cloning it
		// again only creates one page of garbage for every dirty node.
		pagesByNum[page.PageNumber] = page
	}

	for _, page := range tx.dirtyPages {
		pageCopy := clonePage(page)
		if err := tx.db.dal.preparePage(pageCopy); err != nil {
			return nil, err
		}
		pagesByNum[page.PageNumber] = pageCopy
	}

	freelistPages, err := BuildFreelistPages(tx.db.dal, tx.db.dal.freelist)
	if err != nil {
		return nil, err
	}
	for _, page := range freelistPages {
		pagesByNum[page.PageNumber] = page
	}
	if tx.db.dal.meta.root != tx.originalMetaRoot {
		metaPage, err := buildMetaPageAt(metaPageNumber, tx.db.dal.meta.pageSize, tx.db.dal.meta)
		if err != nil {
			return nil, err
		}
		if err := tx.db.dal.preparePage(metaPage); err != nil {
			return nil, err
		}
		pagesByNum[metaPageNumber] = metaPage
		if tx.db.dal.pageChecksums {
			mirrorPage, buildErr := buildMetaPageAt(metaMirrorPageNumber, tx.db.dal.meta.pageSize, tx.db.dal.meta)
			if buildErr != nil {
				return nil, buildErr
			}
			if err := tx.db.dal.preparePage(mirrorPage); err != nil {
				return nil, err
			}
			pagesByNum[metaMirrorPageNumber] = mirrorPage
		}
	}

	pageNums := make([]uint64, 0, len(pagesByNum))
	for pageNum := range pagesByNum {
		pageNums = append(pageNums, pageNum)
	}
	sort.Slice(pageNums, func(i, j int) bool {
		return pageNums[i] < pageNums[j]
	})

	pages := make([]*Page, 0, len(pageNums))
	for _, pageNum := range pageNums {
		pages = append(pages, pagesByNum[pageNum])
	}
	if len(pages) > tx.db.dal.maxJournalPages() {
		return nil, fmt.Errorf("%w: commit needs %d pages, limit including bookkeeping is %d", ErrTransactionTooLarge, len(pages), tx.db.dal.maxJournalPages())
	}
	return pages, nil
}

func (tx *Tx) getRootBucket() *Bucket {
	bucket := newBucket([]byte{})
	bucket.root = tx.db.dal.meta.root
	bucket.tx = tx
	return bucket
}

func (tx *Tx) createOrUpdateBucket(bucket *Bucket) (*Bucket, error) {
	bucket.tx = tx
	data := bucket.serialize()

	rootBucket := tx.getRootBucket()
	err := rootBucket.Put(bucket.name, data.Value)
	if err != nil {
		return nil, err
	}
	return bucket, nil
}

func (tx *Tx) bucketExists(name []byte) (bool, error) {
	rootBucket := tx.getRootBucket()
	_, found, err := rootBucket.GetE(name)
	return found, err
}

func (tx *Tx) GetBucket(name []byte) (*Bucket, error) {
	if err := tx.ensureOpen(); err != nil {
		return nil, err
	}
	if err := validateBucketName(name); err != nil {
		return nil, err
	}
	if bucket, ok := tx.buckets[string(name)]; ok {
		return bucket, nil
	}
	rootBucket := tx.getRootBucket()
	value, found, getErr := rootBucket.GetE(name)
	if getErr != nil {
		return nil, getErr
	}
	if !found || value == nil {
		return nil, ErrBucketNotFound
	}
	bucket := newBucket(name)
	bucket.tx = tx
	if err := bucket.deserialize(value); err != nil {
		tx.recordError(err)
		return nil, err
	}
	if tx.buckets == nil {
		tx.buckets = make(map[string]*Bucket)
	}
	tx.buckets[bucket.nameKey] = bucket
	return bucket, nil
}

func (tx *Tx) CreateBucket(name []byte) (_ *Bucket, returnErr error) {
	if err := tx.ensureWritable(); err != nil {
		return nil, err
	}
	if err := validateBucketName(name); err != nil {
		return nil, err
	}
	if existing, ok := tx.buckets[string(name)]; ok && existing != nil {
		return nil, ErrBucketExists
	}
	exists, existsErr := tx.bucketExists(name)
	if existsErr != nil {
		return nil, existsErr
	}
	if exists {
		return nil, ErrBucketExists
	}

	node := NewBNode()
	pageNumbers, allocatePageErr := tx.allocatePageNumbers(1)
	if allocatePageErr != nil {
		return nil, allocatePageErr
	}
	defer func() {
		if returnErr != nil {
			tx.recordError(returnErr)
		}
	}()
	node.PageNum = pageNumbers[0]
	tx.setNode(node)
	bucket := newBucket(name)
	bucket.root = pageNumbers[0]
	if tx.dirtyBuckets == nil {
		tx.dirtyBuckets = make(map[string]*Bucket)
	}
	tx.dirtyBuckets[bucket.nameKey] = bucket
	if tx.buckets == nil {
		tx.buckets = make(map[string]*Bucket)
	}
	tx.buckets[bucket.nameKey] = bucket
	return tx.createOrUpdateBucket(bucket)
}

func (tx *Tx) CreateBucketIfNotExists(name []byte) (*Bucket, error) {
	if err := tx.ensureWritable(); err != nil {
		return nil, err
	}
	bucket, err := tx.CreateBucket(name)
	if errors.Is(err, ErrBucketExists) {
		return tx.GetBucket(name)
	}
	return bucket, err
}

// MoveBucket atomically changes a bucket's catalog name without traversing or
// rewriting the bucket contents. Existing readers retain their transaction
// snapshot; subsequent lookups use the new name.
func (tx *Tx) MoveBucket(oldName, newName []byte) (*Bucket, error) {
	if err := tx.ensureWritable(); err != nil {
		return nil, err
	}
	if err := validateBucketName(oldName); err != nil {
		return nil, err
	}
	if err := validateBucketName(newName); err != nil {
		return nil, err
	}
	if bytes.Equal(oldName, newName) {
		return tx.GetBucket(oldName)
	}
	if exists, err := tx.bucketExists(newName); err != nil {
		return nil, err
	} else if exists {
		return nil, ErrBucketExists
	}
	source, err := tx.GetBucket(oldName)
	if err != nil {
		return nil, err
	}
	moved := newBucket(newName)
	moved.root = source.root
	moved.counter = source.counter
	moved.itemsN = source.itemsN
	moved.blobsN = source.blobsN
	moved.bytesInUse = source.bytesInUse
	moved.tx = tx

	root := tx.getRootBucket()
	if err = root.Put(newName, moved.serialize().Value); err != nil {
		return nil, err
	}
	if err = root.Remove(oldName); err != nil {
		return nil, err
	}
	delete(tx.buckets, string(oldName))
	delete(tx.dirtyBuckets, string(oldName))
	if tx.buckets == nil {
		tx.buckets = make(map[string]*Bucket)
	}
	tx.buckets[moved.nameKey] = moved
	return moved, nil
}

func (tx *Tx) DeleteBucket(name []byte) (returnErr error) {
	if err := tx.ensureWritable(); err != nil {
		return err
	}
	if err := validateBucketName(name); err != nil {
		return err
	}

	rootBucket := tx.getRootBucket()
	rawBucketValue, found, getErr := rootBucket.GetE(name)
	if getErr != nil {
		return getErr
	}
	if !found || rawBucketValue == nil {
		return ErrBucketNotFound
	}

	bucket := newBucket(name)
	bucket.tx = tx
	if err := bucket.deserialize(rawBucketValue); err != nil {
		return err
	}

	if bucket.root != 0 {
		nodePages := make([]uint64, 0, 8)
		blobRoots := make(map[uint64]struct{})
		if err := tx.collectBucketGarbage(bucket.root, &nodePages, blobRoots); err != nil {
			return err
		}
		defer func() {
			if returnErr != nil {
				tx.recordError(returnErr)
			}
		}()

		for blobRoot := range blobRoots {
			if _, err := DeleteBlob(tx, blobRoot); err != nil {
				return err
			}
		}
		for _, pageNum := range nodePages {
			tx.deletePage(pageNum)
		}
	}

	delete(tx.dirtyBuckets, string(name))
	delete(tx.buckets, string(name))

	err := rootBucket.Remove(name)
	if errors.Is(err, ErrNodeNotFound) {
		return ErrBucketNotFound
	}
	return err
}

func (tx *Tx) collectBucketGarbage(pageNum uint64, nodePages *[]uint64, blobRoots map[uint64]struct{}) error {
	return tx.collectBucketGarbagePage(pageNum, nodePages, blobRoots, make(map[uint64]struct{}), 0)
}

func (tx *Tx) collectBucketGarbagePage(pageNum uint64, nodePages *[]uint64, blobRoots map[uint64]struct{}, visited map[uint64]struct{}, depth int) error {
	if depth >= 128 {
		return fmt.Errorf("%w: b-tree depth exceeds 128 while deleting bucket", ErrCorruptedNode)
	}
	if _, exists := visited[pageNum]; exists {
		return fmt.Errorf("%w: cycle or duplicate page %d while deleting bucket", ErrCorruptedNode, pageNum)
	}
	maxReferences := tx.db.dal.opts.MaxTransactionBytes / 32
	if int64(len(*nodePages)+len(blobRoots)) >= maxReferences {
		return fmt.Errorf("%w: bucket deletion traversal exceeds transaction memory limit", ErrTransactionTooLarge)
	}
	visited[pageNum] = struct{}{}
	node, err := tx.getNode(pageNum)
	if err != nil {
		return err
	}
	*nodePages = append(*nodePages, node.PageNum)

	for _, item := range node.items {
		if len(item.Value) == 0 {
			return ErrUnknownItemType
		}
		switch item.Value[0] {
		case ValueSimple:
		case ValueBlob:
			if len(item.Value) != 1+UInt64Size {
				return ErrUnknownItemType
			}
			blobRoot := binary.LittleEndian.Uint64(item.Value[1:])
			blobRoots[blobRoot] = struct{}{}
		default:
			return ErrUnknownItemType
		}
	}

	for _, childPageNum := range node.childNodes {
		if err := tx.collectBucketGarbagePage(childPageNum, nodePages, blobRoots, visited, depth+1); err != nil {
			return err
		}
	}
	return nil
}

func validateBucketName(name []byte) error {
	if len(name) == 0 || len(name) >= MaxKeySize {
		return ErrInvalidBucketName
	}
	return nil
}

func (tx *Tx) BucketsE() ([][]byte, error) {
	if err := tx.ensureOpen(); err != nil {
		return nil, err
	}
	rootBucket := tx.getRootBucket()
	cursor := rootBucket.Cursor()
	buckets := make([][]byte, 0)
	for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
		buckets = append(buckets, k)
	}
	if err := cursor.Err(); err != nil {
		return nil, err
	}
	return buckets, nil
}

func (tx *Tx) Buckets() [][]byte {
	buckets, err := tx.BucketsE()
	if err != nil {
		tx.recordError(err)
	}
	return buckets
}
