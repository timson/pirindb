package storage

import (
	"encoding/binary"
	"errors"
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
	pagesToDelete     []uint64
	allocatedPageNums []uint64
	originalMetaRoot  uint64
	write             bool
	holdsGroupGate    bool
	closed            bool
	once              sync.Once
	db                *DB
	buckets           sync.Map
}

func newTx(db *DB, write bool) *Tx {
	return &Tx{
		dirtyNodes:        map[uint64]*BNode{},
		dirtyPages:        map[uint64]*Page{},
		dirtyBuckets:      map[string]*Bucket{},
		readNodes:         map[uint64]*BNode{},
		readPages:         map[uint64]*Page{},
		readBlobs:         map[uint64]*Blob{},
		pagesToDelete:     make([]uint64, 0),
		allocatedPageNums: make([]uint64, 0),
		originalMetaRoot:  db.dal.meta.root,
		write:             write,
		holdsGroupGate:    false,
		closed:            false,
		once:              sync.Once{},
		db:                db,
		buckets:           sync.Map{},
	}
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
	pageNums, err := tx.db.dal.AllocateConsecutivePageNumbers(count)
	if err != nil {
		return nil, err
	}
	tx.allocatedPageNums = append(tx.allocatedPageNums, pageNums...)
	return pageNums, nil
}

func (tx *Tx) newNode(items []*Item, childNodes []uint64) *BNode {
	node := NewBNode()
	node.items = make([]*Item, len(items))
	copy(node.items, items)
	node.childNodes = append([]uint64{}, childNodes...)
	page, _ := tx.allocatePage()
	node.PageNum = page.PageNumber
	return node
}

func (tx *Tx) getNode(page uint64) (*BNode, error) {
	if node, ok := tx.dirtyNodes[page]; ok {
		return node, nil
	}
	if !tx.write {
		if node, ok := tx.readNodes[page]; ok {
			return node, nil
		}
	}

	node, err := tx.db.dal.getNode(page)
	if err == nil && !tx.write {
		if _, exists := tx.readNodes[page]; exists || len(tx.readNodes) < readNodeCacheLimit {
			tx.readNodes[page] = node
		}
	}
	return node, err
}

func (tx *Tx) setNode(node *BNode) {
	tx.dirtyNodes[node.PageNum] = node
}

func (tx *Tx) setPage(page *Page) {
	tx.dirtyPages[page.PageNumber] = page
}

func (tx *Tx) getPage(pageNum uint64) (*Page, error) {
	if page, ok := tx.dirtyPages[pageNum]; ok {
		return page, nil
	}
	if !tx.write {
		if page, ok := tx.readPages[pageNum]; ok {
			return page, nil
		}
	}
	page, err := tx.db.dal.GetPage(pageNum)
	if err == nil && !tx.write {
		if _, exists := tx.readPages[pageNum]; exists || len(tx.readPages) < readPageCacheLimit {
			tx.readPages[pageNum] = page
		}
	}
	return page, err
}

func (tx *Tx) writeNodes(nodes ...*BNode) {
	for _, n := range nodes {
		tx.setNode(n)
	}
}

func (tx *Tx) deletePage(pageNum uint64) {
	tx.pagesToDelete = append(tx.pagesToDelete, pageNum)
}

func (tx *Tx) releaseAllocatedPages() {
	for _, pageNum := range tx.allocatedPageNums {
		if err := tx.db.dal.ReleasePage(pageNum); err != nil {
			logger.Error("failed to release allocated page", "page_num", pageNum, "error", err)
		}
	}
}

func cloneFreelist(src *Freelist) *Freelist {
	if src == nil {
		return nil
	}
	clone := *src
	clone.releasedPages = append([]uint64(nil), src.releasedPages...)
	clone.releasedPagesDirty = src.releasedPagesDirty
	clone.freelistPages = append([]uint64(nil), src.freelistPages...)
	if src.releasedPagesSet != nil {
		clone.releasedPagesSet = make(map[uint64]struct{}, len(src.releasedPagesSet))
		for pageNum := range src.releasedPagesSet {
			clone.releasedPagesSet[pageNum] = struct{}{}
		}
	} else {
		clone.releasedPagesSet = nil
	}
	if src.releasedPageIndex != nil {
		clone.releasedPageIndex = make(map[uint64]int, len(src.releasedPageIndex))
		for pageNum, idx := range src.releasedPageIndex {
			clone.releasedPageIndex[pageNum] = idx
		}
	} else {
		clone.releasedPageIndex = nil
	}
	if src.releasedExtents != nil {
		clone.releasedExtents = append([]pageExtent(nil), src.releasedExtents...)
	} else {
		clone.releasedExtents = nil
	}
	clone.releasedExtentsDirty = src.releasedExtentsDirty
	if src.persistedState != nil {
		clone.persistedState = &freelistPersistedState{
			currentPage:   src.persistedState.currentPage,
			maxPages:      src.persistedState.maxPages,
			releasedPages: append([]uint64(nil), src.persistedState.releasedPages...),
			freelistPages: append([]uint64(nil), src.persistedState.freelistPages...),
		}
	} else {
		clone.persistedState = nil
	}
	return &clone
}

func (tx *Tx) Rollback() {
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
		tx.closed = true
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
	}()

	tx.db.dal.meta.root = tx.originalMetaRoot
	tx.dirtyNodes = nil
	tx.dirtyPages = nil
	tx.dirtyBuckets = nil
	tx.readNodes = nil
	tx.readPages = nil
	tx.readBlobs = nil
	tx.pagesToDelete = nil
	tx.releaseAllocatedPages()
}

func (tx *Tx) Commit() (err error) {
	if !tx.write {
		tx.once.Do(func() {
			tx.db.lock.RUnlock()
			tx.db.TxN.Add(-1)
		})
		tx.closed = true
		return nil
	}

	freelistSnapshot := cloneFreelist(tx.db.dal.freelist)
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
		if err == nil && observer != nil {
			commitStats.TotalDuration = time.Since(commitStarted)
			observer(commitStats)
		}
		if err != nil && !checkpointDurable {
			tx.db.dal.meta.root = tx.originalMetaRoot
			tx.db.dal.freelist = freelistSnapshot
			tx.releaseAllocatedPages()
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
		tx.pagesToDelete = nil
		tx.allocatedPageNums = nil
		tx.closed = true
	}()

	root := tx.getRootBucket()
	for _, bucket := range tx.dirtyBuckets {
		data := bucket.serialize()
		if err = root.Put(bucket.name, data.Value); err != nil {
			return err
		}
	}

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
	// First write to physical log
	txLogStats, txLogErr := tx.db.dal.txLog.withStats(func() error {
		for _, node := range tx.dirtyNodes {
			if _, setNodeErr := tx.db.dal.setNode(node); setNodeErr != nil {
				return setNodeErr
			}
		}
		for _, page := range tx.dirtyPages {
			if setPageErr := tx.db.dal.SetPage(page); setPageErr != nil {
				return setPageErr
			}
		}
		for _, pageNum := range tx.pagesToDelete {
			if releaseErr := tx.db.dal.ReleasePage(pageNum); releaseErr != nil {
				return releaseErr
			}
		}

		if writeFreelistErr := WriteFreelist(tx.db.dal, tx.db.dal.freelist); writeFreelistErr != nil {
			return writeFreelistErr
		}
		if writeMetaErr := WriteMeta(tx.db.dal, tx.db.dal.meta); writeMetaErr != nil {
			return writeMetaErr
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
		return false, txLogErr
	}
	checkpointDurable := true

	// Second write to the Database storage
	dbWriteStarted := time.Now()
	for _, node := range tx.dirtyNodes {
		_, err := tx.db.dal.setNode(node)
		if err != nil {
			return checkpointDurable, err
		}
	}

	for _, page := range tx.dirtyPages {
		err := tx.db.dal.SetPage(page)
		if err != nil {
			return checkpointDurable, err
		}
	}

	//for _, pageNum := range tx.pagesToDelete {
	//	tx.db.dal.freelist.ReleasePage(pageNum)
	//}

	if err := WriteFreelist(tx.db.dal, tx.db.dal.freelist); err != nil {
		return checkpointDurable, err
	}
	if err := WriteMeta(tx.db.dal, tx.db.dal.meta); err != nil {
		return checkpointDurable, err
	}
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

	tx.db.dal.mergePendingJournalPages(commitPages)
	pendingPages := tx.db.dal.pendingJournalPages()

	txLogStats, err := tx.db.dal.txLog.ReplaceWithPages(pendingPages, true)
	if collectStats {
		commitStats.TxLogWriteDuration = txLogStats.writeDuration
		commitStats.TxLogSyncDuration = txLogStats.syncDuration
		commitStats.JournalPageCount = txLogStats.bufferedPages
		commitStats.JournalBytes = txLogStats.bufferedBytes
	}
	if err != nil {
		return false, err
	}
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

	batch := tx.db.registerGroupBatch()

	// Release the writer lock early so following writers can join the same flush batch.
	tx.once.Do(func() {
		tx.db.lock.Unlock()
		if tx.holdsGroupGate {
			tx.db.groupGate.RUnlock()
			tx.holdsGroupGate = false
		}
	})

	if err = tx.db.waitForGroupBatch(batch); err != nil {
		return false, err
	}
	return true, nil
}

func (tx *Tx) materializeCommitPages() ([]*Page, error) {
	for _, pageNum := range tx.pagesToDelete {
		if err := tx.db.dal.ReleasePage(pageNum); err != nil {
			return nil, err
		}
	}

	pagesByNum := make(map[uint64]*Page, len(tx.dirtyNodes)+len(tx.dirtyPages)+4)

	for _, node := range tx.dirtyNodes {
		page, err := tx.db.dal.marshalNodePage(node)
		if err != nil {
			return nil, err
		}
		pagesByNum[page.PageNumber] = clonePage(page)
	}

	for _, page := range tx.dirtyPages {
		pagesByNum[page.PageNumber] = clonePage(page)
	}

	freelistPages, err := BuildFreelistPages(tx.db.dal, tx.db.dal.freelist)
	if err != nil {
		return nil, err
	}
	for _, page := range freelistPages {
		pagesByNum[page.PageNumber] = page
	}
	pagesByNum[metaPageNumber] = BuildMetaPage(tx.db.dal.meta.pageSize, tx.db.dal.meta)

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

func (tx *Tx) bucketExists(name []byte) bool {
	rootBucket := tx.getRootBucket()
	_, found := rootBucket.Get(name)
	return found
}

func (tx *Tx) GetBucket(name []byte) (*Bucket, error) {
	if bucket, ok := tx.buckets.Load(string(name)); ok {
		return bucket.(*Bucket), nil
	}
	rootBucket := tx.getRootBucket()
	value, found := rootBucket.Get(name)
	if !found || value == nil {
		return nil, ErrBucketNotFound
	}
	bucket := newBucket([]byte{})
	bucket.deserialize(value)
	bucket.tx = tx
	bucket.name = name
	if tx.write {
		tx.dirtyBuckets[string(name)] = bucket
	}
	tx.buckets.Store(string(name), bucket)
	return bucket, nil
}

func (tx *Tx) CreateBucket(name []byte) (*Bucket, error) {
	if !tx.write {
		return nil, ErrWriteInRxTransaction
	}
	if existing, ok := tx.buckets.Load(string(name)); ok && existing != nil {
		return nil, ErrBucketExists
	}
	if tx.bucketExists(name) {
		return nil, ErrBucketExists
	}

	node := NewBNode()
	page, allocatePageErr := tx.allocatePage()
	if allocatePageErr != nil {
		return nil, allocatePageErr
	}
	node.PageNum = page.PageNumber
	tx.setNode(node)
	bucket := newBucket([]byte{})
	bucket.name = name
	bucket.root = page.PageNumber
	tx.dirtyBuckets[string(name)] = bucket
	tx.buckets.Store(string(name), bucket)
	return tx.createOrUpdateBucket(bucket)
}

func (tx *Tx) CreateBucketIfNotExists(name []byte) (*Bucket, error) {
	if !tx.write {
		return nil, ErrWriteInRxTransaction
	}
	bucket, err := tx.CreateBucket(name)
	if errors.Is(err, ErrBucketExists) {
		return tx.GetBucket(name)
	}
	return bucket, err
}

func (tx *Tx) DeleteBucket(name []byte) error {
	if !tx.write {
		return ErrWriteInRxTransaction
	}

	rootBucket := tx.getRootBucket()
	rawBucketValue, found := rootBucket.Get(name)
	if !found || rawBucketValue == nil {
		return ErrBucketNotFound
	}

	bucket := newBucket(name)
	bucket.deserialize(rawBucketValue)
	bucket.tx = tx

	if bucket.root != 0 {
		nodePages := make([]uint64, 0, 8)
		blobRoots := make(map[uint64]struct{})
		if err := tx.collectBucketGarbage(bucket.root, &nodePages, blobRoots); err != nil {
			return err
		}

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
	tx.buckets.Delete(string(name))

	err := rootBucket.Remove(name)
	if errors.Is(err, ErrNodeNotFound) {
		return ErrBucketNotFound
	}
	return err
}

func (tx *Tx) collectBucketGarbage(pageNum uint64, nodePages *[]uint64, blobRoots map[uint64]struct{}) error {
	node, err := tx.getNode(pageNum)
	if err != nil {
		return err
	}
	*nodePages = append(*nodePages, node.PageNum)

	for _, item := range node.items {
		if len(item.Value) == 0 {
			return ErrUnknownItemType
		}
		if item.Value[0] == ValueBlob {
			if len(item.Value) < 1+UInt64Size {
				return ErrUnknownItemType
			}
			blobRoot := binary.LittleEndian.Uint64(item.Value[1:])
			blobRoots[blobRoot] = struct{}{}
		}
	}

	for _, childPageNum := range node.childNodes {
		if err := tx.collectBucketGarbage(childPageNum, nodePages, blobRoots); err != nil {
			return err
		}
	}
	return nil
}

func (tx *Tx) Buckets() [][]byte {
	rootBucket := tx.getRootBucket()
	cursor := rootBucket.Cursor()
	buckets := make([][]byte, 0)
	for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
		buckets = append(buckets, k)
	}
	return buckets
}
