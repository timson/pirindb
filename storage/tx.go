package storage

import (
	"encoding/binary"
	"errors"
	"sync"
)

type Tx struct {
	dirtyNodes        map[uint64]*BNode
	dirtyPages        map[uint64]*Page
	dirtyBuckets      map[string]*Bucket
	pagesToDelete     []uint64
	allocatedPageNums []uint64
	originalMetaRoot  uint64
	write             bool
	closed            bool
	once              sync.Once
	db                *DB
	buckets           sync.Map
}

func newTx(db *DB, write bool) *Tx {
	return &Tx{
		map[uint64]*BNode{},
		map[uint64]*Page{},
		map[string]*Bucket{},
		make([]uint64, 0),
		make([]uint64, 0),
		db.dal.meta.root,
		write,
		false,
		sync.Once{},
		db,
		sync.Map{},
	}
}

func (tx *Tx) allocatePage() (*Page, error) {
	page, err := tx.db.dal.AllocatePage()
	if err != nil {
		return nil, err
	}
	tx.allocatedPageNums = append(tx.allocatedPageNums, page.PageNumber)
	return page, nil
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

	node, err := tx.db.dal.getNode(page)
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
	page, err := tx.db.dal.GetPage(pageNum)
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
	clone.freelistPages = append([]uint64(nil), src.freelistPages...)
	if src.releasedPagesSet != nil {
		clone.releasedPagesSet = make(map[uint64]struct{}, len(src.releasedPagesSet))
		for pageNum := range src.releasedPagesSet {
			clone.releasedPagesSet[pageNum] = struct{}{}
		}
	} else {
		clone.releasedPagesSet = nil
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
		tx.closed = true
		return
	}
	defer func() {
		tx.allocatedPageNums = nil
		tx.once.Do(func() {
			tx.db.lock.Unlock()
		})
		tx.closed = true
	}()

	tx.db.dal.meta.root = tx.originalMetaRoot
	tx.dirtyNodes = nil
	tx.dirtyPages = nil
	tx.dirtyBuckets = nil
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
	defer func() {
		if err != nil && !checkpointDurable {
			tx.db.dal.meta.root = tx.originalMetaRoot
			tx.db.dal.freelist = freelistSnapshot
			tx.releaseAllocatedPages()
		}
		tx.once.Do(func() {
			tx.db.lock.Unlock()
		})
		tx.dirtyNodes = nil
		tx.dirtyPages = nil
		tx.dirtyBuckets = nil
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

	// First write to physical log
	err = tx.db.dal.txLog.With(func() error {
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
	if err != nil {
		return err
	}
	checkpointDurable = true

	// Second write to the Database storage
	for _, node := range tx.dirtyNodes {
		_, err = tx.db.dal.setNode(node)
		if err != nil {
			return err
		}
	}

	for _, page := range tx.dirtyPages {
		err = tx.db.dal.SetPage(page)
		if err != nil {
			return err
		}
	}

	//for _, pageNum := range tx.pagesToDelete {
	//	tx.db.dal.freelist.ReleasePage(pageNum)
	//}

	err = WriteFreelist(tx.db.dal, tx.db.dal.freelist)
	if err != nil {
		return err
	}
	err = WriteMeta(tx.db.dal, tx.db.dal.meta)
	if err != nil {
		return err
	}
	if err = tx.db.dal.Sync(); err != nil {
		return err
	}
	if err = tx.db.dal.txLog.Clear(); err != nil {
		return err
	}

	return nil
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
