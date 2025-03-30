package storage

import (
	"errors"
	"sync"
)

type Tx struct {
	dirtyNodes        map[uint64]*BNode
	dirtyPages        map[uint64]*Page
	dirtyBuckets      map[string]*Bucket
	pagesToDelete     []uint64
	allocatedPageNums []uint64
	write             bool
	once              sync.Once
	db                *DB
}

func newTx(db *DB, write bool) *Tx {
	return &Tx{
		map[uint64]*BNode{},
		map[uint64]*Page{},
		map[string]*Bucket{},
		make([]uint64, 0),
		make([]uint64, 0),
		write,
		sync.Once{},
		db,
	}
}

func (tx *Tx) newNode(items []*Item, childNodes []uint64) *BNode {
	node := NewBNode()
	node.items = make([]*Item, len(items))
	copy(node.items, items)
	node.childNodes = append([]uint64{}, childNodes...)
	page, _ := tx.db.dal.AllocatePage()
	node.PageNum = page.PageNumber
	tx.allocatedPageNums = append(tx.allocatedPageNums, page.PageNumber)
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

func (tx *Tx) Rollback() {
	if !tx.write {
		tx.once.Do(func() {
			tx.db.lock.RUnlock()
			tx.db.TxN.Add(-1)
		})
		return
	}
	tx.dirtyNodes = nil
	tx.pagesToDelete = nil
	for _, pageNum := range tx.allocatedPageNums {
		tx.db.dal.freelist.ReleasePage(pageNum)
	}
	tx.allocatedPageNums = nil
	tx.once.Do(func() {
		tx.db.lock.Unlock()
	})
}

func (tx *Tx) Commit() error {
	if !tx.write {
		tx.once.Do(func() {
			tx.db.lock.RUnlock()
			tx.db.TxN.Add(-1)
		})
		return nil
	}

	root := tx.getRootBucket()
	for _, bucket := range tx.dirtyBuckets {
		data := bucket.serialize()
		err := root.Put(bucket.name, data.Value)
		if err != nil {
			return err
		}
	}

	for _, node := range tx.dirtyNodes {
		_, err := tx.db.dal.setNode(node)
		if err != nil {
			return err
		}
	}

	for _, page := range tx.dirtyPages {
		err := tx.db.dal.SetPage(page)
		if err != nil {
			return err
		}
	}

	for _, pageNum := range tx.pagesToDelete {
		tx.db.dal.freelist.ReleasePage(pageNum)
	}

	err := WriteFreelist(tx.db.dal, tx.db.dal.freelist)
	if err != nil {
		return err
	}
	err = WriteMeta(tx.db.dal, tx.db.dal.meta)
	if err != nil {
		return err
	}
	tx.dirtyNodes = nil
	tx.dirtyPages = nil
	tx.pagesToDelete = nil
	tx.allocatedPageNums = nil
	tx.once.Do(func() {
		tx.db.lock.Unlock()
	})
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

func (tx *Tx) GetBucket(name []byte) (*Bucket, error) {
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
	return bucket, nil
}

func (tx *Tx) CreateBucket(name []byte) (*Bucket, error) {
	if !tx.write {
		return nil, ErrWriteInRxTransaction
	}
	bucket, err := tx.GetBucket(name)
	if err == nil && bucket != nil {
		return nil, ErrBucketExists
	}
	node := NewBNode()
	page, setNodeErr := tx.db.dal.setNode(node)
	if setNodeErr != nil {
		return nil, setNodeErr
	}
	bucket = newBucket([]byte{})
	bucket.name = name
	bucket.root = page.PageNumber
	tx.dirtyBuckets[string(name)] = bucket
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
	return rootBucket.Remove(name)
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
