package storage

import (
	"bytes"
	"encoding/binary"
	"io"
)

const (
	MaxKeySize   = 512
	MaxValueSize = 1024

	BucketRootSize       = UInt64Size
	BucketCounterSize    = UInt64Size
	BucketItemNSize      = UInt64Size
	BucketBlobsNSize     = UInt64Size
	BucketBytesInUseSize = UInt64Size
	BucketTotalSize      = BucketRootSize + BucketCounterSize + BucketItemNSize + BucketBlobsNSize + BucketBytesInUseSize

	BucketRootOffset       = 0
	BucketCounterOffset    = BucketRootOffset + BucketRootSize
	BucketItemNOffset      = BucketCounterOffset + BucketCounterSize
	BucketBlobNOffset      = BucketItemNOffset + BucketItemNSize
	BucketBytesInUseOffset = BucketBlobNOffset + BucketBlobsNSize
)

type Bucket struct {
	name       []byte
	root       uint64
	counter    uint64
	itemsN     uint64
	blobsN     uint64
	bytesInUse uint64
	tx         *Tx
}

func newBucket(name []byte) *Bucket {
	return &Bucket{
		root: 0,
		name: name,
	}
}

func (bucket *Bucket) Get(key []byte) ([]byte, bool) {
	value, found, err := bucket.getItem(key)
	if err != nil || !found {
		return nil, false
	}
	v, getErr := value.getValue(bucket.tx)
	if getErr != nil {
		return nil, false
	}

	return v, true
}

func (bucket *Bucket) ValueLen(key []byte) (int, bool, error) {
	item, found, err := bucket.getItem(key)
	if err != nil || !found {
		return 0, found, err
	}
	valueLen, err := item.valueLen(bucket.tx)
	if err != nil {
		return 0, false, err
	}
	return valueLen, true, nil
}

func (bucket *Bucket) WriteValueTo(key []byte, w io.Writer) (int64, bool, error) {
	item, found, err := bucket.getItem(key)
	if err != nil || !found {
		return 0, found, err
	}
	written, err := item.writeValueTo(bucket.tx, w)
	if err != nil {
		return written, false, err
	}
	return written, true, nil
}

func (bucket *Bucket) getItem(key []byte) (*Item, bool, error) {
	if bucket.tx == nil {
		return nil, false, ErrTxClosed
	}
	if bucket.root == 0 {
		return nil, false, nil
	}
	node, err := bucket.tx.getNode(bucket.root)
	if err != nil {
		return nil, false, err
	}
	pos, foundNode, found := node.FindExact(bucket.tx, key)
	if !found {
		return nil, false, nil
	}
	return foundNode.items[pos], true, nil
}

// Bucket value map
// 0            8            16         24         32            40
// +------------+------------+-----------+-----------+------------+
// |   Root     |  Counter   |   ItemN   |   BlobN   | BytesInUse |
// |  uint64    |  uint64    |  uint64   |  uint64   |  uint64    |
// +------------+------------+-----------+-----------+------------+

func (bucket *Bucket) serialize() *Item {
	b := make([]byte, BucketTotalSize)
	binary.LittleEndian.PutUint64(b[BucketRootOffset:], bucket.root)
	binary.LittleEndian.PutUint64(b[BucketCounterOffset:], bucket.counter)
	binary.LittleEndian.PutUint64(b[BucketItemNOffset:], bucket.itemsN)
	binary.LittleEndian.PutUint64(b[BucketBlobNOffset:], bucket.blobsN)
	binary.LittleEndian.PutUint64(b[BucketBytesInUseOffset:], bucket.bytesInUse)
	return &Item{bucket.name, b}
}

func (bucket *Bucket) deserialize(data []byte) {
	if len(data) != 0 {
		bucket.root = binary.LittleEndian.Uint64(data[BucketRootOffset:])
		bucket.counter = binary.LittleEndian.Uint64(data[BucketCounterOffset:])
		bucket.itemsN = binary.LittleEndian.Uint64(data[BucketItemNOffset:])
		bucket.blobsN = binary.LittleEndian.Uint64(data[BucketBlobNOffset:])
		bucket.bytesInUse = binary.LittleEndian.Uint64(data[BucketBytesInUseOffset:])
	}
}

func (bucket *Bucket) getNodes(indexes []int) ([]*BNode, error) {
	root, err := bucket.tx.getNode(bucket.root)
	if err != nil {
		return nil, err
	}

	nodes := []*BNode{root}
	child := root
	for i := 1; i < len(indexes); i++ {
		if indexes[i] < 0 || indexes[i] >= len(child.childNodes) {
			return nil, ErrNodeNotFound
		}
		child, err = bucket.tx.getNode(child.childNodes[indexes[i]])
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, child)
	}
	return nodes, nil
}

func (bucket *Bucket) Put(key, value []byte) error {
	item := Item{
		Key:   key,
		Value: value,
	}
	if err := item.setValue(bucket.tx); err != nil {
		return err
	}
	return bucket.putEncodedValue(key, item.Value, len(value), len(value) > MaxValueSize)
}

func (bucket *Bucket) PutReader(key []byte, r io.Reader, valueLen int64) error {
	if bucket.tx == nil {
		return ErrTxClosed
	}
	if len(key) >= MaxKeySize {
		return ErrKeyTooLarge
	}
	if valueLen < 0 {
		return ErrValueTooLarge
	}
	if valueLen >= OneGigabyte {
		return ErrValueTooLarge
	}
	if valueLen <= MaxValueSize {
		buf := make([]byte, int(valueLen))
		if _, err := io.ReadFull(r, buf); err != nil {
			return err
		}
		return bucket.Put(key, buf)
	}

	pageNum, err := SaveBlobFromReader(bucket.tx, r, valueLen)
	if err != nil {
		return err
	}
	encodedValue := make([]byte, 1+UInt64Size)
	encodedValue[0] = ValueBlob
	binary.LittleEndian.PutUint64(encodedValue[1:], pageNum)
	return bucket.putEncodedValue(key, encodedValue, int(valueLen), true)
}

func (bucket *Bucket) putEncodedValue(key []byte, encodedValue []byte, logicalValueLen int, newIsBlob bool) error {
	var root *BNode
	var err error
	var keyExists bool
	newValueLen := logicalValueLen

	if bucket.tx == nil {
		return ErrTxClosed
	}
	if len(key) >= MaxKeySize {
		return ErrKeyTooLarge
	}
	if logicalValueLen >= OneGigabyte {
		return ErrValueTooLarge
	}

	item := Item{
		Key:   key,
		Value: encodedValue,
	}

	// First insert: no root exists yet. Create a root node and set it
	if bucket.root == 0 {
		root = bucket.tx.newNode([]*Item{&item}, []uint64{})
		bucket.tx.setNode(root)
		bucket.root = root.PageNum
		bucket.itemsN++
		bucket.bytesInUse += uint64(len(key) + newValueLen)
		if newIsBlob {
			bucket.blobsN++
		}
		return nil
	}

	// Load root node
	root, err = bucket.tx.getNode(bucket.root)
	if err != nil {
		return err
	}

	// Traverse the tree to find the target node and index for insertion
	insertionIndex, nodeToInsertIn, breadcrumbs, found := root.Find(bucket.tx, item.Key, false)
	if !found {
		return ErrNodeNotFound
	}

	// If the key already exists, update the value
	if nodeToInsertIn.items != nil && insertionIndex < len(nodeToInsertIn.items) && bytes.Equal(nodeToInsertIn.items[insertionIndex].Key, key) {
		oldValueLen, oldWasBlob, oldValueErr := nodeToInsertIn.items[insertionIndex].deleteValue(bucket.tx)
		if oldValueErr != nil {
			return oldValueErr
		}
		nodeToInsertIn.items[insertionIndex] = &item
		keyExists = true

		if newValueLen >= oldValueLen {
			bucket.bytesInUse += uint64(newValueLen - oldValueLen)
		} else {
			bucket.bytesInUse -= uint64(oldValueLen - newValueLen)
		}
		if oldWasBlob && !newIsBlob {
			if bucket.blobsN > 0 {
				bucket.blobsN--
			}
		}
		if !oldWasBlob && newIsBlob {
			bucket.blobsN++
		}
	} else {
		// Otherwise, insert the new item at the appropriate position
		nodeToInsertIn.insertItemAt(&item, insertionIndex)
	}
	bucket.tx.setNode(nodeToInsertIn)

	// Fetch all ancestor nodes along the path (breadcrumbs) to rebalance if needed
	nodesAlongPath, err := bucket.getNodes(breadcrumbs)
	if err != nil {
		return err
	}

	// Rebalance from bottom-up, excluding root
	for i := len(nodesAlongPath) - 2; i >= 0; i-- {
		parentNode := nodesAlongPath[i]
		node := nodesAlongPath[i+1]
		nodeIndex := breadcrumbs[i+1]
		if node.isOverPopulated(bucket.tx.db.dal.maxThreshold()) {
			parentNode.splitChild(bucket.tx, node, nodeIndex)
		}
	}

	// Re-check root in case it was affected and needs splitting
	rootNode := nodesAlongPath[0]
	if rootNode.isOverPopulated(bucket.tx.db.dal.maxThreshold()) {
		newRoot := bucket.tx.newNode([]*Item{}, []uint64{rootNode.PageNum})
		logger.Debug("splitChild root node", "oldPageNum", rootNode.PageNum, "newPageNum", newRoot.PageNum)
		newRoot.splitChild(bucket.tx, rootNode, 0)

		// commit newly created root
		bucket.tx.setNode(newRoot)
		bucket.root = newRoot.PageNum

		// If this is the main bucket, update DB metadata
		if bucket.tx.db.dal.meta.root == rootNode.PageNum {
			bucket.tx.db.dal.meta.root = newRoot.PageNum
		} else {
			// If it's a sub-bucket, persist the bucket with the new root
			_, updateBucketErr := bucket.tx.createOrUpdateBucket(bucket)
			if updateBucketErr != nil {
				return updateBucketErr
			}
		}
	}

	if !keyExists {
		bucket.itemsN++
		bucket.bytesInUse += uint64(len(key) + newValueLen)
		if newIsBlob {
			bucket.blobsN++
		}
	}

	return nil
}

func (bucket *Bucket) Remove(key []byte) error {
	if bucket.tx == nil {
		return ErrTxClosed
	}
	if bucket.root == 0 {
		return ErrNodeNotFound
	}
	// Fetch the root node of the bucket
	rootNode, err := bucket.tx.getNode(bucket.root)
	if err != nil {
		return err
	}

	// Search for the key and collect the path (nodesAlongPath) to the node
	removeItemIndex, nodeToRemoveFrom, breadcrumbs, found := rootNode.Find(bucket.tx, key, true)
	if !found {
		return ErrNodeNotFound
	}

	// Defensive check: key was found, but index is invalid.
	if removeItemIndex == -1 {
		return nil
	}

	// Attempt to delete the blob before removing the item
	item := nodeToRemoveFrom.items[removeItemIndex]
	valueLen, wasBlob, blobDeleteErr := item.deleteValue(bucket.tx)
	if blobDeleteErr != nil {
		return blobDeleteErr
	}

	if nodeToRemoveFrom.isLeaf() {
		// If it's a leaf node, remove the item directly
		nodeToRemoveFrom.removeItemAtLeaf(removeItemIndex)
	} else {
		// If it's an internal node, handle deletion and restructure as needed
		affectedNodes, removeErr := nodeToRemoveFrom.removeItemFromInternal(bucket.tx, removeItemIndex)
		if removeErr != nil {
			return removeErr
		}
		// Add any affected child nodes to the breadcrumb path
		breadcrumbs = append(breadcrumbs, affectedNodes...)
	}

	// Persist the updated node in the transaction state
	bucket.tx.setNode(nodeToRemoveFrom)

	nodesAlongPath, err := bucket.getNodes(breadcrumbs)
	if err != nil {
		return err
	}

	// Rebalance from the bottom-up (excluding the root node)
	for i := len(nodesAlongPath) - 2; i >= 0; i-- {
		parentNode := nodesAlongPath[i]
		node := nodesAlongPath[i+1]
		if node.isUnderPopulated(bucket.tx.db.dal.minThreshold()) {
			err = parentNode.rebalanceRemove(bucket.tx, node, breadcrumbs[i+1])
			if err != nil {
				return err
			}
		}
	}

	// If the root node is now empty but has children, promote the first child as the new root
	rootNode = nodesAlongPath[0]
	if len(rootNode.items) == 0 && len(rootNode.childNodes) > 0 {
		oldRootPage := rootNode.PageNum
		bucket.root = nodesAlongPath[1].PageNum
		bucket.tx.deletePage(oldRootPage)
		if bucket.tx.db.dal.meta.root == oldRootPage {
			bucket.tx.db.dal.meta.root = bucket.root
		} else {
			_, err = bucket.tx.createOrUpdateBucket(bucket)
			if err != nil {
				return err
			}
		}
	}

	// adjust bucket stat
	if bucket.itemsN > 0 {
		bucket.itemsN--
	}
	removedBytes := len(key) + valueLen
	if bucket.bytesInUse >= uint64(removedBytes) {
		bucket.bytesInUse -= uint64(removedBytes)
	} else {
		bucket.bytesInUse = 0
	}
	if wasBlob {
		if bucket.blobsN > 0 {
			bucket.blobsN--
		}
	}

	return nil
}

func (bucket *Bucket) Cursor() *Cursor {
	return &Cursor{bucket: bucket, tx: bucket.tx}
}

func (bucket *Bucket) ForEach(fn func(k, v []byte) error) error {
	cursor := bucket.Cursor()
	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		if err := fn(k, v); err != nil {
			return err
		}
	}
	return nil
}

func (bucket *Bucket) NextSequence() (uint64, error) {
	if bucket.tx == nil {
		return 0, ErrTxClosed
	}
	if !bucket.tx.write {
		return 0, ErrWriteInRxTransaction
	}
	bucket.counter++
	return bucket.counter, nil
}

func (bucket *Bucket) Sequence() uint64 {
	return bucket.counter
}

func (bucket *Bucket) ItemCount() uint64 {
	return bucket.itemsN
}
