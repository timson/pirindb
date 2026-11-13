package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
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
	nameKey    string
	root       uint64
	counter    uint64
	itemsN     uint64
	blobsN     uint64
	bytesInUse uint64
	tx         *Tx
}

func (bucket *Bucket) markDirty() {
	if bucket == nil || bucket.tx == nil || len(bucket.name) == 0 {
		return
	}
	if bucket.tx.dirtyBuckets == nil {
		bucket.tx.dirtyBuckets = make(map[string]*Bucket)
	}
	bucket.tx.dirtyBuckets[bucket.nameKey] = bucket
}

func newBucket(name []byte) *Bucket {
	ownedName := cloneBytes(name)
	return &Bucket{
		root:    0,
		name:    ownedName,
		nameKey: string(ownedName),
	}
}

func (bucket *Bucket) Get(key []byte) ([]byte, bool) {
	value, found, err := bucket.GetE(key)
	if err != nil && bucket != nil && bucket.tx != nil {
		bucket.tx.recordError(err)
	}
	return value, found
}

func (bucket *Bucket) GetE(key []byte) ([]byte, bool, error) {
	value, found, err := bucket.getItem(key)
	if err != nil || !found {
		if err != nil && bucket != nil && bucket.tx != nil && bucket.tx.write {
			bucket.tx.recordError(err)
		}
		return nil, found, err
	}
	v, getErr := value.getValue(bucket.tx)
	if getErr != nil {
		if bucket.tx.write {
			bucket.tx.recordError(getErr)
		}
		return nil, false, getErr
	}

	return v, true, nil
}

func (bucket *Bucket) ValueLen(key []byte) (int, bool, error) {
	item, found, err := bucket.getItem(key)
	if err != nil || !found {
		if err != nil && bucket != nil && bucket.tx != nil && bucket.tx.write {
			bucket.tx.recordError(err)
		}
		return 0, found, err
	}
	valueLen, err := item.valueLen(bucket.tx)
	if err != nil {
		if bucket.tx.write {
			bucket.tx.recordError(err)
		}
		return 0, false, err
	}
	return valueLen, true, nil
}

func (bucket *Bucket) WriteValueTo(key []byte, w io.Writer) (int64, bool, error) {
	if w == nil {
		return 0, false, fmt.Errorf("value writer is nil")
	}
	item, found, err := bucket.getItem(key)
	if err != nil || !found {
		if err != nil && bucket != nil && bucket.tx != nil && bucket.tx.write {
			bucket.tx.recordError(err)
		}
		return 0, found, err
	}
	written, err := item.writeValueTo(bucket.tx, w)
	if err != nil {
		return written, false, err
	}
	return written, true, nil
}

func (bucket *Bucket) getItem(key []byte) (*Item, bool, error) {
	if err := bucket.ensureOpen(); err != nil {
		return nil, false, err
	}
	if bucket.root == 0 {
		return nil, false, nil
	}
	node, err := bucket.tx.getNode(bucket.root)
	if err != nil {
		return nil, false, err
	}
	pos, foundNode, found, findErr := node.FindExactE(bucket.tx, key)
	if findErr != nil {
		return nil, false, findErr
	}
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

func (bucket *Bucket) deserialize(data []byte) error {
	if len(data) != BucketTotalSize {
		return fmt.Errorf("%w: bucket descriptor has %d bytes, expected %d", ErrCorruptedNode, len(data), BucketTotalSize)
	}
	bucket.root = binary.LittleEndian.Uint64(data[BucketRootOffset:])
	bucket.counter = binary.LittleEndian.Uint64(data[BucketCounterOffset:])
	bucket.itemsN = binary.LittleEndian.Uint64(data[BucketItemNOffset:])
	bucket.blobsN = binary.LittleEndian.Uint64(data[BucketBlobNOffset:])
	bucket.bytesInUse = binary.LittleEndian.Uint64(data[BucketBytesInUseOffset:])
	minimumRoot := uint64(legacyRootPageNumber)
	if bucket.tx != nil && bucket.tx.db != nil && bucket.tx.db.dal.pageChecksums {
		minimumRoot = rootPageNumber
	}
	if bucket.root < minimumRoot {
		return fmt.Errorf("%w: invalid bucket root %d", ErrCorruptedNode, bucket.root)
	}
	if bucket.tx != nil && bucket.tx.db != nil && bucket.tx.db.dal != nil {
		dal := bucket.tx.db.dal
		if bucket.root >= dal.maxPages || bucket.root > dal.freelist.currentPage || dal.freelist.containsReleasedPage(bucket.root) || dal.freelist.isFreelistStoragePage(bucket.root) {
			return fmt.Errorf("%w: bucket root %d is not a live data page", ErrCorruptedNode, bucket.root)
		}
	}
	return nil
}

func (bucket *Bucket) Put(key, value []byte) (returnErr error) {
	if err := bucket.ensureWritable(); err != nil {
		return err
	}
	if len(key) >= MaxKeySize {
		return ErrKeyTooLarge
	}
	if len(value) >= OneGigabyte {
		return ErrValueTooLarge
	}
	defer func() {
		if returnErr != nil {
			bucket.tx.recordError(returnErr)
		}
	}()
	item := Item{Value: value}
	if returnErr = item.setValue(bucket.tx); returnErr != nil {
		return returnErr
	}
	returnErr = bucket.putEncodedValue(key, item.Value, len(value), len(value) > MaxValueSize)
	if returnErr == nil {
		returnErr = bucket.tx.ensureDirtyLimit()
	}
	if returnErr == nil {
		bucket.markDirty()
	}
	return returnErr
}

func (bucket *Bucket) PutReader(key []byte, r io.Reader, valueLen int64) (returnErr error) {
	if err := bucket.ensureWritable(); err != nil {
		return err
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
	if r == nil {
		return fmt.Errorf("value reader is nil")
	}
	defer func() {
		if returnErr != nil {
			bucket.tx.recordError(returnErr)
		}
	}()
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
	returnErr = bucket.putEncodedValue(key, encodedValue, int(valueLen), true)
	if returnErr == nil {
		returnErr = bucket.tx.ensureDirtyLimit()
	}
	if returnErr == nil {
		bucket.markDirty()
	}
	return returnErr
}

func (bucket *Bucket) putEncodedValue(key []byte, encodedValue []byte, logicalValueLen int, newIsBlob bool) error {
	var root *BNode
	var err error
	var keyExists bool
	newValueLen := logicalValueLen

	if err := bucket.ensureWritable(); err != nil {
		return err
	}
	if len(key) >= MaxKeySize {
		return ErrKeyTooLarge
	}
	if logicalValueLen >= OneGigabyte {
		return ErrValueTooLarge
	}

	item := Item{
		Key: cloneBytes(key),
		// Both callers pass a newly allocated encoded value. Taking ownership
		// avoids copying every value a second time while still isolating the
		// stored item from caller-owned key/value buffers.
		Value: encodedValue,
	}

	// First insert: no root exists yet. Create a root node and set it
	if bucket.root == 0 {
		root, err = bucket.tx.newNode([]*Item{&item}, []uint64{})
		if err != nil {
			return err
		}
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
	insertionIndex, nodeToInsertIn, breadcrumbs, nodesAlongPath, found, findErr := root.findPathReuseE(bucket.tx, item.Key, false)
	if findErr != nil {
		return findErr
	}
	if !found {
		return ErrNodeNotFound
	}
	if nodeToInsertIn == nil || insertionIndex < 0 || insertionIndex > len(nodeToInsertIn.items) {
		return fmt.Errorf("%w: invalid insertion result", ErrCorruptedNode)
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

	// Rebalance from bottom-up, excluding root
	for i := len(nodesAlongPath) - 2; i >= 0; i-- {
		parentNode := nodesAlongPath[i]
		node := nodesAlongPath[i+1]
		nodeIndex := breadcrumbs[i+1]
		overPopulated, sizeErr := node.isOverPopulated(bucket.tx.db.dal.maxThreshold())
		if sizeErr != nil {
			return sizeErr
		}
		if overPopulated {
			if splitErr := parentNode.splitChild(bucket.tx, node, nodeIndex); splitErr != nil {
				return splitErr
			}
		}
	}

	// Re-check root in case it was affected and needs splitting
	rootNode := nodesAlongPath[0]
	rootOverPopulated, sizeErr := rootNode.isOverPopulated(bucket.tx.db.dal.maxThreshold())
	if sizeErr != nil {
		return sizeErr
	}
	if rootOverPopulated {
		newRoot, newRootErr := bucket.tx.newNode([]*Item{}, []uint64{rootNode.PageNum})
		if newRootErr != nil {
			return newRootErr
		}
		logger.Debug("splitChild root node", "oldPageNum", rootNode.PageNum, "newPageNum", newRoot.PageNum)
		if splitErr := newRoot.splitChild(bucket.tx, rootNode, 0); splitErr != nil {
			return splitErr
		}

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

func (bucket *Bucket) Remove(key []byte) (returnErr error) {
	if err := bucket.ensureWritable(); err != nil {
		return err
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
	removeItemIndex, nodeToRemoveFrom, breadcrumbs, nodesAlongPath, found, findErr := rootNode.findPathReuseE(bucket.tx, key, true)
	if findErr != nil {
		return findErr
	}
	if !found {
		return ErrNodeNotFound
	}

	// Defensive check: key was reported found, but the result is malformed.
	if nodeToRemoveFrom == nil || removeItemIndex < 0 || removeItemIndex >= len(nodeToRemoveFrom.items) {
		err = fmt.Errorf("%w: invalid removal result", ErrCorruptedNode)
		bucket.tx.recordError(err)
		return err
	}
	defer func() {
		if returnErr != nil {
			bucket.tx.recordError(returnErr)
		}
	}()

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
		affectedIndexes, predecessorPath, removeErr := nodeToRemoveFrom.removeItemFromInternal(bucket.tx, removeItemIndex)
		if removeErr != nil {
			return removeErr
		}
		// Add any affected child nodes to the breadcrumb path
		breadcrumbs = append(breadcrumbs, affectedIndexes...)
		nodesAlongPath = append(nodesAlongPath, predecessorPath...)
	}

	// Persist the updated node in the transaction state
	bucket.tx.setNode(nodeToRemoveFrom)

	// Rebalance from the bottom-up (excluding the root node)
	for i := len(nodesAlongPath) - 2; i >= 0; i-- {
		parentNode := nodesAlongPath[i]
		node := nodesAlongPath[i+1]
		underPopulated, sizeErr := node.isUnderPopulated(bucket.tx.db.dal.minThreshold())
		if sizeErr != nil {
			return sizeErr
		}
		if underPopulated {
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
		if len(rootNode.childNodes) != 1 {
			return ErrCorruptedNode
		}
		bucket.root = rootNode.childNodes[0]
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

	returnErr = bucket.tx.ensureDirtyLimit()
	if returnErr == nil {
		bucket.markDirty()
	}
	return returnErr
}

func (bucket *Bucket) Cursor() *Cursor {
	if bucket == nil {
		return &Cursor{err: ErrTxClosed}
	}
	return &Cursor{bucket: bucket, tx: bucket.tx}
}

func (bucket *Bucket) ForEach(fn func(k, v []byte) error) error {
	if fn == nil {
		return fmt.Errorf("foreach callback is nil")
	}
	cursor := bucket.Cursor()
	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		if err := fn(k, v); err != nil {
			return err
		}
	}
	return cursor.Err()
}

func (bucket *Bucket) NextSequence() (uint64, error) {
	if err := bucket.ensureWritable(); err != nil {
		return 0, err
	}
	if bucket.counter == ^uint64(0) {
		return 0, fmt.Errorf("bucket sequence overflow")
	}
	bucket.counter++
	bucket.markDirty()
	return bucket.counter, nil
}

func (bucket *Bucket) Sequence() uint64 {
	if bucket == nil {
		return 0
	}
	return bucket.counter
}

func (bucket *Bucket) ItemCount() uint64 {
	if bucket == nil {
		return 0
	}
	return bucket.itemsN
}
