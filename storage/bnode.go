package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	ValueSimple = 0
	ValueBlob   = 1

	NodePageTypeSize = UInt8Size
	NodeTypeSize     = UInt8Size
	NodeNumItemsSize = UInt16Size
	NodeHeaderSize   = NodePageTypeSize + NodeTypeSize + NodeNumItemsSize

	NodePageTypeOffset = 0
	NodeTypeOffset     = NodePageTypeOffset + NodePageTypeSize
	NodeNumItemsOffset = NodeTypeOffset + NodeTypeSize
)

// BNode map
// 0           1            2            4                   ...                    ...                ...
// +-----------+------------+------------+--------------------+----------------------+--------------------+
// | Page Type | Node Type  | Num Items  |   Child Nodes      |     KV Offsets       |      KV Data       |
// | uint8     |   uint8    |  uint16    | uint64[itemsN]   | uint16[itemsN]     |   (bytes[])        |
// +-----------+------------+------------+--------------------+----------------------+--------------------+

// Item represents a Key-value pair stored in a B-Tree node.
type Item struct {
	Key   []byte
	Value []byte
}

func (item *Item) setValue(tx *Tx) error {
	if len(item.Value) > MaxValueSize {
		blob, err := NewBlob(item.Value)
		if err != nil {
			return err
		}
		pageNum, err := blob.Save(tx)
		if err != nil {
			return err
		}
		item.Value = make([]byte, 9)
		item.Value[0] = ValueBlob
		binary.LittleEndian.PutUint64(item.Value[1:], pageNum)
	} else {
		encoded := make([]byte, 1+len(item.Value))
		encoded[0] = ValueSimple
		copy(encoded[1:], item.Value)
		item.Value = encoded
	}

	return nil
}

func (item *Item) getValue(tx *Tx) ([]byte, error) {
	if len(item.Value) == 0 {
		return nil, ErrUnknownItemType
	}
	switch item.Value[0] {
	case ValueSimple:
		return cloneBytes(item.Value[1:]), nil
	case ValueBlob:
		if len(item.Value) != 1+UInt64Size {
			return nil, ErrUnknownItemType
		}
		pageNum := binary.LittleEndian.Uint64(item.Value[1:])
		blob, err := GetBlob(tx, pageNum)
		if err != nil {
			return nil, err
		}
		return cloneBytes(blob.data), nil
	}
	return nil, ErrUnknownItemType
}

func (item *Item) valueLen(tx *Tx) (int, error) {
	if len(item.Value) == 0 {
		return 0, ErrUnknownItemType
	}
	switch item.Value[0] {
	case ValueSimple:
		return len(item.Value) - 1, nil
	case ValueBlob:
		if len(item.Value) != 1+UInt64Size {
			return 0, ErrUnknownItemType
		}
		pageNum := binary.LittleEndian.Uint64(item.Value[1:])
		return BlobSize(tx, pageNum)
	}
	return 0, ErrUnknownItemType
}

func (item *Item) writeValueTo(tx *Tx, w io.Writer) (int64, error) {
	if len(item.Value) == 0 {
		return 0, ErrUnknownItemType
	}
	switch item.Value[0] {
	case ValueSimple:
		if len(item.Value) == 1 {
			return 0, nil
		}
		n, err := w.Write(item.Value[1:])
		if err != nil {
			return int64(n), err
		}
		if n != len(item.Value)-1 {
			return int64(n), io.ErrShortWrite
		}
		return int64(n), nil
	case ValueBlob:
		if len(item.Value) != 1+UInt64Size {
			return 0, ErrUnknownItemType
		}
		pageNum := binary.LittleEndian.Uint64(item.Value[1:])
		return WriteBlobTo(tx, pageNum, w)
	}
	return 0, ErrUnknownItemType
}

func (item *Item) deleteValue(tx *Tx) (int, bool, error) {
	var err error
	if len(item.Value) == 0 {
		return 0, false, ErrUnknownItemType
	}

	dataLen := len(item.Value) - 1
	blob := false
	switch item.Value[0] {
	case ValueSimple:
	case ValueBlob:
		blob = true
		if len(item.Value) != 1+UInt64Size {
			return 0, false, ErrUnknownItemType
		}
		pageNum := binary.LittleEndian.Uint64(item.Value[1:])
		dataLen, err = DeleteBlob(tx, pageNum)
		if err != nil {
			return 0, false, err
		}
	default:
		return 0, false, ErrUnknownItemType
	}
	return dataLen, blob, nil
}

// BNode represents a node in a B-Tree.
// It contains Key-value pairs and child nodes.
type BNode struct {
	// dal is the data access layer used to interact with the underlying storage.

	// PageNum is the pageNum number of the node in the storage.
	PageNum uint64

	// items is a slice of Key-value pairs stored in the node.
	items []*Item

	// childNodes is a slice of pageNum numbers of the child nodes.
	childNodes []uint64
}

func NewBNode() *BNode {
	return &BNode{}
}

func (node *BNode) Serialize(data []byte) error {
	return node.serializePage(data, true)
}

func (node *BNode) serializePage(data []byte, checksums bool) error {
	payload, err := pagePayload(data, checksums)
	if err != nil {
		return err
	}
	if err = node.serializePayload(payload); err != nil {
		return err
	}
	return sealPageData(data, checksums)
}

func (node *BNode) serializePayload(data []byte) error {
	encodedSize, err := node.encodedSize()
	if err != nil {
		return err
	}
	if encodedSize > len(data) {
		return ErrNotEnoughSpace
	}
	var bitSetVar uint8
	if node.isLeaf() {
		bitSetVar = 1
	}

	clear(data)

	data[NodePageTypeOffset] = NodePage
	data[NodeTypeOffset] = bitSetVar
	binary.LittleEndian.PutUint16(data[NodeNumItemsOffset:], uint16(node.numItems()))
	pos := NodeHeaderSize

	binary.LittleEndian.PutUint16(data[pos:], uint16(len(node.childNodes)))
	pos += UInt16Size
	for _, childNode := range node.childNodes {
		binary.LittleEndian.PutUint64(data[pos:], childNode)
		pos += UInt64Size
	}

	kvPos := pos
	for _, item := range node.items {
		binary.LittleEndian.PutUint16(data[kvPos:], uint16(len(item.Key)))
		kvPos += UInt16Size
		binary.LittleEndian.PutUint16(data[kvPos:], uint16(len(item.Value)))
		kvPos += UInt16Size
		copy(data[kvPos:], item.Key)
		kvPos += len(item.Key)
		copy(data[kvPos:], item.Value)
		kvPos += len(item.Value)
	}

	return nil
}

func (node *BNode) Deserialize(data []byte) error {
	// The exported decoder cannot assume ownership of a caller buffer. DAL
	// reads use deserializePage directly with their private page allocation.
	return node.deserializePage(cloneBytes(data), true)
}

func (node *BNode) deserializePage(data []byte, checksums bool) error {
	if err := verifyPageData(data, checksums); err != nil {
		return err
	}
	payload, err := pagePayload(data, checksums)
	if err != nil {
		return err
	}
	return node.deserializePayload(payload, checksums)
}

func (node *BNode) deserializePayload(data []byte, checksums bool) error {
	if len(data) < NodeHeaderSize+UInt16Size {
		return fmt.Errorf("%w: node page has %d bytes", ErrCorruptedNode, len(data))
	}
	if data[NodePageTypeOffset] != NodePage {
		return fmt.Errorf("%w: invalid page type %d", ErrCorruptedNode, data[NodePageTypeOffset])
	}
	isLeaf := data[NodeTypeOffset]
	if isLeaf != 0 && isLeaf != 1 {
		return fmt.Errorf("%w: invalid node type %d", ErrCorruptedNode, isLeaf)
	}
	numItems := int(binary.LittleEndian.Uint16(data[NodeNumItemsOffset:]))
	pos := NodeHeaderSize
	numbChildren := int(binary.LittleEndian.Uint16(data[pos:]))
	pos += UInt16Size
	if isLeaf == 1 && numbChildren != 0 {
		return fmt.Errorf("%w: leaf has %d children", ErrCorruptedNode, numbChildren)
	}
	if isLeaf == 0 && numbChildren != numItems+1 {
		return fmt.Errorf("%w: internal node has %d items and %d children", ErrCorruptedNode, numItems, numbChildren)
	}
	minimumEncodedSize := NodeHeaderSize + UInt16Size + numbChildren*UInt64Size + numItems*(2*UInt16Size+1)
	if minimumEncodedSize > len(data) {
		return fmt.Errorf("%w: declared node entries cannot fit in page", ErrCorruptedNode)
	}
	childrenBytes := numbChildren * UInt64Size
	if childrenBytes < 0 || pos > len(data)-childrenBytes {
		return fmt.Errorf("%w: child array exceeds page", ErrCorruptedNode)
	}
	node.items = make([]*Item, numItems)
	// Keep decoded items contiguous and point their key/value slices directly
	// into this node's private page buffer. GetPage returns an owned buffer and
	// item data is immutable, so per-key and per-value copies only multiplied
	// allocations for every page read.
	decodedItems := make([]Item, numItems)
	if isLeaf == 0 {
		node.childNodes = make([]uint64, numbChildren)
	} else {
		node.childNodes = nil
	}

	if isLeaf == 0 {
		minimumPage := uint64(legacyRootPageNumber)
		if checksums {
			minimumPage = rootPageNumber
		}
		seenChildren := make(map[uint64]struct{}, numbChildren)
		for idx := range numbChildren {
			childNode := binary.LittleEndian.Uint64(data[pos:])
			if childNode < minimumPage {
				return fmt.Errorf("%w: invalid child page %d", ErrCorruptedNode, childNode)
			}
			if _, exists := seenChildren[childNode]; exists {
				return fmt.Errorf("%w: duplicate child page %d", ErrCorruptedNode, childNode)
			}
			seenChildren[childNode] = struct{}{}
			pos += UInt64Size
			node.childNodes[idx] = childNode
		}
	}
	var previousKey []byte
	for idx := range numItems {
		if pos > len(data)-2*UInt16Size {
			return fmt.Errorf("%w: item %d header exceeds page", ErrCorruptedNode, idx)
		}
		keyLen := binary.LittleEndian.Uint16(data[pos:])
		pos += UInt16Size
		valueLen := binary.LittleEndian.Uint16(data[pos:])
		pos += UInt16Size

		itemBytes := int(keyLen) + int(valueLen)
		if itemBytes < 0 || pos > len(data)-itemBytes {
			return fmt.Errorf("%w: item %d data exceeds page", ErrCorruptedNode, idx)
		}
		if keyLen >= MaxKeySize {
			return fmt.Errorf("%w: item %d key is too large", ErrCorruptedNode, idx)
		}
		if valueLen == 0 {
			return fmt.Errorf("%w: item %d has an empty encoded value", ErrCorruptedNode, idx)
		}

		key := data[pos : pos+int(keyLen)]
		pos += int(keyLen)

		value := data[pos : pos+int(valueLen)]
		pos += int(valueLen)
		if idx > 0 && bytes.Compare(previousKey, key) >= 0 {
			return fmt.Errorf("%w: keys are not strictly ordered at item %d", ErrCorruptedNode, idx)
		}
		switch value[0] {
		case ValueSimple:
		case ValueBlob:
			if len(value) != 1+UInt64Size {
				return fmt.Errorf("%w: item %d has malformed blob reference", ErrCorruptedNode, idx)
			}
		default:
			return fmt.Errorf("%w: item %d has unknown value type %d", ErrCorruptedNode, idx, value[0])
		}
		decodedItems[idx] = Item{Key: key, Value: value}
		node.items[idx] = &decodedItems[idx]
		previousKey = key
	}
	return nil
}

// Return number of children
func (node *BNode) numChildren() int {
	return len(node.childNodes)
}

// Return true if there is no childNodes
func (node *BNode) isLeaf() bool {
	return node.numChildren() == 0
}

func (node *BNode) numItems() int {
	return len(node.items)
}

func itemEncodedSize(item *Item) (int, error) {
	if item == nil {
		return 0, fmt.Errorf("%w: nil item", ErrCorruptedNode)
	}
	if len(item.Key) >= MaxKeySize || len(item.Key) > int(^uint16(0)) {
		return 0, ErrKeyTooLarge
	}
	if len(item.Value) == 0 || len(item.Value) > int(^uint16(0)) {
		return 0, fmt.Errorf("%w: invalid encoded value length %d", ErrCorruptedNode, len(item.Value))
	}
	switch item.Value[0] {
	case ValueSimple:
	case ValueBlob:
		if len(item.Value) != 1+UInt64Size {
			return 0, fmt.Errorf("%w: malformed blob reference", ErrCorruptedNode)
		}
	default:
		return 0, ErrUnknownItemType
	}
	return 2*UInt16Size + len(item.Key) + len(item.Value), nil
}

func (node *BNode) encodedSize() (int, error) {
	size, err := node.encodedSizeTrusted()
	if err != nil {
		return 0, err
	}
	if len(node.childNodes) > 1 {
		seenChildren := make(map[uint64]struct{}, len(node.childNodes))
		for _, childPage := range node.childNodes {
			if _, exists := seenChildren[childPage]; exists {
				return 0, fmt.Errorf("%w: duplicate child page %d", ErrCorruptedNode, childPage)
			}
			seenChildren[childPage] = struct{}{}
		}
	}
	return size, nil
}

// encodedSizeTrusted validates the shape and item encoding without building a
// child-page set. Nodes entering a transaction have already had child
// uniqueness checked by Deserialize, and the mutation helpers preserve it.
// Keeping the full duplicate check at serialization boundaries avoids a large
// map allocation on every B-tree insert/delete sizing check.
func (node *BNode) encodedSizeTrusted() (int, error) {
	if node == nil {
		return 0, fmt.Errorf("%w: nil node", ErrCorruptedNode)
	}
	if len(node.items) > int(^uint16(0)) || len(node.childNodes) > int(^uint16(0)) {
		return 0, fmt.Errorf("%w: node has too many entries", ErrCorruptedNode)
	}
	if len(node.childNodes) != 0 && len(node.childNodes) != len(node.items)+1 {
		return 0, fmt.Errorf("%w: node has %d items and %d children", ErrCorruptedNode, len(node.items), len(node.childNodes))
	}
	size := NodeHeaderSize + UInt16Size + len(node.childNodes)*UInt64Size
	var previousKey []byte
	for idx, item := range node.items {
		itemSize, err := itemEncodedSize(item)
		if err != nil {
			return 0, err
		}
		if idx > 0 && bytes.Compare(previousKey, item.Key) >= 0 {
			return 0, fmt.Errorf("%w: keys are not strictly ordered at item %d", ErrCorruptedNode, idx)
		}
		size += itemSize
		previousKey = item.Key
	}
	return size, nil
}

func (node *BNode) size() int {
	size, err := node.encodedSize()
	if err != nil {
		return int(^uint(0) >> 1)
	}
	return size
}

func (node *BNode) findKeyPosition(key []byte) (int, bool) {
	if len(node.items) == 0 {
		return 0, false
	}

	left := 0
	right := len(node.items) - 1

	for left <= right {
		middle := left + (right-left)/2
		res := bytes.Compare(node.items[middle].Key, key)

		if res == 0 {
			return middle, true
		} else if res < 0 { // Key is greater than middle item
			left = middle + 1
		} else { // Key is smaller than middle item
			right = middle - 1
		}
	}

	return left, false
}

func (node *BNode) Find(tx *Tx, key []byte, exact bool) (int, *BNode, []int, bool) {
	pos, foundNode, ancestors, found, _ := node.FindE(tx, key, exact)
	return pos, foundNode, ancestors, found
}

func (node *BNode) FindE(tx *Tx, key []byte, exact bool) (int, *BNode, []int, bool, error) {
	pos, foundNode, ancestors, _, found, err := node.findPathE(tx, key, exact)
	return pos, foundNode, ancestors, found, err
}

func (node *BNode) findPathE(tx *Tx, key []byte, exact bool) (int, *BNode, []int, []*BNode, bool, error) {
	ancestors := make([]int, 1, 8)
	path := make([]*BNode, 0, 8)
	return node.findPathIntoE(tx, key, exact, ancestors, path)
}

func (node *BNode) findPathReuseE(tx *Tx, key []byte, exact bool) (int, *BNode, []int, []*BNode, bool, error) {
	if tx == nil {
		return node.findPathE(tx, key, exact)
	}
	ancestors := append(tx.btreePathIndexes[:0], 0)
	path := tx.btreePathNodes[:0]
	pos, foundNode, ancestors, path, found, err := node.findPathIntoE(tx, key, exact, ancestors, path)
	tx.btreePathIndexes = ancestors
	tx.btreePathNodes = path
	return pos, foundNode, ancestors, path, found, err
}

func (node *BNode) findPathIntoE(tx *Tx, key []byte, exact bool, ancestors []int, path []*BNode) (int, *BNode, []int, []*BNode, bool, error) {
	current := node
	for {
		if current == nil {
			return -1, nil, ancestors, path, false, ErrNodeNotFound
		}
		if len(path) >= 128 {
			return -1, nil, ancestors, path, false, fmt.Errorf("%w: b-tree depth exceeds 128", ErrCorruptedNode)
		}
		if current.PageNum != 0 {
			for _, visited := range path {
				if visited.PageNum == current.PageNum {
					return -1, nil, ancestors, path, false, fmt.Errorf("%w: b-tree cycle at page %d", ErrCorruptedNode, current.PageNum)
				}
			}
		}
		path = append(path, current)

		pos, found := current.findKeyPosition(key)
		if found {
			return pos, current, ancestors, path, true, nil
		}
		if current.isLeaf() {
			if exact {
				return -1, nil, ancestors, path, false, nil
			}
			return pos, current, ancestors, path, true, nil
		}
		if pos < 0 || pos >= len(current.childNodes) {
			return -1, nil, ancestors, path, false, fmt.Errorf("%w: child index %d is out of bounds", ErrCorruptedNode, pos)
		}
		ancestors = append(ancestors, pos)
		next, err := tx.getNode(current.childNodes[pos])
		if err != nil {
			return -1, nil, ancestors, path, false, err
		}
		current = next
	}
}

func (node *BNode) FindExact(tx *Tx, key []byte) (int, *BNode, bool) {
	pos, foundNode, found, _ := node.FindExactE(tx, key)
	return pos, foundNode, found
}

func (node *BNode) FindExactE(tx *Tx, key []byte) (int, *BNode, bool, error) {
	current := node
	visited := make(map[uint64]struct{})
	for {
		if len(visited) >= 128 {
			return -1, nil, false, fmt.Errorf("%w: b-tree depth exceeds 128", ErrCorruptedNode)
		}
		if current == nil {
			return -1, nil, false, ErrNodeNotFound
		}
		if current.PageNum != 0 {
			if _, exists := visited[current.PageNum]; exists {
				return -1, nil, false, fmt.Errorf("%w: b-tree cycle at page %d", ErrCorruptedNode, current.PageNum)
			}
			visited[current.PageNum] = struct{}{}
		}
		pos, found := current.findKeyPosition(key)
		if found {
			return pos, current, true, nil
		}
		if current.isLeaf() {
			return -1, nil, false, nil
		}
		if pos < 0 || pos >= len(current.childNodes) {
			return -1, nil, false, fmt.Errorf("%w: child index %d is out of bounds", ErrCorruptedNode, pos)
		}
		nextNode, err := tx.getNode(current.childNodes[pos])
		if err != nil {
			return -1, nil, false, err
		}
		current = nextNode
	}
}

func (node *BNode) insertItemAt(newItem *Item, insertIndex int) int {
	if insertIndex >= len(node.items) {
		node.items = append(node.items, newItem)
		return insertIndex
	}

	// expand slice
	node.items = append(node.items, nil)
	copy(node.items[insertIndex+1:], node.items[insertIndex:])
	node.items[insertIndex] = newItem

	return insertIndex
}

func (node *BNode) isOverPopulated(maxThreshold float32) (bool, error) {
	size, err := node.encodedSizeTrusted()
	return float32(size) > maxThreshold, err
}

func (node *BNode) isUnderPopulated(minThreshold float32) (bool, error) {
	size, err := node.encodedSizeTrusted()
	return float32(size) < minThreshold, err
}

func (node *BNode) splitChild(tx *Tx, fullNode *BNode, fullNodeIndex int) error {
	if err := tx.ensureWritable(); err != nil {
		return err
	}
	if node == nil || fullNode == nil || fullNodeIndex < 0 || fullNodeIndex >= len(node.childNodes) || node.childNodes[fullNodeIndex] != fullNode.PageNum {
		return fmt.Errorf("%w: invalid split child index %d", ErrCorruptedNode, fullNodeIndex)
	}
	splitIndex, err := chooseSplitIndex(fullNode, tx.db.dal.usablePageSize(), tx.db.dal.minThreshold())
	if err != nil {
		return err
	}

	// this element will go to parent node
	middleItem := fullNode.items[splitIndex]
	var newNode *BNode

	if fullNode.isLeaf() {
		newNode, err = tx.newNode(fullNode.items[splitIndex+1:], []uint64{})
		if err != nil {
			return err
		}
		fullNode.items = fullNode.items[:splitIndex]
	} else {
		newNode, err = tx.newNode(fullNode.items[splitIndex+1:], fullNode.childNodes[splitIndex+1:])
		if err != nil {
			return err
		}
		fullNode.items = fullNode.items[:splitIndex]
		fullNode.childNodes = fullNode.childNodes[:splitIndex+1]
	}
	tx.setNode(newNode)

	// insert middle item to parent node
	node.insertItemAt(middleItem, fullNodeIndex)

	if len(node.childNodes) == fullNodeIndex+1 { // If middle of list, then move items forward
		node.childNodes = append(node.childNodes, newNode.PageNum)
	} else { // otherwise move items right
		node.childNodes = append(node.childNodes[:fullNodeIndex+1], node.childNodes[fullNodeIndex:]...)
		node.childNodes[fullNodeIndex+1] = newNode.PageNum
	}

	tx.writeNodes(node, fullNode)
	return nil
}

// removeItemAtLeaf removes an item at the given index from a leaf node.
// It shifts the remaining items to maintain order.
func (node *BNode) removeItemAtLeaf(index int) {
	copy(node.items[index:], node.items[index+1:])
	node.items = node.items[:len(node.items)-1]
}

// removeItemFromInternal removes an item from an internal (non-leaf) node.
// The item is replaced by its in-order predecessor (largest value in the left subtree).
// It also removes the predecessor from its original position and updates the affected nodes.
func (node *BNode) removeItemFromInternal(tx *Tx, index int) ([]int, []*BNode, error) {
	if node == nil || index < 0 || index >= len(node.items) || len(node.childNodes) != len(node.items)+1 {
		return nil, nil, fmt.Errorf("%w: invalid internal removal index %d", ErrCorruptedNode, index)
	}
	affectedIndexes := []int{index}

	predecessorNode, err := tx.getNode(node.childNodes[index])
	if err != nil {
		return nil, nil, err
	}
	affectedNodes := []*BNode{predecessorNode}

	visited := map[uint64]struct{}{node.PageNum: {}}
	for !predecessorNode.isLeaf() {
		if len(visited) >= 128 {
			return nil, nil, fmt.Errorf("%w: predecessor path exceeds depth 128", ErrCorruptedNode)
		}
		if _, exists := visited[predecessorNode.PageNum]; exists {
			return nil, nil, fmt.Errorf("%w: b-tree cycle at page %d", ErrCorruptedNode, predecessorNode.PageNum)
		}
		visited[predecessorNode.PageNum] = struct{}{}
		if len(predecessorNode.childNodes) != len(predecessorNode.items)+1 || len(predecessorNode.childNodes) == 0 {
			return nil, nil, fmt.Errorf("%w: malformed predecessor node %d", ErrCorruptedNode, predecessorNode.PageNum)
		}
		traversingIndex := len(predecessorNode.childNodes) - 1
		predecessorNode, err = tx.getNode(predecessorNode.childNodes[traversingIndex])
		if err != nil {
			return nil, nil, err
		}
		affectedIndexes = append(affectedIndexes, traversingIndex)
		affectedNodes = append(affectedNodes, predecessorNode)
	}
	if len(predecessorNode.items) == 0 {
		return nil, nil, fmt.Errorf("%w: predecessor leaf %d is empty", ErrCorruptedNode, predecessorNode.PageNum)
	}

	node.items[index] = predecessorNode.items[len(predecessorNode.items)-1]
	predecessorNode.items = predecessorNode.items[:len(predecessorNode.items)-1]
	tx.writeNodes(node, predecessorNode)

	return affectedIndexes, affectedNodes, nil
}

func isLastItem(index int, parentNode *BNode) bool {
	return index >= len(parentNode.items)-1
}

func isFirstItem(index int) bool {
	return index == 0
}

// rotateRight shifts an item from the right end of leftNode to parentNode,
// and moves the parentNode's item down to the leftmost position in rightNode.
//
// leftNode: The left sibling (giving an item)
// parentNode: The parent node (exchanging an item)
// rightNode: The right sibling (receiving an item)
// rightNodeIndex: The index of rightNode in parentNode.childNodes
func rotateRight(leftNode, parentNode, rightNode *BNode, rightNodeIndex int) {
	// Move the last item of leftNode to parentNode
	movedItem := leftNode.items[len(leftNode.items)-1]
	leftNode.items = leftNode.items[:len(leftNode.items)-1]

	// Swap with the corresponding parentNode item
	parentItemIndex := rightNodeIndex - 1
	if isFirstItem(rightNodeIndex) {
		parentItemIndex = 0
	}
	parentSwapItem := parentNode.items[parentItemIndex]
	parentNode.items[parentItemIndex] = movedItem

	// Move parentSwapItem to the start of rightNode
	rightNode.items = append(rightNode.items, nil) // Extend slice by 1
	copy(rightNode.items[1:], rightNode.items[:])  // Shift items right
	rightNode.items[0] = parentSwapItem

	// If nodes have children, move the last child from leftNode to rightNode
	if !leftNode.isLeaf() {
		childToMove := leftNode.childNodes[len(leftNode.childNodes)-1]
		leftNode.childNodes = leftNode.childNodes[:len(leftNode.childNodes)-1]

		// Put child at the beginning of rightNode's children
		rightNode.childNodes = append(rightNode.childNodes, 0)
		copy(rightNode.childNodes[1:], rightNode.childNodes[:])
		rightNode.childNodes[0] = childToMove
	}
}

func rotateLeft(leftNode, parentNode, rightNode *BNode, rightNodeIndex int) {
	// Move the first item of rightNode to parentNode
	movedItem := rightNode.items[0]
	rightNode.items = rightNode.items[1:]

	// Swap with the corresponding parentNode item
	parentItemIndex := rightNodeIndex
	if isLastItem(rightNodeIndex, parentNode) {
		parentItemIndex = len(parentNode.items) - 1
	}
	parentSwapItem := parentNode.items[parentItemIndex]
	parentNode.items[parentItemIndex] = movedItem

	// Move parentSwapItem to the end of leftNode
	leftNode.items = append(leftNode.items, parentSwapItem)

	// If nodes have children, move the first child from rightNode to leftNode
	if !rightNode.isLeaf() {
		childToMove := rightNode.childNodes[0]
		rightNode.childNodes = rightNode.childNodes[1:]
		leftNode.childNodes = append(leftNode.childNodes, childToMove)
	}
}

func cloneNodeForBalance(node *BNode) *BNode {
	if node == nil {
		return nil
	}
	return &BNode{
		PageNum:    node.PageNum,
		items:      append([]*Item(nil), node.items...),
		childNodes: append([]uint64(nil), node.childNodes...),
	}
}

func canRotateRight(leftNode, parentNode, rightNode *BNode, rightNodeIndex int, minThreshold float32, pageSize int) bool {
	if leftNode == nil || parentNode == nil || rightNode == nil || len(leftNode.items) == 0 || rightNodeIndex <= 0 ||
		rightNodeIndex >= len(parentNode.childNodes) || rightNodeIndex-1 >= len(parentNode.items) || leftNode.isLeaf() != rightNode.isLeaf() {
		return false
	}
	leftCopy := cloneNodeForBalance(leftNode)
	parentCopy := cloneNodeForBalance(parentNode)
	rightCopy := cloneNodeForBalance(rightNode)
	rotateRight(leftCopy, parentCopy, rightCopy, rightNodeIndex)
	leftSize, leftErr := leftCopy.encodedSizeTrusted()
	parentSize, parentErr := parentCopy.encodedSizeTrusted()
	rightSize, rightErr := rightCopy.encodedSizeTrusted()
	donorHealthy := float32(leftSize) >= minThreshold
	if len(rightNode.items) == 0 {
		// Never leave an empty non-root child merely because byte-based fill
		// targets cannot be met with large variable-sized items.
		donorHealthy = len(leftCopy.items) > 0
	}
	return leftErr == nil && parentErr == nil && rightErr == nil && donorHealthy && parentSize <= pageSize && rightSize <= pageSize
}

func canRotateLeft(leftNode, parentNode, rightNode *BNode, rightNodeIndex int, minThreshold float32, pageSize int) bool {
	if leftNode == nil || parentNode == nil || rightNode == nil || len(rightNode.items) == 0 || rightNodeIndex < 0 ||
		rightNodeIndex >= len(parentNode.items) || rightNodeIndex+1 >= len(parentNode.childNodes) || leftNode.isLeaf() != rightNode.isLeaf() {
		return false
	}
	leftCopy := cloneNodeForBalance(leftNode)
	parentCopy := cloneNodeForBalance(parentNode)
	rightCopy := cloneNodeForBalance(rightNode)
	rotateLeft(leftCopy, parentCopy, rightCopy, rightNodeIndex)
	leftSize, leftErr := leftCopy.encodedSizeTrusted()
	parentSize, parentErr := parentCopy.encodedSizeTrusted()
	rightSize, rightErr := rightCopy.encodedSizeTrusted()
	donorHealthy := float32(rightSize) >= minThreshold
	if len(leftNode.items) == 0 {
		donorHealthy = len(rightCopy.items) > 0
	}
	return leftErr == nil && parentErr == nil && rightErr == nil && donorHealthy && parentSize <= pageSize && leftSize <= pageSize
}

func (node *BNode) merge(tx *Tx, rightNode *BNode, rightNodeIndex int) error {
	if node == nil || rightNode == nil || rightNodeIndex <= 0 || rightNodeIndex >= len(node.childNodes) || rightNodeIndex > len(node.items) {
		return fmt.Errorf("%w: invalid merge index %d", ErrCorruptedNode, rightNodeIndex)
	}
	// Get the left sibling of rightNode
	leftNode, err := tx.getNode(node.childNodes[rightNodeIndex-1])
	if err != nil {
		return err
	}

	separatorItem := node.items[rightNodeIndex-1]
	if leftNode.isLeaf() != rightNode.isLeaf() {
		return fmt.Errorf("%w: cannot merge leaf and internal nodes", ErrCorruptedNode)
	}
	mergedCandidate := &BNode{items: make([]*Item, 0, len(leftNode.items)+1+len(rightNode.items))}
	mergedCandidate.items = append(mergedCandidate.items, leftNode.items...)
	mergedCandidate.items = append(mergedCandidate.items, separatorItem)
	mergedCandidate.items = append(mergedCandidate.items, rightNode.items...)
	if !leftNode.isLeaf() {
		mergedCandidate.childNodes = append(mergedCandidate.childNodes, leftNode.childNodes...)
		mergedCandidate.childNodes = append(mergedCandidate.childNodes, rightNode.childNodes...)
	}
	combinedSize, sizeErr := mergedCandidate.encodedSizeTrusted()
	if sizeErr != nil {
		return sizeErr
	}
	pageSize := tx.db.dal.usablePageSize()
	if combinedSize > pageSize {
		return fmt.Errorf("%w: merged node needs %d bytes, page has %d", ErrNotEnoughSpace, combinedSize, pageSize)
	}

	// Take the item from the parent, remove it and add it to the unbalanced node
	parentItem := node.items[rightNodeIndex-1]
	leftNode.items = append(leftNode.items, parentItem)

	// Remove the separator item from parent
	copy(node.items[rightNodeIndex-1:], node.items[rightNodeIndex:])
	node.items = node.items[:len(node.items)-1]

	// Merge rightNode's items into leftNode
	leftNode.items = append(leftNode.items, rightNode.items...)

	// Remove rightNode reference from parent
	copy(node.childNodes[rightNodeIndex:], node.childNodes[rightNodeIndex+1:])
	node.childNodes = node.childNodes[:len(node.childNodes)-1]

	if !leftNode.isLeaf() {
		leftNode.childNodes = append(leftNode.childNodes, rightNode.childNodes...)
	}

	tx.writeNodes(leftNode, node)
	tx.deletePage(rightNode.PageNum)
	return nil
}

// rebalanceRemove balances a B-tree node after a deletion operation.
// It attempts to rebalance by:
// 1. Rotating right if the left sibling has an extra element.
// 2. Rotating left if the right sibling has an extra element.
// 3. Merging with a sibling if rotation is not possible.
func (node *BNode) rebalanceRemove(tx *Tx, unbalancedNode *BNode, nodeIndexInParent int) error {
	parentNode := node
	if parentNode == nil || unbalancedNode == nil || len(parentNode.childNodes) != len(parentNode.items)+1 ||
		nodeIndexInParent < 0 || nodeIndexInParent >= len(parentNode.childNodes) || parentNode.childNodes[nodeIndexInParent] != unbalancedNode.PageNum {
		return fmt.Errorf("%w: invalid rebalance child index %d", ErrCorruptedNode, nodeIndexInParent)
	}
	if len(parentNode.childNodes) == 1 {
		return nil
	}
	minThreshold := tx.db.dal.minThreshold()
	pageSize := tx.db.dal.usablePageSize()

	// Right rotate
	if nodeIndexInParent != 0 {
		leftNode, err := tx.getNode(parentNode.childNodes[nodeIndexInParent-1])
		if err != nil {
			return err
		}
		if canRotateRight(leftNode, parentNode, unbalancedNode, nodeIndexInParent, minThreshold, pageSize) {
			rotateRight(leftNode, parentNode, unbalancedNode, nodeIndexInParent)
			tx.writeNodes(leftNode, parentNode, unbalancedNode)
			return nil
		}
	}

	// Left Balance
	if nodeIndexInParent != len(parentNode.childNodes)-1 {
		rightNode, err := tx.getNode(parentNode.childNodes[nodeIndexInParent+1])
		if err != nil {
			return err
		}
		if canRotateLeft(unbalancedNode, parentNode, rightNode, nodeIndexInParent, minThreshold, pageSize) {
			rotateLeft(unbalancedNode, parentNode, rightNode, nodeIndexInParent)
			tx.writeNodes(unbalancedNode, parentNode, rightNode)
			return nil
		}
	}
	// The merge function merges a given node with its node to the right. So by default, we merge an unbalanced node
	// with its right sibling. In the case where the unbalanced node is the leftmost, we have to replace the merge
	// parameters, so the unbalanced node right sibling, will be merged into the unbalanced node.
	if nodeIndexInParent == 0 {
		rightNode, err := tx.getNode(node.childNodes[nodeIndexInParent+1])
		if err != nil {
			return err
		}
		mergeErr := parentNode.merge(tx, rightNode, nodeIndexInParent+1)
		if errors.Is(mergeErr, ErrNotEnoughSpace) {
			return nil
		}
		return mergeErr
	}

	mergeErr := parentNode.merge(tx, unbalancedNode, nodeIndexInParent)
	if errors.Is(mergeErr, ErrNotEnoughSpace) {
		return nil
	}
	return mergeErr
}

func traverse(tx *Tx, node *BNode, pages *[]uint64) {
	if node == nil {
		return
	}
	if !node.isLeaf() {
		for _, nodePageNumber := range node.childNodes {
			childNode, err := tx.getNode(nodePageNumber)
			if err != nil {
				return
			}
			*pages = append(*pages, childNode.PageNum)
			traverse(tx, childNode, pages)
		}
	}
}

func chooseSplitIndex(node *BNode, pageSize int, minThreshold float32) (int, error) {
	if node == nil || len(node.items) < 3 {
		return -1, fmt.Errorf("%w: cannot split node with %d items", ErrCorruptedNode, len(node.items))
	}
	if _, err := node.encodedSize(); err != nil {
		return -1, err
	}

	totalItemBytes := 0
	for _, item := range node.items {
		itemSize, sizeErr := itemEncodedSize(item)
		if sizeErr != nil {
			return -1, sizeErr
		}
		totalItemBytes += itemSize
	}
	leftItemBytes, _ := itemEncodedSize(node.items[0])
	baseSize := NodeHeaderSize + UInt16Size

	bestIndex := -1
	bestScore := int(^uint(0) >> 1)
	bestMeetsMinimum := false
	for splitIndex := 1; splitIndex < len(node.items)-1; splitIndex++ {
		middleItemBytes, _ := itemEncodedSize(node.items[splitIndex])
		rightItemBytes := totalItemBytes - leftItemBytes - middleItemBytes
		leftSize := baseSize + leftItemBytes
		rightSize := baseSize + rightItemBytes
		if !node.isLeaf() {
			leftSize += (splitIndex + 1) * UInt64Size
			rightSize += (len(node.items) - splitIndex) * UInt64Size
		}
		if leftSize > pageSize || rightSize > pageSize {
			leftItemBytes += middleItemBytes
			continue
		}
		meetsMinimum := float32(leftSize) >= minThreshold && float32(rightSize) >= minThreshold
		score := leftSize - rightSize
		if score < 0 {
			score = -score
		}
		if bestIndex == -1 || (meetsMinimum && !bestMeetsMinimum) || meetsMinimum == bestMeetsMinimum && score < bestScore {
			bestIndex = splitIndex
			bestScore = score
			bestMeetsMinimum = meetsMinimum
		}
		leftItemBytes += middleItemBytes
	}
	if bestIndex == -1 {
		return -1, fmt.Errorf("%w: no valid split for node with %d items", ErrNotEnoughSpace, len(node.items))
	}
	return bestIndex, nil
}
