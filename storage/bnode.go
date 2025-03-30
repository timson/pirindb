package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
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
		item.Value = append(item.Value, 0)
		copy(item.Value[1:], item.Value)
		item.Value[0] = ValueSimple
	}

	return nil
}

func (item *Item) getValue(tx *Tx) ([]byte, error) {
	if item.Value[0] == ValueSimple {
		return item.Value[1:], nil
	} else if item.Value[0] == ValueBlob {
		pageNum := binary.LittleEndian.Uint64(item.Value[1:])
		blob, err := GetBlob(tx, pageNum)
		if err != nil {
			return nil, err
		}
		return blob.data, nil
	}
	return nil, ErrUnknownItemType
}

func (item *Item) deleteValue(tx *Tx) (int, bool, error) {
	var err error
	dataLen := len(item.Value)
	blob := false
	if item.Value[0] == ValueBlob {
		blob = true
		pageNum := binary.LittleEndian.Uint64(item.Value[1:])
		dataLen, err = DeleteBlob(tx, pageNum)
		if err != nil {
			return 0, false, err
		}
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
	var bitSetVar uint8
	if node.isLeaf() {
		bitSetVar = 1
	}

	// clear node.Data
	copy(data, make([]byte, len(data)))

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
		if kvPos+len(item.Key)+len(item.Value) >= len(data) {
			return ErrNotEnoughSpace
		}
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

func (node *BNode) Deserialize(data []byte) {
	isLeaf := data[NodeTypeOffset]
	numItems := int(binary.LittleEndian.Uint16(data[NodeNumItemsOffset:]))
	pos := NodeHeaderSize
	numbChildren := int(binary.LittleEndian.Uint16(data[pos:]))
	pos += UInt16Size

	if isLeaf == 0 {
		for idx := 0; idx < numbChildren; idx++ {
			childNode := binary.LittleEndian.Uint64(data[pos:])
			pos += UInt64Size
			node.childNodes = append(node.childNodes, childNode)
		}
	}
	for idx := 0; idx < numItems; idx++ {
		keyLen := binary.LittleEndian.Uint16(data[pos:])
		pos += UInt16Size
		valueLen := binary.LittleEndian.Uint16(data[pos:])
		pos += UInt16Size

		// Allocate new slices for Key and value to ensure they are copies
		key := make([]byte, keyLen)
		copy(key, data[pos:pos+int(keyLen)])
		pos += int(keyLen)

		value := make([]byte, valueLen)
		copy(value, data[pos:pos+int(valueLen)])
		pos += int(valueLen)
		node.items = append(node.items, &Item{Key: key, Value: value})
	}
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

func (node *BNode) elemSize(item *Item) int {
	// Len of key + key, len of value + value + child node
	return UInt16Size + len(item.Key) + UInt16Size + len(item.Value) + UInt64Size
}

func (node *BNode) size() int {
	size := NodeHeaderSize
	for _, item := range node.items {
		size += node.elemSize(item)
	}
	return size
}

func (node *BNode) findKeyPosition(key []byte) (int, bool) {
	if node.items == nil || len(node.items) == 0 {
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
	ancestorsIndexes := []int{0}
	pos, foundNode, isFound := traverseBTree(tx, node, key, exact, &ancestorsIndexes)
	return pos, foundNode, ancestorsIndexes, isFound
}

func traverseBTree(tx *Tx, node *BNode, key []byte, exact bool, ancestorsIndexes *[]int) (int, *BNode, bool) {
	pos, isFound := node.findKeyPosition(key)
	if isFound {
		return pos, node, isFound
	}
	if node.isLeaf() {
		if exact {
			return -1, nil, false
		}
		return pos, node, true
	}
	*ancestorsIndexes = append(*ancestorsIndexes, pos)
	child, err := tx.getNode(node.childNodes[pos])
	if err != nil {
		return -1, nil, false
	}
	return traverseBTree(tx, child, key, exact, ancestorsIndexes)
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

func (node *BNode) isOverPopulated(maxThreshold float32) bool {
	return float32(node.size()) > maxThreshold
}

func (node *BNode) isUnderPopulated(minThreshold float32) bool {
	return float32(node.size()) < minThreshold
}

func (node *BNode) splitChild(tx *Tx, fullNode *BNode, fullNodeIndex int) {
	// determine splitChild index
	splitIndex := getSplitIndex(fullNode, tx.db.dal.minThreshold())

	// this element will go to parent node
	middleItem := fullNode.items[splitIndex]
	var newNode *BNode

	if fullNode.isLeaf() {
		newNode = tx.newNode(fullNode.items[splitIndex+1:], []uint64{})
		tx.setNode(newNode)
		fullNode.items = fullNode.items[:splitIndex]
	} else {
		newNode = tx.newNode(fullNode.items[splitIndex+1:], fullNode.childNodes[splitIndex+1:])
		tx.setNode(newNode)
		fullNode.items = fullNode.items[:splitIndex]
		fullNode.childNodes = fullNode.childNodes[:splitIndex+1]
	}

	// insert middle item to parent node
	node.insertItemAt(middleItem, fullNodeIndex)

	if len(node.childNodes) == fullNodeIndex+1 { // If middle of list, then move items forward
		node.childNodes = append(node.childNodes, newNode.PageNum)
	} else { // otherwise move items right
		node.childNodes = append(node.childNodes[:fullNodeIndex+1], node.childNodes[fullNodeIndex:]...)
		node.childNodes[fullNodeIndex+1] = newNode.PageNum
	}

	tx.writeNodes(node, fullNode)
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
func (node *BNode) removeItemFromInternal(tx *Tx, index int) ([]int, error) {
	affectedNodes := make([]int, 0)
	affectedNodes = append(affectedNodes, index)

	predecessorNode, err := tx.getNode(node.childNodes[index])
	if err != nil {
		return nil, err
	}

	for !predecessorNode.isLeaf() {
		traversingIndex := len(predecessorNode.childNodes) - 1
		predecessorNode, err = tx.getNode(predecessorNode.childNodes[traversingIndex])
		if err != nil {
			return nil, err
		}
		affectedNodes = append(affectedNodes, traversingIndex)
	}

	node.items[index] = predecessorNode.items[len(predecessorNode.items)-1]
	predecessorNode.items = predecessorNode.items[:len(predecessorNode.items)-1]
	tx.writeNodes(node, predecessorNode)

	return affectedNodes, nil
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

func (node *BNode) merge(tx *Tx, rightNode *BNode, rightNodeIndex int) error {
	// Get the left sibling of rightNode
	leftNode, err := tx.getNode(node.childNodes[rightNodeIndex-1])
	if err != nil {
		return err
	}

	// Calculate the potential size after merging
	separatorItem := node.items[rightNodeIndex-1]
	combinedSize := leftNode.size() + rightNode.size() + node.elemSize(separatorItem)

	// Check for overflow
	if combinedSize+NodeHeaderSize > BTreePageSize {
		return fmt.Errorf("merge overflow error: combined size %d exceeds max page size %d", combinedSize, BTreePageSize)
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

// hasExtraElement checks if a node has more elements than the minimum allowed threshold.
func (node *BNode) hasExtraElement(minThreshold float32) bool {
	splitIndex := getSplitIndex(node, minThreshold)
	if splitIndex == -1 {
		return false
	}
	return true
}

// rebalanceRemove balances a B-tree node after a deletion operation.
// It attempts to rebalance by:
// 1. Rotating right if the left sibling has an extra element.
// 2. Rotating left if the right sibling has an extra element.
// 3. Merging with a sibling if rotation is not possible.
func (node *BNode) rebalanceRemove(tx *Tx, unbalancedNode *BNode, nodeIndexInParent int) error {
	parentNode := node
	minThreshold := tx.db.dal.minThreshold()

	// Right rotate
	if nodeIndexInParent != 0 {
		leftNode, err := tx.getNode(parentNode.childNodes[nodeIndexInParent-1])
		if err != nil {
			return err
		}
		if leftNode.hasExtraElement(minThreshold) {
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
		if rightNode.hasExtraElement(minThreshold) {
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
		return parentNode.merge(tx, rightNode, nodeIndexInParent+1)
	}

	return parentNode.merge(tx, unbalancedNode, nodeIndexInParent)
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
	return
}

func getSplitIndex(node *BNode, minThreshold float32) int {
	size := 0
	size += NodeHeaderSize
	for idx, item := range node.items {
		size += node.elemSize(item)
		if size > int(minThreshold) && idx < len(node.items) {
			return idx + 1
		}
	}
	return -1
}
