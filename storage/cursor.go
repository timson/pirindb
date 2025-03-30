package storage

type cursorFrame struct {
	childIndex int
	pageNum    uint64
	children   []uint64
	itemIndex  int
}

type Cursor struct {
	tx         *Tx
	bucket     *Bucket
	node       *BNode
	itemIndex  int
	childIndex int
	stack      []cursorFrame
}

// stackPop removes and returns the last item from the stack.
// If the stack is empty, it returns false.
func stackPop(cursorStack *[]cursorFrame) (cursorFrame, bool) {
	if len(*cursorStack) == 0 {
		return cursorFrame{}, false
	}
	lastIndex := len(*cursorStack) - 1
	frame := (*cursorStack)[lastIndex]
	*cursorStack = (*cursorStack)[:lastIndex]
	return frame, true
}

// stackPush adds a new frame to the cursor stack.
func stackPush(cursorStack *[]cursorFrame, item cursorFrame) {
	*cursorStack = append(*cursorStack, item)
}

// traverseToLastItem recursively finds the last (rightmost) item in the B-tree.
// It follows the last child node until it reaches a leaf and returns the last item.
func traverseToLastItem(tx *Tx, node *BNode, stack *[]cursorFrame) (*Item, *BNode, error) {
	if node == nil {
		return nil, nil, ErrNodeNotFound
	}

	// If it's not a leaf, traverse to the last child
	if !node.isLeaf() {
		lastChildPage := node.childNodes[len(node.childNodes)-1]

		childNode, err := tx.getNode(lastChildPage)
		if err != nil {
			return nil, nil, err
		}
		stackPush(stack, cursorFrame{pageNum: node.PageNum, children: node.childNodes, childIndex: len(node.childNodes) - 1,
			itemIndex: len(node.items)})
		return traverseToLastItem(tx, childNode, stack)
	}
	if len(node.items) == 0 {
		return nil, nil, ErrNodeNotFound
	}
	return node.items[len(node.items)-1], node, nil
}

// traverseToFirstItem recursively finds the first (leftmost) item in the B-tree.
// It follows the first child node until it reaches a leaf and returns the first item.
// It also tracks the traversal path using a stack.
func traverseToFirstItem(tx *Tx, node *BNode, stack *[]cursorFrame) (*Item, *BNode, error) {
	if node == nil {
		return nil, nil, ErrNodeNotFound
	}
	if !node.isLeaf() {
		firstNodeNumber := node.childNodes[0]
		childNode, err := tx.getNode(firstNodeNumber)
		if err != nil {
			return nil, nil, err
		}
		stackPush(stack, cursorFrame{pageNum: node.PageNum, children: node.childNodes})
		return traverseToFirstItem(tx, childNode, stack)
	}
	if len(node.items) == 0 {
		return nil, nil, ErrNodeNotFound
	}
	return node.items[0], node, nil
}

// traverseToItem recursively finds the given item in the B-tree
// It also tracks the traversal path using a stack.
func traverseToItem(tx *Tx, node *BNode, key []byte, exact bool, stack *[]cursorFrame) (int, *BNode, bool) {
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
	*stack = append(*stack, cursorFrame{pageNum: node.PageNum, children: node.childNodes, childIndex: pos, itemIndex: pos})
	child, err := tx.getNode(node.childNodes[pos])
	if err != nil {
		return -1, nil, false
	}
	return traverseToItem(tx, child, key, exact, stack)
}

func (cursor *Cursor) First() (key []byte, value []byte) {
	root, _ := cursor.tx.getNode(cursor.bucket.root)
	var pages []uint64
	traverse(cursor.tx, root, &pages)
	item, node, err := traverseToFirstItem(cursor.tx, root, &cursor.stack)
	if err != nil {
		return nil, nil
	}
	cursor.node = node
	v, _ := item.getValue(cursor.tx)
	return item.Key, v
}

func (cursor *Cursor) Last() (key []byte, value []byte) {
	root, _ := cursor.tx.getNode(cursor.bucket.root)
	item, node, err := traverseToLastItem(cursor.tx, root, &cursor.stack)
	if err != nil {
		return nil, nil
	}
	cursor.node = node
	cursor.itemIndex = len(cursor.node.items) - 1
	v, _ := item.getValue(cursor.tx)
	return item.Key, v
}

func (cursor *Cursor) Seek(key []byte) ([]byte, []byte) {
	root, _ := cursor.tx.getNode(cursor.bucket.root)
	pos, foundNode, isFound := traverseToItem(cursor.tx, root, key, false, &cursor.stack)
	if !isFound {
		return nil, nil
	}
	cursor.node = foundNode
	cursor.itemIndex = pos

	value, _ := foundNode.items[pos].getValue(cursor.tx)
	return foundNode.items[pos].Key, value
}

func (cursor *Cursor) Next() ([]byte, []byte) {
	// If we are in a leaf node, iterate over items
	var err error
	if cursor.node.isLeaf() {
		if cursor.itemIndex < len(cursor.node.items)-1 {
			cursor.itemIndex++
			value, _ := cursor.node.items[cursor.itemIndex].getValue(cursor.tx)
			return cursor.node.items[cursor.itemIndex].Key, value
		}
		for {
			parent, ok := stackPop(&cursor.stack)
			if !ok {
				return nil, nil
			}
			if parent.childIndex < len(parent.children)-1 {
				cursor.node, err = cursor.tx.getNode(parent.pageNum)
				if err != nil {
					return nil, nil
				}
				item := cursor.node.items[parent.itemIndex]
				cursor.childIndex = parent.childIndex
				cursor.itemIndex = parent.itemIndex + 1
				value, _ := item.getValue(cursor.tx)
				return item.Key, value
			}
		}
	}

	// If we are in an internal node, move down to the next child
	cursor.childIndex++
	if cursor.childIndex >= len(cursor.node.childNodes) {
		return nil, nil // Defensive check: prevent out-of-bounds access
	}
	childPage := cursor.node.childNodes[cursor.childIndex]
	childNode, errGetNode := cursor.tx.getNode(childPage)
	if errGetNode != nil {
		return nil, nil
	}
	// Push the current cursor onto the stack before descending
	stackPush(&cursor.stack, cursorFrame{
		pageNum:    cursor.node.PageNum,
		children:   cursor.node.childNodes,
		childIndex: cursor.childIndex,
		itemIndex:  cursor.itemIndex,
	})
	// Traverse down to the first item of the new subtree
	item, node, _ := traverseToFirstItem(cursor.tx, childNode, &cursor.stack)
	cursor.node = node
	cursor.itemIndex = 0
	value, _ := item.getValue(cursor.tx)
	return item.Key, value
}

func (cursor *Cursor) Prev() ([]byte, []byte) {
	var err error

	// If we are in a leaf node, iterate backward over items
	if cursor.node.isLeaf() {
		if cursor.itemIndex > 0 {
			cursor.itemIndex--
			return cursor.node.items[cursor.itemIndex].Key, cursor.node.items[cursor.itemIndex].Value
		}

		// Leaf node is finished, move up the stack
		for {
			parent, ok := stackPop(&cursor.stack)
			if !ok {
				return nil, nil // No more elements in the tree
			}

			// If the parent has a previous child to explore, break and process it
			if parent.childIndex > 0 {
				cursor.node, err = cursor.tx.getNode(parent.pageNum)
				if err != nil {
					return nil, nil
				}
				cursor.childIndex = parent.childIndex
				cursor.itemIndex = parent.itemIndex - 1
				return cursor.node.items[cursor.itemIndex].Key, cursor.node.items[cursor.itemIndex].Value
			}
		}
	}

	// If we are in an internal node, move down to the last child of the left subtree
	cursor.childIndex--

	childPage := cursor.node.childNodes[cursor.childIndex]
	childNode, errGetNode := cursor.tx.getNode(childPage)
	if errGetNode != nil {
		return nil, nil
	}

	// Push the current cursor onto the stack before descending
	stackPush(&cursor.stack, cursorFrame{
		pageNum:    cursor.node.PageNum,
		children:   cursor.node.childNodes,
		childIndex: cursor.childIndex,
		itemIndex:  cursor.itemIndex,
	})

	// Traverse down to the last item of the new subtree
	item, node, _ := traverseToLastItem(cursor.tx, childNode, &cursor.stack)
	cursor.node = node
	cursor.itemIndex = len(cursor.node.items) - 1
	return item.Key, item.Value
}
