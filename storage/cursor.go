package storage

import "fmt"

type cursorFrame struct {
	pageNum    uint64
	childIndex int
}

type Cursor struct {
	tx        *Tx
	bucket    *Bucket
	node      *BNode
	itemIndex int
	stack     []cursorFrame
	err       error
}

func (cursor *Cursor) reset() {
	if cursor == nil {
		return
	}
	cursor.node = nil
	cursor.itemIndex = 0
	cursor.stack = cursor.stack[:0]
	cursor.err = nil
}

func (cursor *Cursor) setErr(err error) {
	if err != nil && cursor.err == nil {
		cursor.err = err
		if cursor.tx != nil {
			cursor.tx.recordError(err)
		}
	}
}

func (cursor *Cursor) Err() error {
	if cursor == nil {
		return ErrTxClosed
	}
	return cursor.err
}

func (cursor *Cursor) ensureOpen() bool {
	if cursor == nil || cursor.tx == nil || cursor.bucket == nil {
		if cursor != nil {
			cursor.setErr(ErrTxClosed)
		}
		return false
	}
	if err := cursor.tx.ensureOpen(); err != nil {
		cursor.setErr(err)
		return false
	}
	return true
}

func (cursor *Cursor) getNode(pageNum uint64) (*BNode, bool) {
	node, err := cursor.tx.getNode(pageNum)
	if err != nil {
		cursor.setErr(err)
		return nil, false
	}
	return node, true
}

func (cursor *Cursor) descendFirst(node *BNode) bool {
	visited := make(map[uint64]struct{})
	for node != nil && !node.isLeaf() {
		if len(visited) >= 128 {
			cursor.setErr(fmt.Errorf("%w: cursor depth exceeds 128", ErrCorruptedNode))
			return false
		}
		if len(node.childNodes) != len(node.items)+1 || len(node.childNodes) == 0 {
			cursor.setErr(fmt.Errorf("%w: invalid child count at page %d", ErrCorruptedNode, node.PageNum))
			return false
		}
		if _, exists := visited[node.PageNum]; exists {
			cursor.setErr(fmt.Errorf("%w: cursor cycle at page %d", ErrCorruptedNode, node.PageNum))
			return false
		}
		visited[node.PageNum] = struct{}{}
		cursor.stack = append(cursor.stack, cursorFrame{pageNum: node.PageNum, childIndex: 0})
		var ok bool
		node, ok = cursor.getNode(node.childNodes[0])
		if !ok {
			return false
		}
	}
	if node == nil {
		return false
	}
	if len(node.items) == 0 {
		return cursor.ascendNext()
	}
	cursor.node = node
	cursor.itemIndex = 0
	return true
}

func (cursor *Cursor) descendLast(node *BNode) bool {
	visited := make(map[uint64]struct{})
	for node != nil && !node.isLeaf() {
		if len(visited) >= 128 {
			cursor.setErr(fmt.Errorf("%w: cursor depth exceeds 128", ErrCorruptedNode))
			return false
		}
		if len(node.childNodes) != len(node.items)+1 || len(node.childNodes) == 0 {
			cursor.setErr(fmt.Errorf("%w: invalid child count at page %d", ErrCorruptedNode, node.PageNum))
			return false
		}
		if _, exists := visited[node.PageNum]; exists {
			cursor.setErr(fmt.Errorf("%w: cursor cycle at page %d", ErrCorruptedNode, node.PageNum))
			return false
		}
		visited[node.PageNum] = struct{}{}
		childIndex := len(node.childNodes) - 1
		cursor.stack = append(cursor.stack, cursorFrame{pageNum: node.PageNum, childIndex: childIndex})
		var ok bool
		node, ok = cursor.getNode(node.childNodes[childIndex])
		if !ok {
			return false
		}
	}
	if node == nil {
		return false
	}
	if len(node.items) == 0 {
		return cursor.ascendPrev()
	}
	cursor.node = node
	cursor.itemIndex = len(node.items) - 1
	return true
}

func (cursor *Cursor) currentItem() *Item {
	if cursor.node == nil || cursor.itemIndex < 0 || cursor.itemIndex >= len(cursor.node.items) {
		return nil
	}
	return cursor.node.items[cursor.itemIndex]
}

func cloneItem(item *Item) *Item {
	if item == nil {
		return nil
	}
	return &Item{Key: cloneBytes(item.Key), Value: cloneBytes(item.Value)}
}

func (cursor *Cursor) currentKV() ([]byte, []byte) {
	item := cursor.currentItem()
	if item == nil {
		return nil, nil
	}
	value, err := item.getValue(cursor.tx)
	if err != nil {
		cursor.setErr(err)
		return nil, nil
	}
	return cloneBytes(item.Key), value
}

func (cursor *Cursor) firstPosition() bool {
	cursor.reset()
	if !cursor.ensureOpen() || cursor.bucket.root == 0 {
		return false
	}
	root, ok := cursor.getNode(cursor.bucket.root)
	return ok && cursor.descendFirst(root)
}

func (cursor *Cursor) lastPosition() bool {
	cursor.reset()
	if !cursor.ensureOpen() || cursor.bucket.root == 0 {
		return false
	}
	root, ok := cursor.getNode(cursor.bucket.root)
	return ok && cursor.descendLast(root)
}

func (cursor *Cursor) First() ([]byte, []byte) {
	if !cursor.firstPosition() {
		return nil, nil
	}
	return cursor.currentKV()
}

func (cursor *Cursor) FirstItem() *Item {
	if !cursor.firstPosition() {
		return nil
	}
	return cloneItem(cursor.currentItem())
}

func (cursor *Cursor) Last() ([]byte, []byte) {
	if !cursor.lastPosition() {
		return nil, nil
	}
	return cursor.currentKV()
}

func (cursor *Cursor) seekPosition(key []byte) bool {
	cursor.reset()
	if !cursor.ensureOpen() || cursor.bucket.root == 0 {
		return false
	}
	node, ok := cursor.getNode(cursor.bucket.root)
	if !ok {
		return false
	}
	visited := make(map[uint64]struct{})
	for {
		if len(visited) >= 128 {
			cursor.setErr(fmt.Errorf("%w: cursor depth exceeds 128", ErrCorruptedNode))
			return false
		}
		if _, exists := visited[node.PageNum]; exists {
			cursor.setErr(fmt.Errorf("%w: cursor cycle at page %d", ErrCorruptedNode, node.PageNum))
			return false
		}
		visited[node.PageNum] = struct{}{}
		pos, found := node.findKeyPosition(key)
		if found {
			cursor.node = node
			cursor.itemIndex = pos
			return true
		}
		if node.isLeaf() {
			if pos < len(node.items) {
				cursor.node = node
				cursor.itemIndex = pos
				return true
			}
			return cursor.ascendNext()
		}
		if pos < 0 || pos >= len(node.childNodes) {
			cursor.setErr(fmt.Errorf("%w: child index %d is out of bounds", ErrCorruptedNode, pos))
			return false
		}
		cursor.stack = append(cursor.stack, cursorFrame{pageNum: node.PageNum, childIndex: pos})
		node, ok = cursor.getNode(node.childNodes[pos])
		if !ok {
			return false
		}
	}
}

func (cursor *Cursor) Seek(key []byte) ([]byte, []byte) {
	if !cursor.seekPosition(key) {
		return nil, nil
	}
	return cursor.currentKV()
}

// SeekItem positions the cursor at the first item whose key is greater than or
// equal to key without materializing a blob-backed value.
func (cursor *Cursor) SeekItem(key []byte) *Item {
	if !cursor.seekPosition(key) {
		return nil
	}
	return cloneItem(cursor.currentItem())
}

func (cursor *Cursor) ascendNext() bool {
	for len(cursor.stack) > 0 {
		last := len(cursor.stack) - 1
		frame := cursor.stack[last]
		cursor.stack = cursor.stack[:last]
		parent, ok := cursor.getNode(frame.pageNum)
		if !ok {
			return false
		}
		if parent.isLeaf() || len(parent.childNodes) != len(parent.items)+1 || frame.childIndex < 0 || frame.childIndex >= len(parent.childNodes) {
			cursor.setErr(fmt.Errorf("%w: invalid cursor parent frame at page %d", ErrCorruptedNode, parent.PageNum))
			return false
		}
		if frame.childIndex < len(parent.items) {
			cursor.node = parent
			cursor.itemIndex = frame.childIndex
			return true
		}
	}
	cursor.node = nil
	return false
}

func (cursor *Cursor) ascendPrev() bool {
	for len(cursor.stack) > 0 {
		last := len(cursor.stack) - 1
		frame := cursor.stack[last]
		cursor.stack = cursor.stack[:last]
		parent, ok := cursor.getNode(frame.pageNum)
		if !ok {
			return false
		}
		if parent.isLeaf() || len(parent.childNodes) != len(parent.items)+1 || frame.childIndex < 0 || frame.childIndex >= len(parent.childNodes) {
			cursor.setErr(fmt.Errorf("%w: invalid cursor parent frame at page %d", ErrCorruptedNode, parent.PageNum))
			return false
		}
		if frame.childIndex > 0 {
			cursor.node = parent
			cursor.itemIndex = frame.childIndex - 1
			return true
		}
	}
	cursor.node = nil
	return false
}

func (cursor *Cursor) nextPosition() bool {
	if cursor == nil || cursor.err != nil || cursor.node == nil || !cursor.ensureOpen() {
		return false
	}
	if !cursor.node.isLeaf() {
		childIndex := cursor.itemIndex + 1
		if childIndex >= len(cursor.node.childNodes) {
			cursor.setErr(fmt.Errorf("%w: successor child is out of bounds", ErrCorruptedNode))
			return false
		}
		parent := cursor.node
		cursor.stack = append(cursor.stack, cursorFrame{pageNum: parent.PageNum, childIndex: childIndex})
		child, ok := cursor.getNode(parent.childNodes[childIndex])
		return ok && cursor.descendFirst(child)
	}
	if cursor.itemIndex+1 < len(cursor.node.items) {
		cursor.itemIndex++
		return true
	}
	return cursor.ascendNext()
}

func (cursor *Cursor) Next() ([]byte, []byte) {
	if !cursor.nextPosition() {
		return nil, nil
	}
	return cursor.currentKV()
}

func (cursor *Cursor) NextItem() *Item {
	if !cursor.nextPosition() {
		return nil
	}
	return cloneItem(cursor.currentItem())
}

func (cursor *Cursor) prevPosition() bool {
	if cursor == nil || cursor.err != nil || cursor.node == nil || !cursor.ensureOpen() {
		return false
	}
	if !cursor.node.isLeaf() {
		childIndex := cursor.itemIndex
		if childIndex < 0 || childIndex >= len(cursor.node.childNodes) {
			cursor.setErr(fmt.Errorf("%w: predecessor child is out of bounds", ErrCorruptedNode))
			return false
		}
		parent := cursor.node
		cursor.stack = append(cursor.stack, cursorFrame{pageNum: parent.PageNum, childIndex: childIndex})
		child, ok := cursor.getNode(parent.childNodes[childIndex])
		return ok && cursor.descendLast(child)
	}
	if cursor.itemIndex > 0 {
		cursor.itemIndex--
		return true
	}
	return cursor.ascendPrev()
}

func (cursor *Cursor) Prev() ([]byte, []byte) {
	if !cursor.prevPosition() {
		return nil, nil
	}
	return cursor.currentKV()
}
