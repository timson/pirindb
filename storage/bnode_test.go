package storage

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/require"
	"reflect"
	"strings"
	"testing"
)

func createNode(keys [][]byte, children []uint64, pageNum uint64) *BNode {
	items := make([]*Item, len(keys))
	for i, key := range keys {
		items[i] = &Item{Key: key, Value: []byte(strings.Repeat("x", 1024))}
	}
	return &BNode{items: items, childNodes: children, PageNum: pageNum}
}

func TestInsertItemAt(t *testing.T) {
	tests := []struct {
		name     string
		insertAt int
		newKey   []byte
		expected [][]byte
	}{
		{
			name:     "Put at beginning",
			insertAt: 0,
			newKey:   []byte("A"),
			expected: [][]byte{[]byte("A"), []byte("B"), []byte("D"), []byte("F")},
		},
		{
			name:     "Put in the middle",
			insertAt: 2,
			newKey:   []byte("C"),
			expected: [][]byte{[]byte("B"), []byte("D"), []byte("C"), []byte("F")},
		},
		{
			name:     "Put at end",
			insertAt: 3,
			newKey:   []byte("G"),
			expected: [][]byte{[]byte("B"), []byte("D"), []byte("F"), []byte("G")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testNode := createNode([][]byte{
				[]byte("B"), []byte("D"), []byte("F"),
			}, nil, 0)
			testNode.insertItemAt(&Item{Key: tt.newKey}, tt.insertAt)
			require.True(t, reflect.DeepEqual(testNode.itemsToKeys(), tt.expected))
		})
	}
}

func TestRemoveAtLeaf(t *testing.T) {
	tests := []struct {
		name     string
		removeAt int
		expected [][]byte
	}{
		{
			name:     "Remove at beginning",
			removeAt: 0,
			expected: [][]byte{[]byte("B"), []byte("C")},
		},
		{
			name:     "Put in the middle",
			removeAt: 1,
			expected: [][]byte{[]byte("A"), []byte("C")},
		},
		{
			name:     "Put at end",
			removeAt: 2,
			expected: [][]byte{[]byte("A"), []byte("B")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testNode := createNode([][]byte{
				[]byte("A"), []byte("B"), []byte("C"),
			}, nil, 0)
			testNode.removeItemAtLeaf(tt.removeAt)
			require.True(t, reflect.DeepEqual(testNode.itemsToKeys(), tt.expected))
		})
	}
}

// TestRemoveItemFromInternal tests removing an item from an internal node in a B-tree.
func TestRemoveItemFromInternal(t *testing.T) {
	db, _ := createTestDB(t)

	// 1. Internal node with left and right children
	// Structure:
	//       [M]
	//      /   \
	//  [A,B]   [X,Z]
	//
	tx := db.Begin(false)
	parentNode := createNode([][]byte{
		[]byte("M")}, []uint64{1, 2}, 0,
	)
	leftChild := createNode([][]byte{
		[]byte("A"), []byte("B")}, nil, 1)

	rightChild := createNode([][]byte{
		[]byte("X"), []byte("Z")}, nil, 2)

	// Link the parent with children
	parentNode.childNodes = []uint64{1, 2} // Assuming these are node pageNum references
	tx.writeNodes(parentNode, leftChild, rightChild)

	t.Run("Remove internal node item with left subtree replacement", func(t *testing.T) {
		_, err := parentNode.removeItemFromInternal(tx, 0) // Remove "M", should be replaced by "B"
		require.NoError(t, err)

		expectedParent := [][]byte{[]byte("B")}
		expectedLeft := [][]byte{[]byte("A")}

		require.True(t, reflect.DeepEqual(expectedParent, parentNode.itemsToKeys()))
		require.True(t, reflect.DeepEqual(expectedLeft, leftChild.itemsToKeys()))
	})

	tx.Rollback()

	// 2. Internal node with a deep left subtree
	// Structure:
	//          [M]
	//         /   \
	//   [A,B]     [X,Z]
	//      \
	//      [C,D]
	tx = db.Begin(false)
	parentNode = createNode([][]byte{
		[]byte("M")}, []uint64{1, 2}, 0,
	)
	leftChild = createNode([][]byte{
		[]byte("A"), []byte("B")}, []uint64{3}, 1)

	rightChild = createNode([][]byte{
		[]byte("X"), []byte("Z")}, nil, 2)

	deepLeftChild := createNode([][]byte{
		[]byte("C"), []byte("D")}, nil, 3,
	)
	tx.writeNodes(parentNode, leftChild, deepLeftChild)

	t.Run("Remove internal node item with deep left subtree replacement", func(t *testing.T) {
		_, err := parentNode.removeItemFromInternal(tx, 0) // Remove "M", should be replaced by "D"
		require.NoError(t, err)
		expectedParent := [][]byte{[]byte("D")}
		expectedDeepLeft := [][]byte{[]byte("C")}

		require.True(t, reflect.DeepEqual(expectedParent, parentNode.itemsToKeys()))
		require.True(t, reflect.DeepEqual(expectedDeepLeft, deepLeftChild.itemsToKeys()))
	})
	tx.Rollback()
}

func TestBNodeLookupInNode(t *testing.T) {
	node := NewBNode()
	for i := range 1000 {
		item := Item{
			Key:   []byte(fmt.Sprintf("test_%03d", i)),
			Value: []byte("foo"),
		}
		node.insertItemAt(&item, i)
	}
	pos, found := node.findKeyPosition([]byte("test_925"))
	require.True(t, found)
	require.Equal(t, pos, 925)
}

func TestBNodeSerialize(t *testing.T) {
	node := NewBNode()

	idx := node.insertItemAt(&Item{Key: []byte("test1"), Value: []byte("123")}, 0)
	require.Equal(t, idx, 0)

	idx = node.insertItemAt(&Item{Key: []byte("test2"), Value: []byte("123")}, 1)
	require.Equal(t, idx, 1)

	data := make([]byte, BTreePageSize)
	err := node.Serialize(data)
	require.NoError(t, err, "unable to Serialize")

	nodeDst := NewBNode()
	nodeDst.Deserialize(data)

	equalItems := reflect.DeepEqual(node.items, nodeDst.items)
	require.True(t, equalItems, "items not equal after deserialization")

	equalChildren := reflect.DeepEqual(node.childNodes, nodeDst.childNodes)
	require.True(t, equalChildren, "childNodes not equal after deserialization")
}

func TestSplitChild(t *testing.T) {
	db, _ := createTestDB(t)
	tx := db.Begin(false)

	// Create a full node (before splitting) with 5 keys
	fullNode := createNode([][]byte{
		[]byte("A"), []byte("B"), []byte("C"), []byte("D"), []byte("E"),
	}, nil, 0)

	// Create the parent node (initially empty)
	parentNode := createNode(nil, []uint64{1}, 0) // One child (fullNode)

	// Perform the split operation
	parentNode.splitChild(tx, fullNode, 0)

	// Verify that the middle key moved to the parent node
	expectedMiddle := []byte("C")
	if !bytes.Equal(parentNode.items[0].Key, expectedMiddle) {
		t.Errorf("Expected key %s in parent node, but got %s", expectedMiddle, parentNode.items[0].Key)
	}

	// Verify that fullNode now contains only the first half of items
	expectedFullItems := [][]byte{[]byte("A"), []byte("B")}
	if len(fullNode.items) != len(expectedFullItems) {
		t.Errorf("Expected fullNode to have %d items, but got %d", len(expectedFullItems), len(fullNode.items))
	}
	for i, key := range expectedFullItems {
		if !bytes.Equal(fullNode.items[i].Key, key) {
			t.Errorf("Expected fullNode[%d] = %s, but got %s", i, key, fullNode.items[i].Key)
		}
	}

	// Retrieve the newly created node
	newNode, _ := tx.getNode(parentNode.childNodes[1])

	// Verify that the new node contains the second half of the items
	expectedNewItems := [][]byte{[]byte("D"), []byte("E")}
	if len(newNode.items) != len(expectedNewItems) {
		t.Errorf("Expected newNode to have %d items, but got %d", len(expectedNewItems), len(newNode.items))
	}
	for i, key := range expectedNewItems {
		if !bytes.Equal(newNode.items[i].Key, key) {
			t.Errorf("Expected newNode[%d] = %s, but got %s", i, key, newNode.items[i].Key)
		}
	}

	// Verify that parentNode now has two child nodes
	if len(parentNode.childNodes) != 2 {
		t.Errorf("Expected 2 child nodes, but got %d", len(parentNode.childNodes))
	}
}
