package storage

import "sync"

// cached nodes are immutable. Read transactions may share them, while write
// transactions clone the node's slices before making any structural change.
// Ten page sizes per entry conservatively cover the raw page, decoded Item
// array, pointer slice, child array, and map overhead even for unusually dense
// nodes containing many tiny keys.
const nodeCacheEntryPageMultiplier = 10

type nodeCacheValue struct {
	node *BNode
	slot int
}

type nodeCacheSlot struct {
	pageNum uint64
	valid   bool
}

type nodeCache struct {
	mu       sync.Mutex
	nodes    sync.Map
	slots    []nodeCacheSlot
	next     int
	capacity int
}

func newNodeCache(pageSize uint64, cacheBytes int64) *nodeCache {
	if pageSize == 0 || cacheBytes <= 0 {
		return nil
	}
	entryBytes := pageSize * nodeCacheEntryPageMultiplier
	if uint64(cacheBytes) < entryBytes {
		return nil
	}
	capacity := int(uint64(cacheBytes) / entryBytes)
	return &nodeCache{
		slots:    make([]nodeCacheSlot, capacity),
		capacity: capacity,
	}
}

func (cache *nodeCache) get(pageNum uint64) (*BNode, bool) {
	if cache == nil {
		return nil, false
	}
	value, found := cache.nodes.Load(pageNum)
	if !found {
		return nil, false
	}
	entry, ok := value.(*nodeCacheValue)
	if !ok || entry == nil {
		return nil, false
	}
	return entry.node, true
}

func (cache *nodeCache) put(pageNum uint64, node *BNode) {
	if cache == nil || node == nil || cache.capacity == 0 {
		return
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()

	if value, found := cache.nodes.Load(pageNum); found {
		existing := value.(*nodeCacheValue)
		cache.nodes.Store(pageNum, &nodeCacheValue{node: node, slot: existing.slot})
		return
	}

	slotIndex := cache.next
	cache.next = (cache.next + 1) % cache.capacity
	oldSlot := cache.slots[slotIndex]
	if oldSlot.valid {
		if value, found := cache.nodes.Load(oldSlot.pageNum); found && value.(*nodeCacheValue).slot == slotIndex {
			cache.nodes.Delete(oldSlot.pageNum)
		}
	}
	cache.slots[slotIndex] = nodeCacheSlot{pageNum: pageNum, valid: true}
	cache.nodes.Store(pageNum, &nodeCacheValue{node: node, slot: slotIndex})
}

func (cache *nodeCache) invalidate(pageNum uint64) {
	if cache == nil {
		return
	}
	cache.mu.Lock()
	if value, found := cache.nodes.Load(pageNum); found {
		entry := value.(*nodeCacheValue)
		cache.nodes.Delete(pageNum)
		if entry.slot >= 0 && entry.slot < len(cache.slots) && cache.slots[entry.slot].pageNum == pageNum {
			cache.slots[entry.slot].valid = false
		}
	}
	cache.mu.Unlock()
}
