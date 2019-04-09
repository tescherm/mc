package cache

import (
	"bytes"
	"sync"
)

type Item struct {
	Key   string
	Value []byte

	versionID int64
}

func NewItem(key string, value []byte, versionID int64) *Item {
	return &Item{
		Key:       key,
		Value:     value,
		versionID: versionID,
	}
}

func (i *Item) VersionID() int64 {
	return i.versionID
}

func (i *Item) Size() uint64 {
	return uint64(len(i.Key) + len(i.Value))
}

type Cache interface {
	Get(key string) *Item
	Set(item *Item)
	CompareAndSwap(item *Item) (swapped bool)
	Remove(key string) *Item
	Clear()
	Size() uint64
	Stats() Stats
}

type cacheNode struct {
	item *Item

	prev *cacheNode
	next *cacheNode
}

func (n *cacheNode) Size() uint64 {
	return n.item.Size()
}

type cacheList struct {
	head *cacheNode
	tail *cacheNode
}

func (l *cacheList) shift() *cacheNode {
	if l.head == nil {
		return nil
	}
	return l.remove(l.head)
}

func (l *cacheList) remove(node *cacheNode) *cacheNode {
	// removing the final node
	if node == l.head && node == l.tail {
		l.head = nil
		l.tail = nil
		return node
	}

	// removing the head
	if node == l.head {
		node.next.prev = nil
		l.head = node.next
		node.next = nil
		node.prev = nil
		return node
	}

	// removing the tail
	if node == l.tail {
		node.prev.next = nil
		l.tail = node.prev

		node.next = nil
		node.prev = nil

		return node
	}

	node.prev.next = node.next
	node.next.prev = node.prev

	node.next = nil
	node.prev = nil

	return node
}

func (l *cacheList) setUsed(node *cacheNode) {
	// node already most recently used
	if node == l.tail {
		return
	}

	l.remove(node)
	l.add(node)
}

func (l *cacheList) add(node *cacheNode) {
	// first node in the list
	if l.head == nil && l.tail == nil {
		l.head = node
		l.tail = node

		return
	}

	// make node the new tail
	l.tail.next = node
	node.prev = l.tail
	l.tail = node
}

func (l *cacheList) clear() {
	l.head = nil
	l.tail = nil
}

type Config struct {
	// Capacity is the cache capacity, in bytes
	Capacity uint64
}

type Stats struct {
	Evicts  uint64
	Removes uint64
	Clears  uint64
	Sets    uint64
	Hits    uint64
	Misses  uint64

	CurrentCapacity uint64
}

func NewLRUCache(conf Config) *LRUCache {
	nodeMap := make(map[string]*cacheNode, 0)
	list := &cacheList{}

	return &LRUCache{
		nodeMap:         nodeMap,
		list:            list,
		maxCapacity:     conf.Capacity,
		currentCapacity: 0,
	}
}

type LRUCache struct {
	sync.RWMutex

	nodeMap map[string]*cacheNode
	list    *cacheList

	// current and max cache capacity, in bytes
	maxCapacity     uint64
	currentCapacity uint64

	// stats
	evicts  uint64
	removes uint64
	clears  uint64
	sets    uint64
	hits    uint64
	misses  uint64
}

func (c *LRUCache) Get(key string) *Item {
	c.Lock()
	defer c.Unlock()

	node, ok := c.nodeMap[key]
	if !ok {
		c.misses++
		return nil
	}

	c.hits++
	c.list.setUsed(node)

	item := Item(*node.item)
	return &item
}

func (c *LRUCache) Set(item *Item) {
	c.Lock()
	defer c.Unlock()

	c.doSet(item)
}

func (c *LRUCache) CompareAndSwap(item *Item) bool {
	c.Lock()
	defer c.Unlock()

	node, ok := c.nodeMap[item.Key]

	if ok {
		if node.item.VersionID() != item.VersionID() {
			return false
		}
	}

	c.doSet(item)
	return true
}

func (c *LRUCache) doSet(item *Item) {
	node, ok := c.nodeMap[item.Key]

	if ok {
		// entry already exists
		node.item = item
		c.list.setUsed(node)

		c.currentCapacity -= node.Size()
	} else {
		// we are seeing this item for the first time
		node = &cacheNode{
			item: item,
		}
		c.list.add(node)
	}

	node.item.versionID++

	c.nodeMap[item.Key] = node
	c.currentCapacity += node.Size()
	c.sets++

	for c.currentCapacity > c.maxCapacity {
		// evict the least recently used by removing at the head
		del := c.list.shift()
		delete(c.nodeMap, del.item.Key)

		c.currentCapacity -= del.Size()
		c.evicts++
	}
}

func (c *LRUCache) Remove(key string) *Item {
	c.Lock()
	defer c.Unlock()

	node, ok := c.nodeMap[key]
	if !ok {
		return nil
	}

	delete(c.nodeMap, key)
	c.list.remove(node)

	c.currentCapacity -= node.Size()
	c.removes++

	item := Item(*node.item)
	return &item
}

func (c *LRUCache) Clear() {
	c.Lock()
	defer c.Unlock()

	c.nodeMap = make(map[string]*cacheNode, 0)
	c.list.clear()
	c.currentCapacity = 0

	c.clears++
}

func (c *LRUCache) Size() uint64 {
	c.RLock()
	defer c.RUnlock()

	return uint64(len(c.nodeMap))
}

func (c *LRUCache) Stats() Stats {
	c.RLock()
	defer c.RUnlock()

	return Stats{
		Clears:          c.clears,
		Evicts:          c.evicts,
		Hits:            c.hits,
		Misses:          c.misses,
		Removes:         c.removes,
		Sets:            c.sets,
		CurrentCapacity: c.currentCapacity,
	}
}

func (c *LRUCache) String() string {
	var buf bytes.Buffer
	buf.WriteString("cache =====\n")
	for node := c.list.tail; node != nil; node = node.next {
		buf.WriteString(node.item.Key)
		buf.WriteString("\n")
	}
	buf.WriteString("=====\n")
	return buf.String()
}
