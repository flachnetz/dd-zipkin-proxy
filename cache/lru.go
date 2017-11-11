package cache

import (
	"container/list"
	"sync"
)

type lruCache struct {
	lock sync.Mutex

	maxSize int

	size  int
	count int

	values map[string]lruCacheItem
	usage  *list.List
}

type lruCacheItem struct {
	element *list.Element
	value   []byte
}

func NewLRUCache(maxSize int) *lruCache {
	return &lruCache{
		values:  make(map[string]lruCacheItem),
		usage:   list.New(),
		maxSize: maxSize,
	}
}

func (c *lruCache) Get(key string) []byte {
	var result []byte

	c.lock.Lock()

	item, ok := c.values[key]
	if ok {
		result = item.value
		c.usage.MoveToFront(item.element)
	}

	c.lock.Unlock()

	return result
}

func (c *lruCache) Set(value []byte) {
	key := byteSliceToString(value)

	c.lock.Lock()

	// check if the element is already in the cache
	item, ok := c.values[key]
	if ok {
		c.usage.MoveToFront(item.element)

	} else {
		// remove the least recently used element
		c.ensureCacheSize()

		// and push the new value to the beginning of the cache
		el := c.usage.PushFront(key)
		c.values[key] = lruCacheItem{
			element: el,
			value:   value,
		}

		c.count++
		c.size += len(key)
	}

	c.lock.Unlock()
}

func (c *lruCache) ensureCacheSize() {
	for c.size >= c.maxSize {
		lru := c.usage.Back()
		key := lru.Value.(string)

		c.usage.Remove(lru)
		delete(c.values, key)

		c.count--
		c.size -= len(key)
	}
}

func (c *lruCache) Size() int {
	c.lock.Lock()
	result := c.size
	c.lock.Unlock()

	return result
}

func (c *lruCache) Count() int {
	c.lock.Lock()
	result := c.count
	c.lock.Unlock()

	return result
}

func (c *lruCache) Snapshot() []string {
	c.lock.Lock()

	var result []string
	for i := c.usage.Front(); i != nil; i = i.Next() {
		result = append(result, i.Value.(string))
	}

	c.lock.Unlock()

	return result
}
