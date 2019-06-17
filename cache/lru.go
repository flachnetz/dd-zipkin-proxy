package cache

import (
	"github.com/sirupsen/logrus"
	"unsafe"
)

var log = logrus.WithField("prefix", "cache")

type lruCache struct {
	maxSize int

	size  int
	count int

	values map[string]*entry
	usage  linkedList
}

const itemOverhead = int(0 +
	unsafe.Sizeof(entry{}) + // the element in the list
	unsafe.Sizeof(&entry{}) + // the pointer to the item in the list
	unsafe.Sizeof("")) // the size of the string key in the map

func NewLRUCache(maxSize int) *lruCache {
	return &lruCache{
		values:  make(map[string]*entry),
		usage:   linkedList{},
		maxSize: maxSize,
	}
}

func (c *lruCache) Get(key string) string {
	var result string

	item, ok := c.values[key]
	if ok {
		result = item.Value
		c.usage.PushHead(item)
	}

	return result
}

func (c *lruCache) Set(value string) {
	// check if the element is already in the cache
	item, ok := c.values[value]
	if ok {
		c.usage.PushHead(item)

	} else {
		// remove the least recently used element
		c.ensureCacheSize()

		// create a new entry for it
		entry := &entry{Value: value}

		// and push the new value to the beginning of the cache
		c.usage.PushHead(entry)
		c.values[value] = entry

		c.count++
		c.size += len(value)
	}
}

func (c *lruCache) ensureCacheSize() {
	for c.Size() >= c.maxSize {
		el := c.usage.DropTail()
		if el == nil {
			log.Warnf("could not drop tail from list")
			continue
		}

		delete(c.values, el.Value)

		c.count--
		c.size -= len(el.Value)
	}
}

func (c *lruCache) Size() int {
	return c.size + c.count*itemOverhead
}

func (c *lruCache) Count() int {
	return c.count
}

type entry struct {
	prev, next *entry
	Value      string
}

type linkedList struct {
	head *entry
	tail *entry
}

func (l *linkedList) PushHead(e *entry) {
	if l.head == e {
		return
	}

	//
	if l.tail == e {
		l.tail = e.prev
	}

	// remove e as the follower of the previous node
	if e.prev != nil {
		e.prev.next = e.next
	}

	// remove e as the predecessor of the next node
	if e.next != nil {
		e.next.prev = e.prev
	}

	// add e as the predecessor of the first node
	if l.head != nil {
		l.head.prev = e
	}

	// and set it as the new head of the list
	e.prev = nil
	e.next = l.head
	l.head = e

	// if the list was empty, this is also the new tail
	if l.tail == nil {
		l.tail = e
	}
}

func (l *linkedList) DropTail() *entry {
	if l.tail == nil {
		return nil
	}

	entry := l.tail

	if prev := entry.prev; prev != nil {
		// remove entry as the successor of its predecessor
		prev.next = nil

		// and set the entries predecessor as the new tail
		l.tail = prev
	}

	// mark list as empty if the entry is also the lists head
	if l.head == entry {
		l.head = nil
		l.tail = nil
	}

	return entry
}

func (l *linkedList) Count() int {
	node := l.head

	var count int
	for node != nil {
		count++
		node = node.next
	}

	return count
}
