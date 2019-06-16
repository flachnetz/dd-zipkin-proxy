package cache

import (
	"fmt"
	"strings"
	"testing"
)

func TestLruCache_Get(t *testing.T) {
	format := func(idx int) string {
		return strings.Repeat(fmt.Sprintf("%016d", idx), 16)
	}

	l := NewLRUCache(1024 * 1024)
	for idx := 0; idx < 100000; idx++ {
		l.Set(format(idx))
	}

	if l.Size() > 2*1024*1024 {
		t.Fatal(l.Size())
	}

	if l.Count() > 5000 {
		t.Fatal(l.Size())
	}

	for idx := 99000; idx < 100000; idx++ {
		if l.Get(format(idx)) == "" {
			t.Fatal("Item should be in the cache: " + format(idx))
		}
	}
}
