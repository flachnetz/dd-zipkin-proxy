package cache

import (
	"fmt"
	"strings"
	"testing"
)

func TestLruCache_Get(t *testing.T) {
	l := NewLRUCache(1024 * 1024)
	for idx := 0; idx < 100000; idx++ {
		l.Set(strings.Repeat(fmt.Sprintf("%016d", idx), 16))
	}

	if l.Size() > 2*1024*1024 {
		t.Fatal(l.Size())
	}

	if l.Count() > 5000 {
		t.Fatal(l.Size())
	}
}
