package cache

import (
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/karlseguin/ccache"
	"github.com/rcrowley/go-metrics"
	"hash/fnv"
	"strconv"
	"sync/atomic"
	"time"
	"unsafe"
)

var binaryCache = ccache.New(ccache.Configure())
var binaryCacheHit, binaryCacheMiss, binaryCacheConflict uint64

type CachingProtocol struct {
	*thrift.TBinaryProtocol
}

var prefixString = []byte("s")
var prefixBytes = []byte("b")

func (p CachingProtocol) ReadString() (string, error) {
	value, err := p.TBinaryProtocol.ReadString()

	// try to de-duplicate the data.
	if err == nil && len(value) > 0 {
		var key string
		if len(value) < 16 {
			key = value
		} else {
			key = cacheKeyForValue(prefixString, bytesOfString(value))
		}

		item := binaryCache.Get(key)
		if item != nil {
			cachedValue, ok := item.Value().(string)
			if ok && len(cachedValue) == len(value) {
				atomic.AddUint64(&binaryCacheHit, 1)
				value = cachedValue

			} else {
				atomic.AddUint64(&binaryCacheConflict, 1)
			}

		} else {
			atomic.AddUint64(&binaryCacheMiss, 1)
			binaryCache.Set(key, value, 1*time.Hour)
		}
	}

	return value, err
}

func (p CachingProtocol) ReadBinary() ([]byte, error) {
	value, err := p.TBinaryProtocol.ReadBinary()

	// try to de-duplicate the data.
	if err == nil && len(value) > 0 {
		key := cacheKeyForValue(prefixBytes, value)

		item := binaryCache.Get(key)
		if item != nil {
			cachedValue, ok := item.Value().([]byte)
			if ok && len(cachedValue) == len(value) {
				atomic.AddUint64(&binaryCacheHit, 1)
				value = cachedValue

			} else {
				atomic.AddUint64(&binaryCacheConflict, 1)
			}

		} else {
			atomic.AddUint64(&binaryCacheMiss, 1)
			binaryCache.Set(key, value, 1*time.Hour)
		}
	}

	return value, err
}

// Creates a
func cacheKeyForValue(prefix []byte, data []byte) string {
	h := fnv.New64()
	h.Write(prefix)
	h.Write(data)
	return strconv.FormatUint(h.Sum64(), 36)
}

// Returns the content of the string as a byte slice. This uses unsafe magic
// to not copy the backing data of the string into a new slice
func bytesOfString(str string) []byte {
	// from runtime/string.go
	type stringStruct struct {
		str unsafe.Pointer
		len int
	}

	p := (*stringStruct)(unsafe.Pointer(&str)).str
	data := (*[0xffffff]byte)(p)
	return data[:len(str)]
}

func RegisterCacheMetrics(m metrics.Registry) {
	metrics.NewRegisteredFunctionalGauge("binary.cache.hit.count", m, func() int64 {
		return int64(atomic.LoadUint64(&binaryCacheHit))
	})

	metrics.NewRegisteredFunctionalGauge("binary.cache.miss.count", m, func() int64 {
		return int64(atomic.LoadUint64(&binaryCacheMiss))
	})

	metrics.NewRegisteredFunctionalGauge("binary.cache.conflict.count", m, func() int64 {
		return int64(atomic.LoadUint64(&binaryCacheConflict))
	})

	metrics.NewRegisteredFunctionalGaugeFloat64("binary.cache.hit.rate", m, func() float64 {
		hitCount := atomic.LoadUint64(&binaryCacheHit)
		missCount := atomic.LoadUint64(&binaryCacheMiss)
		conflictCount := atomic.LoadUint64(&binaryCacheConflict)
		return float64(hitCount) / float64(hitCount+missCount+conflictCount)
	})
}
