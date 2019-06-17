package cache

import (
	"github.com/modern-go/reflect2"
	"github.com/rcrowley/go-metrics"
	"sync"
	"sync/atomic"
	"unsafe"
)

var binaryCacheLock sync.Mutex
var binaryCache = NewLRUCache(512)

var metricMissCount, metricHitCount uint64

func String(str string) string {
	return lookupCache(false, stringToByteSlice(str))
}

func StringForByteSliceCopy(value []byte) string {
	return lookupCache(true, value)
}

func StringForByteSliceNoCopy(value []byte) string {
	return lookupCache(false, value)
}

func lookupCache(copyOnInsert bool, value []byte) string {
	if len(value) == 0 {
		return ""
	}

	key := byteSliceToString(value)

	binaryCacheLock.Lock()
	cachedValue := binaryCache.Get(key)
	binaryCacheLock.Unlock()

	if cachedValue != "" {
		atomic.AddUint64(&metricHitCount, 1)
		return cachedValue
	}

	if copyOnInsert {
		// create a copy of the data that we want to cache
		newValue := make([]byte, 0, len(value))
		key = byteSliceToString(append(newValue, value...))
	}

	atomic.AddUint64(&metricMissCount, 1)

	binaryCacheLock.Lock()
	binaryCache.Set(key)
	binaryCacheLock.Unlock()

	return key
}

func stringToByteSlice(str string) []byte {
	return reflect2.UnsafeCastString(str)
}

func byteSliceToString(bytes []byte) string {
	if bytes == nil {
		return ""
	}

	return *(*string)(unsafe.Pointer(&bytes))
}

func RegisterCacheMetrics(m metrics.Registry) {
	metrics.NewRegisteredFunctionalGaugeFloat64("binary.cache.hit.rate", m, func() float64 {
		hitCount := atomic.SwapUint64(&metricHitCount, 0)
		missCount := atomic.SwapUint64(&metricMissCount, 0)
		return float64(hitCount) / float64(hitCount+missCount)
	})

	metrics.NewRegisteredFunctionalGauge("binary.cache.size", m, func() int64 {
		binaryCacheLock.Lock()
		defer binaryCacheLock.Unlock()
		return int64(binaryCache.Size())
	})

	metrics.NewRegisteredFunctionalGauge("binary.cache.count", m, func() int64 {
		binaryCacheLock.Lock()
		defer binaryCacheLock.Unlock()
		return int64(binaryCache.Count())
	})
}
