package cache

import (
	"errors"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/rcrowley/go-metrics"
	"io"
	"reflect"
	"sync/atomic"
	"unsafe"
)

var binaryCache = NewLRUCache(50000)

var metricMissCount, metricHitCount uint64
var metricReadBinarySize = metrics.NewHistogram(metrics.NewUniformSample(1024 * 8))

var invalidDataLength = thrift.NewTProtocolExceptionWithType(thrift.INVALID_DATA, errors.New("invalid data length"))

type CachingProtocol struct {
	*thrift.TBinaryProtocol
	trans   thrift.TRichTransport
	scratch [1024]byte
}

func NewProtocol(p *thrift.TBinaryProtocol) *CachingProtocol {
	ptr := reflect.ValueOf(p).Elem().FieldByName("trans").UnsafeAddr()
	trans := *(*thrift.TRichTransport)(unsafe.Pointer(ptr))

	return &CachingProtocol{
		TBinaryProtocol: p,
		trans:           trans,
	}
}

func (p *CachingProtocol) ReadString() (string, error) {
	value, err := p.ReadBinary()

	if err == nil && len(value) > 0 {
		return byteSliceToString(value), nil

	} else {
		return "", err
	}
}

func (p *CachingProtocol) ReadBinary() ([]byte, error) {
	size, e := p.ReadI32()
	if e != nil {
		return nil, e
	}

	if size < 0 {
		return nil, invalidDataLength
	}

	if uint64(size) > p.trans.RemainingBytes() {
		return nil, invalidDataLength
	}

	metricReadBinarySize.Update(int64(size))

	// we can try to read the data into our scratch space if possible
	var err error
	var buf []byte
	if int(size) <= len(p.scratch) {
		_, err = io.ReadFull(p.trans, p.scratch[:size])

		if err == nil {
			// cache the value directly from scratch, do a copy if needed
			buf = byteSlice(true, p.scratch[:size])
		}

	} else {
		// normal code for large buffers
		buf = make([]byte, int(size))
		_, err = io.ReadFull(p.trans, buf)

		// deduplicate
		if err == nil && size > 0 {
			buf = byteSlice(false, buf)
		}
	}

	return buf, thrift.NewTProtocolException(err)
}

func String(str string) string {
	bytes := stringToByteSlice(str)
	return byteSliceToString(ByteSlice(bytes))
}

func ByteSlice(value []byte) []byte {
	return byteSlice(false, value)
}

func StringForByteSliceCopy(value []byte) string {
	return byteSliceToString(byteSlice(true, value))
}

func StringForByteSliceNoCopy(value []byte) string {
	return byteSliceToString(byteSlice(false, value))
}

func byteSlice(copyOnInsert bool, value []byte) []byte {
	key := byteSliceToString(value)

	cachedValue := binaryCache.Get(key)
	if cachedValue != nil {
		atomic.AddUint64(&metricHitCount, 1)
		return cachedValue
	}

	if copyOnInsert {
		newValue := make([]byte, 0, len(value))
		value = append(newValue, value...)
	}

	atomic.AddUint64(&metricMissCount, 1)
	binaryCache.Set(value)

	return value
}

// from runtime/string.go
type stringStruct struct {
	str unsafe.Pointer
	len int
}

// Returns the content of the string as a byte slice. This uses unsafe magic
// to not copy the backing data of the string into a new slice
func stringToByteSlice(str string) []byte {
	p := (*stringStruct)(unsafe.Pointer(&str)).str
	if p == nil {
		return nil
	}

	data := (*[0xffffff]byte)(p)
	return data[:len(str)]
}

// Returns a string that shares the data with the given byte slice.
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
		return 1000 * float64(hitCount) / float64(hitCount+missCount)
	})

	metrics.NewRegisteredFunctionalGauge("binary.cache.size", m, func() int64 {
		return int64(binaryCache.Size())
	})

	metrics.NewRegisteredFunctionalGauge("binary.cache.count", m, func() int64 {
		return int64(binaryCache.Count())
	})
}
