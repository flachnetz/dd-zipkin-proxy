package cache

import (
	"errors"
	"github.com/apache/thrift/lib/go/thrift"
	"io"
	"reflect"
	"unsafe"
)

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
			buf = stringToByteSlice(lookupCache(true, p.scratch[:size]))
		}

	} else {
		// normal code for large buffers
		buf = make([]byte, int(size))
		_, err = io.ReadFull(p.trans, buf)

		// deduplicate
		if err == nil && size > 0 {
			buf = stringToByteSlice(lookupCache(false, buf))
		}
	}

	return buf, thrift.NewTProtocolException(err)
}
