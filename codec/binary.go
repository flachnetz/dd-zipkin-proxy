package codec

import (
	"fmt"
	"github.com/flachnetz/dd-zipkin-proxy/cache"
	"github.com/flachnetz/dd-zipkin-proxy/proxy"
	"io"
	"math"
	"sync"
	"time"
)

type ByteWriter interface {
	Grow(int)
	WriteByte(byte) error
}

type StringWriter interface {
	WriteString(string) (int, error)
}

func encodeInt(w io.Writer, byteCount int, encoded uint64) error {
	var err error
	var bb []byte
	bw, ok := w.(ByteWriter)
	// To avoid reallocations, grow capacity to the largest possible size
	// for this integer
	if ok {
		bw.Grow(byteCount)
	} else {
		bb = make([]byte, 0, byteCount)
	}

	if encoded == 0 {
		if bw != nil {
			err = bw.WriteByte(0)
			if err != nil {
				return err
			}
		} else {
			bb = append(bb, byte(0))
		}
	} else {
		for encoded > 0 {
			b := byte(encoded & 127)
			encoded = encoded >> 7
			if !(encoded == 0) {
				b |= 128
			}
			if bw != nil {
				err = bw.WriteByte(b)
				if err != nil {
					return err
				}
			} else {
				bb = append(bb, b)
			}
		}
	}
	if bw == nil {
		_, err := w.Write(bb)
		return err
	}
	return nil

}

func readLong(r io.Reader) (int64, error) {
	var v uint64
	buf := make([]byte, 1)
	for shift := uint(0); ; shift += 7 {
		if _, err := io.ReadFull(r, buf); err != nil {
			return 0, err
		}
		b := buf[0]
		v |= uint64(b&127) << shift
		if b&128 == 0 {
			break
		}
	}
	datum := int64(v>>1) ^ -int64(v&1)
	return datum, nil
}

func readMapString(r io.Reader) (map[string]string, error) {
	m := make(map[string]string)
	for {
		blkSize, err := readLong(r)
		if err != nil {
			return nil, err
		}
		if blkSize == 0 {
			break
		}
		if blkSize < 0 {
			blkSize = -blkSize
			_, err := readLong(r)
			if err != nil {
				return nil, err
			}
		}
		for i := int64(0); i < blkSize; i++ {
			key, err := readString(r)
			if err != nil {
				return nil, err
			}
			val, err := readString(r)
			if err != nil {
				return nil, err
			}
			m[key] = val
		}
	}
	return m, nil
}

func readId(r io.Reader) (Id, error) {
	id, err := readLong(r)
	return Id(id), err
}

func readTimestamp(r io.Reader) (proxy.Timestamp, error) {
	ts, err := readLong(r)
	return proxy.Timestamp(ts), err
}

func readDuration(r io.Reader) (time.Duration, error) {
	value, err := readLong(r)
	return time.Duration(value), err
}

func BinaryDecode(r io.Reader) (proxy.Span, error) {
	var str = proxy.Span{}
	var err error
	str.Id, err = readId(r)
	if err != nil {
		return str, err
	}
	str.Trace, err = readId(r)
	if err != nil {
		return str, err
	}
	str.Parent, err = readId(r)
	if err != nil {
		return str, err
	}
	str.Name, err = readString(r)
	if err != nil {
		return str, err
	}
	str.Service, err = readString(r)
	if err != nil {
		return str, err
	}
	str.Timestamp, err = readTimestamp(r)
	if err != nil {
		return str, err
	}
	str.Duration, err = readDuration(r)
	if err != nil {
		return str, err
	}
	str.Timings.CS, err = readTimestamp(r)
	if err != nil {
		return str, err
	}
	str.Timings.CR, err = readTimestamp(r)
	if err != nil {
		return str, err
	}
	str.Timings.SS, err = readTimestamp(r)
	if err != nil {
		return str, err
	}
	str.Timings.SR, err = readTimestamp(r)
	if err != nil {
		return str, err
	}
	str.Tags, err = readMapString(r)
	if err != nil {
		return str, err
	}

	return str, nil
}

const byteSlicesSize = 1024

var byteSlices = sync.Pool{
	New: func() interface{} {
		return make([]byte, byteSlicesSize)
	},
}

func readString(r io.Reader) (string, error) {
	len, err := readLong(r)
	if err != nil {
		return "", err
	}

	// makeslice can fail depending on available memory.
	// We arbitrarily limit string size to sane default (~2.2GB).
	if len < 0 || len > math.MaxInt32 {
		return "", fmt.Errorf("string length out of range: %d", len)
	}

	if len == 0 {
		return "", nil
	}

	if len > byteSlicesSize {
		bb := make([]byte, len)
		if _, err = io.ReadFull(r, bb); err != nil {
			return "", err
		}

		return cache.StringForByteSliceNoCopy(bb), nil

	} else {
		// get a previously used byte slice form the pool
		byteSlice := byteSlices.Get().([]byte)
		byteSlice = byteSlice[:len]

		defer byteSlices.Put(byteSlice)

		// read the data
		bb := make([]byte, len)
		_, err = io.ReadFull(r, bb)
		if err != nil {
			return "", err
		}

		return cache.StringForByteSliceCopy(bb), nil
	}
}

func writeLong(r int64, w io.Writer) error {
	downShift := uint64(63)
	encoded := uint64((r << 1) ^ (r >> downShift))
	const maxByteSize = 10
	return encodeInt(w, maxByteSize, encoded)
}

func writeMapString(r map[string]string, w io.Writer) error {
	err := writeLong(int64(len(r)), w)
	if err != nil || len(r) == 0 {
		return err
	}
	for k, e := range r {
		err = writeString(k, w)
		if err != nil {
			return err
		}
		err = writeString(e, w)
		if err != nil {
			return err
		}
	}
	return writeLong(0, w)
}

func BinaryEncode(r proxy.Span, w io.Writer) error {
	var err error
	err = writeLong(int64(r.Id), w)
	if err != nil {
		return err
	}
	err = writeLong(int64(r.Trace), w)
	if err != nil {
		return err
	}
	err = writeLong(int64(r.Parent), w)
	if err != nil {
		return err
	}
	err = writeString(r.Name, w)
	if err != nil {
		return err
	}
	err = writeString(r.Service, w)
	if err != nil {
		return err
	}
	err = writeLong(int64(r.Timestamp), w)
	if err != nil {
		return err
	}
	err = writeLong(int64(r.Duration), w)
	if err != nil {
		return err
	}
	err = writeLong(int64(r.Timings.CS), w)
	if err != nil {
		return err
	}
	err = writeLong(int64(r.Timings.CR), w)
	if err != nil {
		return err
	}
	err = writeLong(int64(r.Timings.SS), w)
	if err != nil {
		return err
	}
	err = writeLong(int64(r.Timings.SR), w)
	if err != nil {
		return err
	}
	err = writeMapString(r.Tags, w)
	if err != nil {
		return err
	}

	return nil
}

func writeString(r string, w io.Writer) error {
	err := writeLong(int64(len(r)), w)
	if err != nil {
		return err
	}
	if sw, ok := w.(StringWriter); ok {
		_, err = sw.WriteString(r)
	} else {
		_, err = w.Write([]byte(r))
	}
	return err
}
