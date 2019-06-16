package codec

import (
	"bytes"
	"fmt"
	"github.com/flachnetz/dd-zipkin-proxy/cache"
	"github.com/flachnetz/dd-zipkin-proxy/proxy"
	"io"
	"math"
	"time"
)

func encodeInt(w *bytes.Buffer, byteCount int, encoded uint64) error {
	w.Grow(byteCount)

	if encoded == 0 {
		return w.WriteByte(0)
	} else {
		for encoded > 0 {
			b := byte(encoded & 127)
			encoded = encoded >> 7
			if !(encoded == 0) {
				b |= 128
			}
			if err := w.WriteByte(b); err != nil {
				return err
			}
		}
	}

	return nil
}

func readLong(r *bytes.Reader) (int64, error) {
	var v uint64
	for shift := uint(0); ; shift += 7 {
		b, err := r.ReadByte()
		if err != nil {
			return 0, err
		}

		v |= uint64(b&127) << shift
		if b&128 == 0 {
			break
		}
	}

	datum := int64(v>>1) ^ -int64(v&1)
	return datum, nil
}

func readMapString(r *bytes.Reader) (map[string]string, error) {
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

func readId(r *bytes.Reader) (Id, error) {
	id, err := readLong(r)
	return Id(id), err
}

func readTimestamp(r *bytes.Reader) (proxy.Timestamp, error) {
	ts, err := readLong(r)
	return proxy.Timestamp(ts), err
}

func readDuration(r *bytes.Reader) (time.Duration, error) {
	value, err := readLong(r)
	return time.Duration(value), err
}

func BinaryDecode(r *bytes.Reader) (proxy.Span, error) {
	var span = proxy.Span{}
	var err error
	span.Id, err = readId(r)
	if err != nil {
		return span, err
	}
	span.Trace, err = readId(r)
	if err != nil {
		return span, err
	}
	span.Parent, err = readId(r)
	if err != nil {
		return span, err
	}
	span.Name, err = readString(r)
	if err != nil {
		return span, err
	}
	span.Service, err = readString(r)
	if err != nil {
		return span, err
	}
	span.Timestamp, err = readTimestamp(r)
	if err != nil {
		return span, err
	}
	span.Duration, err = readDuration(r)
	if err != nil {
		return span, err
	}
	span.Timings.CS, err = readTimestamp(r)
	if err != nil {
		return span, err
	}
	span.Timings.CR, err = readTimestamp(r)
	if err != nil {
		return span, err
	}
	span.Timings.SS, err = readTimestamp(r)
	if err != nil {
		return span, err
	}
	span.Timings.SR, err = readTimestamp(r)
	if err != nil {
		return span, err
	}
	span.Tags, err = readMapString(r)
	if err != nil {
		return span, err
	}

	return span, nil
}

func readString(r *bytes.Reader) (string, error) {
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

	if len > pooledBufferSliceLength {
		bb := make([]byte, len)
		if _, err = io.ReadFull(r, bb); err != nil {
			return "", err
		}

		return cache.StringForByteSliceNoCopy(bb), nil

	} else {
		// get a previously used byte slice form the pool
		byteSliceIf := bufferPool.Get()
		bb := byteSliceIf.([]byte)[:len]

		defer bufferPool.Put(byteSliceIf)

		// read the data
		_, err = io.ReadFull(r, bb)
		if err != nil {
			return "", err
		}

		return cache.StringForByteSliceCopy(bb), nil
	}
}

func writeLong(r int64, w *bytes.Buffer) error {
	downShift := uint64(63)
	encoded := uint64((r << 1) ^ (r >> downShift))
	const maxByteSize = 10
	return encodeInt(w, maxByteSize, encoded)
}

func writeMapString(r map[string]string, w *bytes.Buffer) error {
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

func BinaryEncode(r proxy.Span, w *bytes.Buffer) error {
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

func writeString(r string, w *bytes.Buffer) error {
	err := writeLong(int64(len(r)), w)
	if err != nil {
		return err
	}

	_, err = w.WriteString(r)
	return err
}
