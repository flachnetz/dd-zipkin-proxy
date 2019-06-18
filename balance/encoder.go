package balance

import (
	"bytes"
	"encoding/binary"
	"github.com/Shopify/sarama"
	"github.com/flachnetz/dd-zipkin-proxy/codec"
	"github.com/flachnetz/dd-zipkin-proxy/proxy"
	"sync"
	"sync/atomic"
)

var metricSampleCounter uint64

var encoders = sync.Pool{
	New: func() interface{} {
		return &encoder{}
	},
}

func makeProducerMessage(span proxy.Span, topic string) *sarama.ProducerMessage {
	enc := encoders.Get().(*encoder)
	defer encoders.Put(enc)

	return enc.Encode(span, topic)
}

type encoder struct {
	buf    []byte
	slices [][]byte
}

const expectedMaxSpanSize = 512

func (enc *encoder) Encode(span proxy.Span, topic string) *sarama.ProducerMessage {
	// get memory for encoding to avro
	if len(enc.buf) < 8+expectedMaxSpanSize {
		enc.buf = make([]byte, 32*1024)
	}

	// get memory for encoding keys and slices
	if len(enc.slices) < 2 {
		enc.slices = make([][]byte, 1024)
	}

	// get a pointer to slices. This way we dont need to allocate later, as we can just
	// put the pointer into the "Encoder" interface.
	keySlice := &enc.slices[0]
	valueSlice := &enc.slices[1]
	enc.slices = enc.slices[2:]

	// encode trace-id to do sharding
	*keySlice, enc.buf = enc.buf[:8], enc.buf[8:]
	binary.LittleEndian.PutUint64(*keySlice, span.Trace.Uint64())

	// encode the message in at most 'expectedMaxSpanSize' bytes (or allocate somewhere else)
	buf := bytes.NewBuffer(enc.buf[0:0:expectedMaxSpanSize])
	_ = codec.BinaryEncode(span, buf)

	// only consume from enc.buf if the Buffer did not re-allocate.
	if &buf.Bytes()[0] == &enc.buf[0] {
		enc.buf = enc.buf[buf.Len():]
	}

	// only update the histogram every few iterations
	if atomic.AddUint64(&metricSampleCounter, 1)%128 == 0 {
		metricKafkaSpanSize.Update(int64(buf.Len()))
	}

	*valueSlice = buf.Bytes()

	return &sarama.ProducerMessage{
		Key:   (*ptrSliceEncoder)(keySlice),
		Value: (*ptrSliceEncoder)(valueSlice),
		Topic: topic,
	}
}

type ptrSliceEncoder []byte

func (p *ptrSliceEncoder) Encode() ([]byte, error) {
	return *p, nil
}

func (p *ptrSliceEncoder) Length() int {
	return len(*p)
}
