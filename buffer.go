package zipkinproxy

import (
	"github.com/flachnetz/dd-zipkin-proxy/jsoncodec"
	"github.com/openzipkin-contrib/zipkin-go-opentracing/thrift/gen-go/zipkincore"
	"sync"
)

type SpansBuffer struct {
	lock     sync.Mutex
	position uint
	spans    []*zipkincore.Span
}

func NewSpansBuffer(capacity uint) *SpansBuffer {
	spans := make([]*zipkincore.Span, capacity)
	return &SpansBuffer{spans: spans}
}

func (buffer *SpansBuffer) ReadFrom(spans <-chan *zipkincore.Span) {
	for span := range spans {
		buffer.lock.Lock()
		buffer.spans[buffer.position] = span
		buffer.position = (buffer.position + 1) % uint(len(buffer.spans))
		buffer.lock.Unlock()
	}
}

func (buffer *SpansBuffer) ToSlice() []jsoncodec.SpanV1 {
	buffer.lock.Lock()
	defer buffer.lock.Unlock()

	var result []jsoncodec.SpanV1
	for _, span := range buffer.spans {
		if span != nil {
			result = append(result, jsoncodec.FromSpan(span))
		}
	}

	return result
}
