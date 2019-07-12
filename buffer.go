package zipkinproxy

import (
	"github.com/flachnetz/dd-zipkin-proxy/proxy"
	"sync"
)

type SpansBuffer struct {
	lock     sync.Mutex
	position uint
	spans    []proxy.Span
}

func NewSpansBuffer(capacity uint) *SpansBuffer {
	spans := make([]proxy.Span, capacity)
	return &SpansBuffer{spans: spans}
}

func (buffer *SpansBuffer) ReadFrom(tracesCh <-chan proxy.Trace) {
	for trace := range tracesCh {
		buffer.lock.Lock()

		for _, span := range trace {
			buffer.spans[buffer.position] = span
			buffer.position = (buffer.position + 1) % uint(len(buffer.spans))
		}

		buffer.lock.Unlock()
	}
}

func (buffer *SpansBuffer) ToSlice() []proxy.Span {
	buffer.lock.Lock()
	defer buffer.lock.Unlock()

	var result []proxy.Span
	for _, span := range buffer.spans {
		if span.Id != 0 {
			result = append(result, span)
		}
	}

	return result
}
