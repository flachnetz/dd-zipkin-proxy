package balance

import (
	"github.com/flachnetz/dd-zipkin-proxy/proxy"
	"testing"
	"time"
)

func BenchmarkMakeProducerMessage(b *testing.B) {
	span := proxy.Span{
		Id:      0xdeadbeef,
		Trace:   0xdeadbeef,
		Parent:  0,
		Name:    "consume",
		Service: "zipkin-proxy",
		Tags: map[string]string{
			"http.path":       "/some/url",
			"http.status":     "404",
			"protocolVersion": "json v1",
		},

		Timestamp: proxy.Timestamp(1560847100 * time.Nanosecond),
		Duration:  50 * time.Millisecond,

		Timings: proxy.Timings{
			CS: proxy.Timestamp(1560847100 * time.Nanosecond),
			CR: proxy.Timestamp(1560847100*time.Nanosecond + 50*time.Millisecond),
		},
	}

	for idx := 0; idx < b.N; idx++ {
		makeProducerMessage(span, "my topic")
	}
}
