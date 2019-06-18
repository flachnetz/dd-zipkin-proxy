package balance

import (
	"bytes"
	"github.com/flachnetz/dd-zipkin-proxy/codec"
	"github.com/flachnetz/dd-zipkin-proxy/proxy"
	. "github.com/onsi/gomega"
	"math/rand"
	"strconv"
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

func TestEncodeDecode(t *testing.T) {
	var spans []proxy.Span
	var encoded [][]byte

	for idx := 0; idx < 10000; idx++ {
		span := proxy.Span{
			Id:      proxy.Id(rand.Int()),
			Trace:   proxy.Id(rand.Int()),
			Parent:  proxy.Id(rand.Int()),
			Name:    "consume " + strconv.Itoa(rand.Int()),
			Service: "zipkin-proxy" + strconv.Itoa(rand.Int()),

			Tags: map[string]string{
				"http.path":       "/some/url",
				"http.status":     strconv.Itoa(rand.Intn(200) + 200),
				"protocolVersion": "json v1",
			},

			Timestamp: proxy.Timestamp(rand.Int63n(1000) + 1560847100*int64(time.Nanosecond)),
			Duration:  time.Duration(rand.Int63n(1000)) + 50*time.Millisecond,

			Timings: proxy.Timings{
				CS: proxy.Timestamp(rand.Int63n(1000) + 1560847100*int64(time.Nanosecond)),
			},
		}

		spans = append(spans, span)
		encoded = append(encoded, *(makeProducerMessage(span, "topic").Value.(*ptrSliceEncoder)))
	}

	g := NewGomegaWithT(t)
	for idx, span := range spans {
		g.Expect(codec.BinaryDecode(bytes.NewReader(encoded[idx]))).To(Equal(span))
	}

}
