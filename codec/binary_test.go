package codec

import (
	"bytes"
	"github.com/flachnetz/dd-zipkin-proxy/proxy"
	. "github.com/onsi/gomega"
	"testing"
	"time"
)

var binaryTestSpan = proxy.Span{
	Id:      0xdead,
	Trace:   0xbeaf,
	Parent:  0xbeaf,
	Name:    "span name",
	Service: "my-service",

	// timestamp is also picked from the CS/CR if available
	Timestamp: proxy.Timestamp(1560276970 * time.Second),

	// duration is taken from the CS/CR if available
	Duration: 1000 * time.Millisecond,

	Tags: map[string]string{
		"http.path":        "/my/path",
		tagProtocolVersion: tagJsonV1,
	},

	Timings: proxy.Timings{
		CS: proxy.Timestamp(1560276970 * time.Second),
		CR: proxy.Timestamp(1560276971 * time.Second),
		SR: proxy.Timestamp(1560276972 * time.Second),
		SS: proxy.Timestamp(1560276973 * time.Second),
	},
}

func TestBinaryEncoding(t *testing.T) {
	g := NewGomegaWithT(t)

	var buf bytes.Buffer
	g.Expect(BinaryEncode(binaryTestSpan, &buf)).ToNot(HaveOccurred())
	g.Expect(BinaryDecode(bytes.NewReader(buf.Bytes()))).To(Equal(binaryTestSpan))
}

func BenchmarkBinaryDecode(b *testing.B) {
	var buf bytes.Buffer
	_ = BinaryEncode(binaryTestSpan, &buf)

	var reader bytes.Reader

	var sum Id
	for idx := 0; idx < b.N; idx++ {
		reader.Reset(buf.Bytes())
		decoded, _ := BinaryDecode(&reader)
		sum += decoded.Id
	}
}

func BenchmarkBinaryEncode(b *testing.B) {
	var buf bytes.Buffer

	var sum int
	for idx := 0; idx < b.N; idx++ {
		buf.Reset()
		_ = BinaryEncode(binaryTestSpan, &buf)

		sum += buf.Len()
	}
}
