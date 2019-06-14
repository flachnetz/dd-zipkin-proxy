package codec

import (
	"bytes"
	"github.com/flachnetz/dd-zipkin-proxy/proxy"
	. "github.com/onsi/gomega"
	"strings"
	"testing"
	"time"
)

func TestParseJsonV2(t *testing.T) {
	g := NewGomegaWithT(t)

	spans, err := ParseJsonV2(strings.NewReader(encodedJsonV2))

	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(spans).To(HaveLen(1))

	g.Expect(spans[0]).To(Equal(proxy.Span{
		Id:      0xdead,
		Trace:   0xbeaf,
		Parent:  0xbeaf,
		Name:    "span name",
		Service: "my-service",

		Timestamp: proxy.Timestamp(1560276970 * time.Second),
		Duration:  50 * time.Millisecond,

		Tags: map[string]string{
			"http.path":        "/my/path",
			"http.status":      "404",
			tagProtocolVersion: tagJsonV2,
		},

		Timings: proxy.Timings{
			CS: proxy.Timestamp(1560276970 * time.Second),
			CR: proxy.Timestamp(1560276970*time.Second + 50*time.Millisecond),
		},
	}))
}

func BenchmarkParseJsonV2(b *testing.B) {
	data := []byte(encodedJsonV2)

	var sum proxy.Id
	for idx := 0; idx < b.N; idx++ {
		spans, _ := ParseJsonV2(bytes.NewReader(data))
		for _, span := range spans {
			sum += span.Id
		}
	}
}

const encodedJsonV2 = `[
		{
			"traceId": "beaf",
			"id": "dead",
			"parentId": "beaf",

			"name": "span name",

			"localEndpoint": {
				"serviceName": "my-service"
			},

			"tags": {
				"http.path": "/my/path",
				"http.status": "404"
			},

			"kind": "CLIENT",

			"timestamp": 1560276970000000,
			"duration": 50000
		}
	]`
