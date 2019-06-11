package codec

import (
	"github.com/flachnetz/dd-zipkin-proxy/proxy"
	. "github.com/onsi/gomega"
	"strings"
	"testing"
	"time"
)

func TestParseJsonV1(t *testing.T) {
	g := NewGomegaWithT(t)

	spans, err := ParseJsonV1(strings.NewReader(encodedJsonV1))

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
			tagProtocolVersion: tagJsonV1,
		},

		Timings: map[string]proxy.Timestamp{
			"cs": proxy.Timestamp(1560276970 * time.Second),
		},
	}))
}

const encodedJsonV1 = `[
		{
			"traceId": "beaf",
			"id": "dead",
			"parentId": "beaf",

			"name": "span name",

			"timestamp": 1560276970000000,
			"duration": 50000,

			"annotations": [
				{
					"timestamp": 1560276970000000,
					"value": "cs",
					"endpoint": {
						"serviceName": "my-service"
					}
				}
			],

			"binaryAnnotations": [
				{
					"key": "http.path",
					"value": "/my/path", 
					"endpoint": {
						"serviceName": "my-service"
					}
				}
			]
		}
	]`
