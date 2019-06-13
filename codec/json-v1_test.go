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
	}))
}

const encodedJsonV1 = `[
		{
			"traceId": "beaf",
			"id": "dead",
			"parentId": "beaf",

			"name": "span name",

			"timestamp": 1560276900000000,
			"duration": 50000,

			"annotations": [
				{
					"timestamp": 1560276970000000,
					"value": "cs",
					"endpoint": {"serviceName": "my-service"}
				},
				{
					"timestamp": 1560276971000000,
					"value": "cr",
					"endpoint": {"serviceName": "my-service"}
				},
				{
					"timestamp": 1560276972000000,
					"value": "sr",
					"endpoint": {"serviceName": "my-service"}
				},
				{
					"timestamp": 1560276973000000,
					"value": "ss",
					"endpoint": {"serviceName": "my-service"}
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
