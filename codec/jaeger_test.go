package codec

import (
	"github.com/flachnetz/dd-zipkin-proxy/proxy"
	. "github.com/onsi/gomega"
	"strings"
	"testing"
	"time"
)

func TestParseJaeger(t *testing.T) {
	g := NewGomegaWithT(t)

	spans, err := ParseJaeger(strings.NewReader(encodedJaeger))

	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(spans).To(HaveLen(1))

	g.Expect(spans[0]).To(Equal(proxy.Span{
		Id:      0xbeaf,
		Trace:   0xdead,
		Parent:  0xaaaa,
		Name:    "getconnection",
		Service: "core-services",

		// timestamp is also picked from the CS/CR if available
		Timestamp: proxy.Timestamp(1560276970 * time.Second),

		// duration is taken from the CS/CR if available
		Duration: 1000 * time.Millisecond,

		Tags: map[string]string{
			"lc":               "postgres",
			tagProtocolVersion: tagJaeger,
		},

		Timings: proxy.Timings{
			CS: proxy.Timestamp(1560276970 * time.Second),
			CR: proxy.Timestamp(1560276971 * time.Second),
		},
	}))
}

const encodedJaeger = `
{
    "data": [
        {
            "traceID": "dead",
            "spans": [
                {
                    "traceID": "dead",
                    "spanID": "beaf",
                    "operationName": "getconnection",
                    "references": [
                        {
                            "refType": "CHILD_OF",
                            "traceID": "dead",
                            "spanID": "aaaa"
                        }
                    ],
                    "startTime": 1560276970000000,
                    "duration": 1000000,
                    "tags": [
                        {
                            "key": "component",
                            "type": "string",
                            "value": "postgres"
                        },
                        {
                            "key": "internal.span.format",
                            "type": "string",
                            "value": "zipkin"
                        },
                        {
                            "key": "span.kind",
                            "type": "string",
                            "value": "client"
                        }
                    ],
                    "logs": [],
                    "processID": "p1",
                    "warnings": null
                }
            ],
            "processes": {
                "p1": {
                    "serviceName": "core-services",
                    "tags": []
                }
            },
            "warnings": null
        }
    ],
    "total": 0,
    "limit": 0,
    "offset": 0,
    "errors": null
}
`
