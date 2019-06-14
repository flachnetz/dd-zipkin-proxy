package codec

import (
	"github.com/flachnetz/dd-zipkin-proxy/proxy"
	"github.com/pkg/errors"
	"io"
	"strings"
	"time"
)

type spanV2 struct {
	TraceID  Id `json:"traceId"`
	ID       Id `json:"id"`
	ParentID Id `json:"parentId"`

	Name string `json:"name"`

	Endpoint endpoint `json:"localEndpoint"`

	Tags map[string]string `json:"tags"`

	Kind string `json:"kind"`

	Timestamp int64 `json:"timestamp"`
	Duration  int64 `json:"duration"`
}

func ParseJsonV2(input io.Reader) ([]proxy.Span, error) {
	var decoded []spanV2
	if err := jsonConfig.NewDecoder(input).Decode(&decoded); err != nil {
		return nil, errors.WithMessage(err, "parse spans for json v2")
	}

	parsedSpans := make([]proxy.Span, len(decoded))
	for idx, span := range decoded {
		parsedSpans[idx] = span.ToSpan()
	}

	return parsedSpans, nil
}

func (span *spanV2) ToSpan() proxy.Span {
	proxySpan := proxy.NewSpan(span.Name, span.TraceID, span.ID, span.ParentID)

	if span.Endpoint.ServiceName != "" {
		proxySpan.Service = span.Endpoint.ServiceName
	}

	proxySpan.Tags = span.Tags
	proxySpan.AddTag(tagProtocolVersion, tagJsonV2)

	proxySpan.Timestamp = proxy.Microseconds(span.Timestamp)
	proxySpan.Duration = time.Duration(span.Duration) * time.Microsecond

	if proxySpan.Duration == 0 {
		proxySpan.Duration = 1 * time.Millisecond
	}

	if strings.EqualFold(span.Kind, "client") {
		proxySpan.Timings.CS = proxySpan.Timestamp
		proxySpan.Timings.CR = proxySpan.Timestamp.Add(proxySpan.Duration)
	}

	if strings.EqualFold(span.Kind, "server") {
		proxySpan.Timings.SR = proxySpan.Timestamp
		proxySpan.Timings.SS = proxySpan.Timestamp.Add(proxySpan.Duration)
	}

	return proxySpan
}
