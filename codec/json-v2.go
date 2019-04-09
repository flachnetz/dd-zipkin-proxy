package codec

import (
	"encoding/json"
	"github.com/flachnetz/dd-zipkin-proxy/cache"
	"github.com/flachnetz/dd-zipkin-proxy/proxy"
	"github.com/pkg/errors"
	"io"
	"time"
)

type spanV2 struct {
	TraceID  Id  `json:"traceId"`
	ID       Id  `json:"id"`
	ParentID *Id `json:"parentId"`

	Name string `json:"name"`

	Endpoint *endpoint `json:"localEndpoint"`

	Tags map[string]string `json:"tags"`

	Kind string `json:"kind"`

	Timestamp int64 `json:"timestamp"`
	Duration  int64 `json:"duration"`
}

func ParseJsonV2(input io.Reader) ([]proxy.Span, error) {
	var decoded []spanV2
	if err := json.NewDecoder(input).Decode(&decoded); err != nil {
		return nil, errors.WithMessage(err, "parse spans for json v2")
	}

	parsedSpans := make([]proxy.Span, len(decoded))
	for _, span := range decoded {
		parsedSpans = append(parsedSpans, span.ToSpan())
	}

	return parsedSpans, nil
}

func (span *spanV2) ToSpan() proxy.Span {
	proxySpan := proxy.NewSpan(cache.String(span.Name),
		span.TraceID, span.ID, span.ParentID.OrZero())

	if span.Endpoint != nil {
		proxySpan.Service = span.Endpoint.ServiceName
	}

	for key, value := range span.Tags {
		proxySpan.AddTag(cache.String(key), cache.String(value))
	}

	proxySpan.AddTag("protocolVersion", "json v2")

	proxySpan.Timestamp = proxy.Microseconds(span.Timestamp)
	proxySpan.Duration = time.Duration(span.Duration) * time.Microsecond

	if span.Kind == "CLIENT" {
		proxySpan.AddTiming("cs", proxySpan.Timestamp)
		proxySpan.AddTiming("cr", proxySpan.Timestamp.Add(proxySpan.Duration))
	}

	if span.Kind == "SERVER" {
		proxySpan.AddTiming("sr", proxySpan.Timestamp)
		proxySpan.AddTiming("ss", proxySpan.Timestamp.Add(proxySpan.Duration))
	}

	return proxySpan
}
