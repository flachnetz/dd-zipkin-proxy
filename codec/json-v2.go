package codec

import (
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
	if err := jsonConfig.NewDecoder(input).Decode(&decoded); err != nil {
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
		proxySpan.Service = cache.String(span.Endpoint.ServiceName)
	}

	for key, value := range span.Tags {
		proxySpan.AddTag(cache.String(key), cache.String(value))
	}

	proxySpan.AddTag(tagProtocolVersion, tagJsonV2)

	proxySpan.Timestamp = proxy.Microseconds(span.Timestamp)
	proxySpan.Duration = time.Duration(span.Duration) * time.Microsecond

	if proxySpan.Duration == 0 {
		proxySpan.Duration = 1 * time.Millisecond
	}

	if span.Kind == "CLIENT" {
		proxySpan.AddTiming(tagCS, proxySpan.Timestamp)
		proxySpan.AddTiming(tagCR, proxySpan.Timestamp.Add(proxySpan.Duration))
	}

	if span.Kind == "SERVER" {
		proxySpan.AddTiming(tagSR, proxySpan.Timestamp)
		proxySpan.AddTiming(tagSS, proxySpan.Timestamp.Add(proxySpan.Duration))
	}

	return proxySpan
}
