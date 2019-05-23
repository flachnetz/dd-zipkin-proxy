package codec

import (
	"encoding/json"
	"github.com/flachnetz/dd-zipkin-proxy/cache"
	"github.com/flachnetz/dd-zipkin-proxy/proxy"
	"github.com/pkg/errors"
	"io"
	"time"
)

type spanV1 struct {
	TraceID  Id  `json:"traceId"`
	ID       Id  `json:"id"`
	ParentID *Id `json:"parentId"`

	Annotations       []annotationV1       `json:"annotations"`
	BinaryAnnotations []binaryAnnotationV1 `json:"binaryAnnotations"`

	Name string `json:"name"`

	Timestamp *int64 `json:"timestamp"`
	Duration  *int64 `json:"duration"`
}

type annotationV1 struct {
	Timestamp int64     `json:"timestamp"`
	Value     string    `json:"value"`
	Endpoint  *endpoint `json:"endpoint"`
}

type binaryAnnotationV1 struct {
	Key      string      `json:"key"`
	Value    interface{} `json:"value"`
	Endpoint *endpoint   `json:"endpoint"`
}

type endpoint struct {
	ServiceName string `json:"serviceName"`
	// Ipv4        net.IP `json:"ipv4,omitempty"`
	// Ipv6        net.IP `json:"ipv6,omitempty"`
	// Port        uint16 `json:"port"`
}

func ParseJsonV1(input io.Reader) ([]proxy.Span, error) {
	var decoded []spanV1
	if err := json.NewDecoder(input).Decode(&decoded); err != nil {
		return nil, errors.WithMessage(err, "parse spans for json v1")
	}

	parsedSpans := make([]proxy.Span, len(decoded))
	for _, span := range decoded {
		parsedSpans = append(parsedSpans, span.ToSpan())
	}

	return parsedSpans, nil
}

func (span *spanV1) ToSpan() proxy.Span {
	proxySpan := proxy.NewSpan(cache.String(span.Name),
		span.TraceID, span.ID, span.ParentID.OrZero())

	for _, annotation := range span.Annotations {
		proxySpan.AddTiming(annotation.Value, proxy.Microseconds(annotation.Timestamp))

		if annotation.Endpoint != nil && proxySpan.Service == "" {
			proxySpan.Service = annotation.Endpoint.ServiceName
		}
	}

	for _, annotation := range span.BinaryAnnotations {
		proxySpan.AddTag(cache.String(annotation.Key), toStringCached(annotation.Value))

		if annotation.Endpoint != nil && proxySpan.Service == "" {
			proxySpan.Service = annotation.Endpoint.ServiceName
		}
	}

	proxySpan.AddTag("protocolVersion", "json v1")

	if span.Timestamp != nil {
		proxySpan.Timestamp = proxy.Microseconds(*span.Timestamp)
	}

	if span.Duration != nil {
		proxySpan.Duration = time.Duration(*span.Duration) * time.Microsecond
	}

	fillInTimestamp(&proxySpan)

	return proxySpan
}

func fillInTimestamp(proxySpan *proxy.Span) {
	sr := proxySpan.Timings["sr"]
	ss := proxySpan.Timings["ss"]
	if sr > 0 && ss > 0 {
		proxySpan.Timestamp = sr
		proxySpan.Duration = time.Duration(ss - sr)
	}

	cs := proxySpan.Timings["cs"]
	cr := proxySpan.Timings["cr"]
	if cs > 0 && cr > 0 {
		proxySpan.Timestamp = cs
		proxySpan.Duration = time.Duration(cr - cs)
	}

	if proxySpan.Duration == 0 {
		proxySpan.Duration = 1 * time.Millisecond
	}
}
