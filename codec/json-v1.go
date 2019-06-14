package codec

import (
	"github.com/flachnetz/dd-zipkin-proxy/proxy"
	"github.com/pkg/errors"
	"io"
	"time"
)

type spanV1 struct {
	TraceID  Id `json:"traceId"`
	ID       Id `json:"id"`
	ParentID Id `json:"parentId"`

	Annotations       [4]annotationV1      `json:"annotations"`
	BinaryAnnotations []binaryAnnotationV1 `json:"binaryAnnotations"`

	Name string `json:"name"`

	Timestamp int64 `json:"timestamp"`
	Duration  int64 `json:"duration"`
}

type annotationV1 struct {
	Timestamp int64    `json:"timestamp"`
	Value     string   `json:"value"`
	Endpoint  endpoint `json:"endpoint"`
}

type binaryAnnotationV1 struct {
	Key      string   `json:"key"`
	Value    string   `json:"value"`
	Endpoint endpoint `json:"endpoint"`
}

type endpoint struct {
	ServiceName string `json:"serviceName"`
}

func ParseJsonV1(input io.Reader) ([]proxy.Span, error) {
	var decoded []spanV1

	if err := jsonConfig.NewDecoder(input).Decode(&decoded); err != nil {
		return nil, errors.WithMessage(err, "parse spans for json v1")
	}

	parsedSpans := make([]proxy.Span, len(decoded))
	for idx, span := range decoded {
		parsedSpans[idx] = span.ToSpan()
	}

	return parsedSpans, nil
}

func (span *spanV1) ToSpan() proxy.Span {
	proxySpan := proxy.NewSpan(span.Name, span.TraceID, span.ID, span.ParentID)

	for _, annotation := range span.Annotations {
		if annotation.Timestamp == 0 {
			continue
		}

		proxySpan.AddTiming(annotation.Value,
			proxy.Microseconds(annotation.Timestamp))

		if proxySpan.Service == "" && annotation.Endpoint.ServiceName != "" {
			proxySpan.Service = annotation.Endpoint.ServiceName
		}
	}

	proxySpan.Tags = make(map[string]string, 1+len(span.BinaryAnnotations))

	for _, annotation := range span.BinaryAnnotations {
		proxySpan.AddTag(annotation.Key, annotation.Value)

		if proxySpan.Service == "" && annotation.Endpoint.ServiceName != "" {
			proxySpan.Service = annotation.Endpoint.ServiceName
		}
	}

	proxySpan.AddTag(tagProtocolVersion, tagJsonV1)

	if span.Timestamp != 0 {
		proxySpan.Timestamp = proxy.Microseconds(span.Timestamp)
	}

	if span.Duration != 0 {
		proxySpan.Duration = time.Duration(span.Duration) * time.Microsecond
	}

	fillInTimestamp(&proxySpan)

	return proxySpan
}

func fillInTimestamp(proxySpan *proxy.Span) {
	sr := proxySpan.Timings.SR
	ss := proxySpan.Timings.SS
	if sr > 0 && ss > 0 {
		proxySpan.Timestamp = sr
		proxySpan.Duration = time.Duration(ss - sr)
	}

	cs := proxySpan.Timings.CS
	cr := proxySpan.Timings.CR
	if cs > 0 && cr > 0 {
		proxySpan.Timestamp = cs
		proxySpan.Duration = time.Duration(cr - cs)
	}

	if proxySpan.Duration == 0 {
		proxySpan.Duration = 1 * time.Millisecond
	}
}
