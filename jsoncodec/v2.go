package jsoncodec

import (
	"github.com/flachnetz/dd-zipkin-proxy/cache"
	"github.com/openzipkin-contrib/zipkin-go-opentracing/thrift/gen-go/zipkincore"
)

type SpanV2 struct {
	TraceID  Id  `json:"traceId"`
	ID       Id  `json:"id"`
	ParentID *Id `json:"parentId"`

	Name string `json:"name"`

	Endpoint *Endpoint `json:"localEndpoint"`

	Tags map[string]string `json:"tags"`

	Kind      string `json:"kind"`
	Timestamp int64  `json:"timestamp"`
	Duration  int64  `json:"duration"`
}

func (span *SpanV2) ToZipkincoreSpan() *zipkincore.Span {
	var annotations []*zipkincore.Annotation

	endpoint := endpointToZipkin(span.Endpoint)

	if len(span.Tags) == 0 {
		span.Tags = map[string]string{}
		span.Tags["dd.name"] = span.Name
	}

	var binaryAnnotations []*zipkincore.BinaryAnnotation
	for key, value := range span.Tags {
		binaryAnnotations = append(binaryAnnotations, &zipkincore.BinaryAnnotation{
			Key:            cache.String(key),
			Value:          toBytesCached(value),
			Host:           endpoint,
			AnnotationType: zipkincore.AnnotationType_STRING,
		})
	}

	// in root spans the traceId equals the span id.
	parentId := span.ParentID
	if span.TraceID == span.ID {
		parentId = nil
	}

	times := [2]int64{span.Timestamp, span.Duration}

	return &zipkincore.Span{
		TraceID: int64(span.TraceID),
		ID:      int64(span.ID),
		Name:    cache.String(span.Name),

		ParentID: (*int64)(parentId),

		Annotations:       annotations,
		BinaryAnnotations: binaryAnnotations,

		Timestamp: &times[0],
		Duration:  &times[1],
	}
}
