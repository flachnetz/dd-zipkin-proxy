package codec

import (
	"github.com/flachnetz/dd-zipkin-proxy/codec/hyperjson"
	"github.com/flachnetz/dd-zipkin-proxy/proxy"
	"github.com/modern-go/reflect2"
	"github.com/pkg/errors"
	"io"
	"sync"
	"time"
	"unsafe"
)

type spanV1 struct {
	TraceID  Id `json:"traceId"`
	ID       Id `json:"id"`
	ParentID Id `json:"parentId"`

	Annotations       [4]annotationV1      `json:"annotations"`
	BinaryAnnotations []binaryAnnotationV1 `json:"binaryAnnotations"`

	Name string `json:"name"`

	Timestamp uint64 `json:"timestamp"`
	Duration  uint64 `json:"duration"`
}

type annotationV1 struct {
	Timestamp uint64   `json:"timestamp"`
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

var poolSpansV1 = sync.Pool{
	New: func() interface{} {
		return make([]spanV1, 64)
	},
}

func ParseJsonV1(input io.Reader) ([]proxy.Span, error) {
	// get a buffer to re-use
	buf := bufferPool.Get()
	defer bufferPool.Put(buf)

	// get a new reader and initialize it with the pooled buffer.
	r := hyperjson.NewWithReader(input, buf.([]byte))

	// we know, that the reader wont escape, sadly, go doesnt know that,
	// so we give it a little hint
	p := (*hyperjson.Parser)(reflect2.NoEscape(unsafe.Pointer(r)))

	// we can also re-use the slice we use for decoding
	ifSlice := poolSpansV1.Get()
	defer poolSpansV1.Put(ifSlice)

	// decode spans into the span slice.
	decoded := ifSlice.([]spanV1)[:0]

	if err := decoderSliceSpanV1(reflect2.NoEscape(unsafe.Pointer(&decoded)), p); err != nil {
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
			proxy.Microseconds(int64(annotation.Timestamp)))

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
		proxySpan.Timestamp = proxy.Microseconds(int64(span.Timestamp))
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

func spanV1ValueDecoder() hyperjson.ValueDecoder {
	decoder := hyperjson.MakeStructDecoder([]hyperjson.Field{
		{
			JsonName: "id",
			Offset:   hyperjson.OffsetOf(spanV1{}, "ID"),
			Decoder:  idValueDecoder,
		},
		{
			JsonName: "traceId",
			Offset:   hyperjson.OffsetOf(spanV1{}, "TraceID"),
			Decoder:  idValueDecoder,
		},
		{
			JsonName: "parentId",
			Offset:   hyperjson.OffsetOf(spanV1{}, "ParentID"),
			Decoder:  idValueDecoder,
		},
		{
			JsonName: "timestamp",
			Offset:   hyperjson.OffsetOf(spanV1{}, "Timestamp"),
			Decoder:  hyperjson.Uint64ValueDecoder,
		},
		{
			JsonName: "duration",
			Offset:   hyperjson.OffsetOf(spanV1{}, "Duration"),
			Decoder:  hyperjson.Uint64ValueDecoder,
		},
		{
			JsonName: "name",
			Offset:   hyperjson.OffsetOf(spanV1{}, "Name"),
			Decoder:  hyperjson.StringValueDecoder,
		},
		{
			JsonName: "binaryAnnotations",
			Offset:   hyperjson.OffsetOf(spanV1{}, "BinaryAnnotations"),
			Decoder: hyperjson.MakeSliceDecoder(
				reflect2.TypeOf([]binaryAnnotationV1{}).(reflect2.SliceType),
				hyperjson.MakeStructDecoder([]hyperjson.Field{
					{
						JsonName: "key",
						Offset:   hyperjson.OffsetOf(binaryAnnotationV1{}, "Key"),
						Decoder:  hyperjson.StringValueDecoder,
					},
					{
						JsonName: "value",
						Offset:   hyperjson.OffsetOf(binaryAnnotationV1{}, "Value"),
						Decoder:  anyValueDecoder,
					},
					{
						JsonName: "endpoint",
						Offset:   hyperjson.OffsetOf(binaryAnnotationV1{}, "Endpoint"),
						Decoder:  endpointValueDecoder,
					},
				})),
		},
		{
			JsonName: "annotations",
			Offset:   hyperjson.OffsetOf(spanV1{}, "Annotations"),
			Decoder: hyperjson.MakeArrayDecoder(
				reflect2.TypeOf([4]annotationV1{}).(reflect2.ArrayType),
				hyperjson.MakeStructDecoder([]hyperjson.Field{
					{
						JsonName: "timestamp",
						Offset:   hyperjson.OffsetOf(annotationV1{}, "Timestamp"),
						Decoder:  hyperjson.Uint64ValueDecoder,
					},
					{
						JsonName: "value",
						Offset:   hyperjson.OffsetOf(annotationV1{}, "Value"),
						Decoder:  hyperjson.StringValueDecoder,
					},
					{
						JsonName: "endpoint",
						Offset:   hyperjson.OffsetOf(annotationV1{}, "Endpoint"),
						Decoder:  endpointValueDecoder,
					},
				})),
		},
	})

	return func(target unsafe.Pointer, p *hyperjson.Parser) error {
		span := (*spanV1)(target)

		span.TraceID = 0
		span.ID = 0
		span.ParentID = 0
		span.Duration = 0
		span.Timestamp = 0
		span.Name = ""

		span.Annotations = [4]annotationV1{}

		for idx := range span.BinaryAnnotations {
			span.BinaryAnnotations[idx] = binaryAnnotationV1{}
		}

		span.BinaryAnnotations = span.BinaryAnnotations[:0]

		return decoder(target, p)
	}
}

var decoderSliceSpanV1 = hyperjson.MakeSliceDecoder(
	reflect2.TypeOf([]spanV1{}).(reflect2.SliceType),
	spanV1ValueDecoder())
