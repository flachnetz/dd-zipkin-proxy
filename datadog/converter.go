package datadog

import (
	"github.com/DataDog/dd-trace-go/tracer"
	"github.com/openzipkin/zipkin-go-opentracing/_thrift/gen-go/zipkincore"
)

type SpanConverterFunc func(span *zipkincore.Span) *tracer.Span

// Reads all zipkin spans from the given channel, converts them to datadog spans using the given converter
// and write them into another channel.
func ConvertZipkinSpans(zipkinSpans <-chan *zipkincore.Span, converter SpanConverterFunc) <-chan *tracer.Span {
	datadogSpans := make(chan *tracer.Span, 16)

	go func() {
		defer close(datadogSpans)

		for span := range zipkinSpans {
			datadogSpans <- converter(span)
		}
	}()

	return datadogSpans
}
