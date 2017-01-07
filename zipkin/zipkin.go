package zipkin

import (
	"github.com/openzipkin/zipkin-go-opentracing"
	"github.com/openzipkin/zipkin-go-opentracing/_thrift/gen-go/zipkincore"
)

func ReportSpans(collector zipkintracer.Collector, spans <-chan *zipkincore.Span) {
	for span := range spans {
		collector.Collect(span)
	}
}
