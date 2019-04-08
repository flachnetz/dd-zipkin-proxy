package zipkin

import (
	"github.com/openzipkin-contrib/zipkin-go-opentracing"
	"github.com/openzipkin-contrib/zipkin-go-opentracing/thrift/gen-go/zipkincore"
)

func ReportSpans(collector zipkintracer.Collector, spans <-chan *zipkincore.Span) {
	for span := range spans {
		_ = collector.Collect(span)
	}
}
