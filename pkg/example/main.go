package main

import (
	zipkinproxy "github.com/flachnetz/dd-zipkin-proxy"
	"github.com/flachnetz/dd-zipkin-proxy/datadog"
	"github.com/openzipkin-contrib/zipkin-go-opentracing/thrift/gen-go/zipkincore"
)

func main() {
	datadog.Initialize("127.0.0.1:8126")

	zipkinproxy.Main(func(span *zipkincore.Span) *zipkincore.Span {
		return span
	})
}
