package main

import (
	zipkinproxy "github.com/flachnetz/dd-zipkin-proxy"
	"github.com/flachnetz/dd-zipkin-proxy/datadog"
	"github.com/flachnetz/dd-zipkin-proxy/proxy"
)

func main() {
	datadog.Initialize("127.0.0.1:8126")

	zipkinproxy.Main(func(span proxy.Span) (proxy.Span, error) {
		return span, nil
	})
}
