package main

import (
	zipkinproxy "github.com/flachnetz/dd-zipkin-proxy"
	"github.com/flachnetz/dd-zipkin-proxy/proxy"
)

func main() {
	zipkinproxy.Main(func(span proxy.Span) (proxy.Span, error) {
		return span, nil
	})
}
