package zipkinproxy

import (
	"github.com/flachnetz/dd-zipkin-proxy/datadog"
	"github.com/flachnetz/dd-zipkin-proxy/proxy"
	"github.com/flachnetz/go-admin"
	"github.com/flachnetz/startup"
	"github.com/flachnetz/startup/startup_base"
	"github.com/flachnetz/startup/startup_http"
	"github.com/flachnetz/startup/startup_metrics"
	"github.com/julienschmidt/httprouter"
	"github.com/pkg/profile"
	"github.com/sirupsen/logrus"
	"net/http"

	_ "github.com/apache/thrift/lib/go/thrift"
)

var log = logrus.WithField("prefix", "main")

type SpanConverter func(span proxy.Span) (proxy.Span, error)
type Routing func(*httprouter.Router) http.Handler

func Main(spanConverter SpanConverter) {
	routing := func(router *httprouter.Router) http.Handler {
		return router
	}

	MainWithRouting(routing, spanConverter)
}

func MainWithRouting(routing Routing,  spanConverter SpanConverter) {
	var opts struct {
		Base    startup_base.BaseOptions       `group:"Base options"`
		HTTP    startup_http.HTTPOptions       `group:"HTTP server options"`
		Metrics startup_metrics.MetricsOptions `group:"Metrics configuration"`

		DisableSpanCorrection bool `long:"disable-span-correction" description:"Do not to correct the spans."`

		TraceAgent struct {
			Host string `long:"trace-host" value:"localhost" description:"Hostname of the trace agent."`
			Port string `long:"trace-port" value:"8126" description:"Port of the trace agent."`
		}
	}

	defer profile.Start().Stop()

	opts.Metrics.Inputs.MetricsPrefix = "zipkin.proxy"

	startup.MustParseCommandLine(&opts)

	var channels []chan<- proxy.Span

	{
		log.Info("Enable forwarding of spans to datadog trace-agent")
		datadog.Initialize(opts.TraceAgent.Host + ":" + opts.TraceAgent.Port)

		// accept zipkin spans
		spans := make(chan proxy.Span, 1024)
		channels = append(channels, spans)

		go datadog.Sink(spans)
	}

	// a channel to store the last spans that were received
	var buffer *SpansBuffer
	{
		zipkinSpans := make(chan proxy.Span, 1024)
		channels = append(channels, zipkinSpans)

		// just keep references to previous spans.
		buffer = NewSpansBuffer(2048)
		go buffer.ReadFrom(zipkinSpans)
	}

	// multiplex input channel to all the target channels
	proxySpans := make(chan proxy.Span, 16)
	go forwardSpansToChannels(proxySpans, channels, spanConverter)

	originalZipkinSpans := make(chan proxy.Span, 1024)
	if opts.DisableSpanCorrection {
		originalZipkinSpans = proxySpans
	} else {
		// do error correction for spans
		go ErrorCorrectSpans(originalZipkinSpans, proxySpans)
	}

	opts.HTTP.Serve(startup_http.Config{
		Name: "dd-zipkin-proxy",

		AdminHandlers: []admin.RouteConfig{
			admin.Describe("A buffer of the previous traces (in openzipkin-format) in the order they were received.",
				admin.WithGenericValue("/spans", buffer.ToSlice)),
		},

		Routing: func(router *httprouter.Router) http.Handler {
			// we emulate the zipkin api
			router.POST("/api/v1/spans", handleSpans(originalZipkinSpans, 1))
			router.POST("/api/v2/spans", handleSpans(originalZipkinSpans, 2))

			return routing(router)
		},
	})
}

func forwardSpansToChannels(source <-chan proxy.Span, targets []chan<- proxy.Span, converter SpanConverter) {
	for span := range source {
		converted, err := converter(span)
		if err == nil {
			for _, target := range targets {
				target <- converted
			}
		}
	}
}
