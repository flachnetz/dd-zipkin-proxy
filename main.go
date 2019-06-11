package zipkinproxy

import (
	"github.com/flachnetz/dd-zipkin-proxy/cache"
	"github.com/flachnetz/dd-zipkin-proxy/datadog"
	"github.com/flachnetz/dd-zipkin-proxy/proxy"
	"github.com/flachnetz/dd-zipkin-proxy/zipkin"
	"github.com/flachnetz/go-admin"
	"github.com/flachnetz/startup"
	"github.com/flachnetz/startup/startup_base"
	"github.com/flachnetz/startup/startup_http"
	"github.com/flachnetz/startup/startup_metrics"
	"github.com/julienschmidt/httprouter"
	"github.com/pkg/profile"
	"github.com/rcrowley/go-metrics"
	"github.com/sirupsen/logrus"
	"net/http"
	"strconv"

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

func MainWithRouting(routing Routing, spanConverter SpanConverter) {
	var opts struct {
		Base    startup_base.BaseOptions       `group:"Base options"`
		HTTP    startup_http.HTTPOptions       `group:"HTTP server options"`
		Metrics startup_metrics.MetricsOptions `group:"Metrics configuration"`

		ProfileCPU bool `long:"profile" description:"Enable CPU profiling"`

		TraceAgent struct {
			Host string `long:"trace-host" default:"localhost" description:"Hostname of the trace agent."`
			Port int    `long:"trace-port" default:"8126" description:"Port of the trace agent."`
		}
	}

	opts.Metrics.Inputs.MetricsPrefix = "zipkin.proxy"

	startup.MustParseCommandLine(&opts)

	cache.RegisterCacheMetrics(metrics.DefaultRegistry)

	if opts.ProfileCPU {
		defer profile.Start().Stop()
	}

	var channels []chan<- proxy.Span

	if true {
		log.Info("Enable forwarding of spans to datadog trace-agent")
		transport := datadog.DefaultTransport(opts.TraceAgent.Host, strconv.Itoa(opts.TraceAgent.Port))

		// accept zipkin spans
		spans := make(chan proxy.Span, 256)
		channels = append(channels, spans)

		go datadog.Sink(transport, spans)
	}

	if false {
		log.Info("Enable forwarding to a zipkin agent")

		// accept zipkin spans
		spans := make(chan proxy.Span, 256)
		channels = append(channels, spans)

		go zipkin.Sink(spans)

	}

	// a channel to store the last spans that were received
	var buffer *SpansBuffer
	{
		spans := make(chan proxy.Span, 256)
		channels = append(channels, spans)

		// just keep references to previous spans.
		buffer = NewSpansBuffer(2048)
		go buffer.ReadFrom(spans)
	}

	// multiplex input channel to all the target channels
	processedSpans := make(chan proxy.Span, 64)
	go forwardSpansToChannels(processedSpans, channels, spanConverter)

	inputSpans := make(chan proxy.Span, 256)
	go ErrorCorrectSpans(inputSpans, processedSpans)

	opts.HTTP.Serve(startup_http.Config{
		Name: "dd-zipkin-proxy",

		AdminHandlers: []admin.RouteConfig{
			admin.Describe("A buffer of the previous traces (in openzipkin-format) in the order they were received.",
				admin.WithGenericValue("/spans", buffer.ToSlice)),
		},

		Routing: func(router *httprouter.Router) http.Handler {
			handleSpans(router, inputSpans)
			return handleGzipRequestBody(routing(router))
		},
	})
}

func forwardSpansToChannels(source <-chan proxy.Span, targets []chan<- proxy.Span, converter SpanConverter) {
	for span := range source {
		converted, err := converter(span)
		if err != nil {
			continue
		}

		for _, target := range targets {
			target <- converted
		}
	}
}
