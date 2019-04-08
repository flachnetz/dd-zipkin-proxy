package zipkinproxy

import (
	"net/http"
	"os"
	"time"

	"github.com/flachnetz/dd-zipkin-proxy/datadog"
	"github.com/flachnetz/dd-zipkin-proxy/zipkin"
	"github.com/gorilla/handlers"
	"github.com/jessevdk/go-flags"
	"github.com/julienschmidt/httprouter"
	"github.com/openzipkin-contrib/zipkin-go-opentracing"
	"github.com/openzipkin-contrib/zipkin-go-opentracing/thrift/gen-go/zipkincore"
	"github.com/sirupsen/logrus"

	datadog2 "github.com/eSailors/go-datadog"
	"github.com/flachnetz/dd-zipkin-proxy/cache"
	. "github.com/flachnetz/go-admin"
	"github.com/rcrowley/go-metrics"
	"github.com/x-cray/logrus-prefixed-formatter"
	"gopkg.in/tylerb/graceful.v1"
	"regexp"
	"strings"

	_ "github.com/apache/thrift/lib/go/thrift"
)

var log = logrus.WithField("prefix", "main")

var Metrics = metrics.NewPrefixedRegistry("zipkin.proxy.")

type SpanConverter func(span *zipkincore.Span) *zipkincore.Span

func Main(spanConverter SpanConverter) {
	var opts struct {
		ZipkinReporterUrl     string `long:"zipkin-url" value-name:"URL" description:"Url for zipkin reporting."`
		DatadogReporting      bool   `long:"datadog-reporting" description:"Enable datadog trace reporting with the default url."`
		ListenAddr            string `long:"listen-address" value-name:"ADDR" default:":9411" description:"Address to listen for zipkin connections."`
		DisableSpanCorrection bool   `long:"disable-span-correction" description:"Do not to correct the spans."`

		TraceAgent struct {
			Host string `long:"trace-host" value:"localhost" description:"Hostname of the trace agent."`
			Port string `long:"trace-port" value:"8126" description:"Port of the trace agent."`
		}

		Metrics struct {
			DatadogApiKey string `long:"dd-apikey" value-name:"KEY" description:"Provide the datadog api key to enable datadog metrics reporting."`
			DatadogTags   string `long:"dd-tags" value-name:"TAGS" description:"Comma separated list of tags to add the datadog metrics."`
		} `namespace:"metrics" group:"Metrics configuration"`

		Verbose bool `long:"verbose" description:"Enable verbose debug logging."`
	}

	// parse options with a "-" as separator.
	parser := flags.NewParser(&opts, flags.Default)
	parser.NamespaceDelimiter = "-"
	configureEnvironmentVariables(parser.Group)
	if _, err := parser.Parse(); err != nil {
		os.Exit(1)
	}

	// enable prefix logger for logrus
	logrus.SetFormatter(&prefixed.TextFormatter{FullTimestamp: true, ForceFormatting: true})

	if opts.Verbose {
		logrus.SetLevel(logrus.DebugLevel)
	}

	initializeMetrics()

	if opts.Metrics.DatadogApiKey != "" {
		// and metrics reporting
		tags := strings.FieldsFunc(opts.Metrics.DatadogTags, isComma)
		initializeDatadogReporting(opts.Metrics.DatadogApiKey, tags)
	}

	var channels []chan<- *zipkincore.Span

	if opts.DatadogReporting {
		log.Info("Enable forwarding of spans to datadog trace-agent")

		// accept zipkin spans
		zipkinSpans := make(chan *zipkincore.Span, 1024)
		channels = append(channels, zipkinSpans)

		// send out traces to datadog
		go datadog.Sink(zipkinSpans)
	}

	if opts.ZipkinReporterUrl != "" {
		log.Info("Enable zipkin reporting at url ", opts.ZipkinReporterUrl)

		collector, err := zipkintracer.NewHTTPCollector(opts.ZipkinReporterUrl,
			zipkintracer.HTTPBatchSize(5000),
			zipkintracer.HTTPLogger(funcLogger(logrus.WithField("prefix", "zipkin").Warn)))

		if err != nil {
			log.Fatal("Could not create zipkin http collector: ", err)
			return
		}

		// accept zipkin spans
		zipkinSpans := make(chan *zipkincore.Span, 1024)
		channels = append(channels, zipkinSpans)

		// forward them to another zipkin service.
		go zipkin.ReportSpans(collector, zipkinSpans)
	}

	// a channel to store the last spans that were received
	var buffer *SpansBuffer
	{
		zipkinSpans := make(chan *zipkincore.Span, 1024)
		channels = append(channels, zipkinSpans)

		// just keep references to previous spans.
		buffer = NewSpansBuffer(2048)
		go buffer.ReadFrom(zipkinSpans)
	}

	// multiplex input channel to all the target channels
	zipkinSpans := make(chan *zipkincore.Span, 16)
	go forwardSpansToChannels(zipkinSpans, channels, spanConverter)

	originalZipkinSpans := make(chan *zipkincore.Span, 1024)
	if opts.DisableSpanCorrection {
		go PipeThroughSpans(originalZipkinSpans, zipkinSpans)
	} else {
		// do error correction for spans
		go ErrorCorrectSpans(originalZipkinSpans, zipkinSpans)
	}

	// show a nice little admin page with information and stuff.
	admin := NewAdminHandler("/admin", "dd-zipkin-proxy",
		WithDefaults(),
		WithForceGC(),
		WithPProfHandlers(),
		WithHeapDump(),

		WithMetrics(Metrics),

		Describe("A buffer of the previous traces (in openzipkin-format) in the order they were received.",
			WithGenericValue("/spans", buffer.ToSlice)))

	router := httprouter.New()
	registerAdminHandler(router, admin)

	// we emulate the zipkin api
	router.POST("/api/v1/spans", handleSpans(originalZipkinSpans, 1))
	router.POST("/api/v2/spans", handleSpans(originalZipkinSpans, 2))

	// listen for zipkin messages on http api
	if err := httpListen(opts.ListenAddr, router); err != nil {
		log.Errorf("Could not start http server: %s", err)
		return
	}
}

func configureEnvironmentVariables(group *flags.Group) {
	for _, gr := range group.Groups() {
		configureEnvironmentVariables(gr)
	}

	expr := regexp.MustCompile("[^A-Z0-9]+")
	for _, op := range group.Options() {

		name := strings.ToUpper(op.LongNameWithNamespace())
		op.EnvDefaultKey = "DDZK_" + expr.ReplaceAllString(name, "_")
	}
}

func httpListen(addr string, handler http.Handler) error {
	// add logging to requests
	handler = handlers.LoggingHandler(logrus.
		WithField("prefix", "httpd").
		WriterLevel(logrus.DebugLevel), handler)

	// catch and log panics from the http handlers
	handler = handlers.RecoveryHandler(
		handlers.PrintRecoveryStack(true),
		handlers.RecoveryLogger(logrus.WithField("prefix", "httpd")),
	)(handler)

	log.Infof("Starting http server on %s now.", addr)
	server := &graceful.Server{
		Timeout: 10 * time.Second,
		LogFunc: logrus.WithField("prefix", "httpd").Warnf,
		Server: &http.Server{
			Addr:    addr,
			Handler: handler,
		},
	}

	return server.ListenAndServe()
}

func registerAdminHandler(router *httprouter.Router, handler http.Handler) {
	router.Handler("GET", "/", http.RedirectHandler("/admin", http.StatusTemporaryRedirect))

	methods := []string{"GET", "POST", "PUT", "DELETE", "OPTIONS", "HEAD"}
	for _, method := range methods {
		router.Handler(method, "/admin", handler)
		router.Handler(method, "/admin/*path", handler)
	}
}

func forwardSpansToChannels(source <-chan *zipkincore.Span, targets []chan<- *zipkincore.Span, converter SpanConverter) {
	for span := range source {
		for _, target := range targets {
			target <- converter(span)
		}
	}
}

func initializeMetrics() {
	metrics.RegisterRuntimeMemStats(Metrics)
	go metrics.CaptureRuntimeMemStats(Metrics, 10*time.Second)

	cache.RegisterCacheMetrics(Metrics)
}

// Start metrics reporting to datadog. This starts a reporter that sends the
// applications metrics once per minute to datadog if you provide a valid api key.
func initializeDatadogReporting(apikey string, tags []string) {
	if apikey != "" {
		hostname := os.Getenv("HOSTNAME")
		if hostname == "" {
			hostname, _ = os.Hostname()
		}

		log.Debugf("Starting datadog reporting on hostname '%s' with tags: %s",
			hostname, strings.Join(tags, ", "))

		client := datadog2.New(hostname, apikey)
		go datadog2.Reporter(client, Metrics, tags).Start(1 * time.Minute)
	}
}

// Returns true if the given rune is equal to a comma.
func isComma(ch rune) bool {
	return ch == ','
}

type funcLogger func(keyvals ...interface{})

func (fn funcLogger) Log(keyvals ...interface{}) error {
	fn(keyvals...)
	return nil
}

type noopResponseWriter struct{}

func (noopResponseWriter) Header() http.Header {
	return make(http.Header)
}

func (noopResponseWriter) Write(b []byte) (int, error) {
	return len(b), nil
}

func (noopResponseWriter) WriteHeader(int) {
}
