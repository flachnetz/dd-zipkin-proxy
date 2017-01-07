package zipkinproxy

import (
	"bytes"
	"compress/gzip"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/flachnetz/dd-zipkin-proxy/datadog"
	"github.com/flachnetz/dd-zipkin-proxy/zipkin"
	"github.com/gorilla/handlers"
	"github.com/jessevdk/go-flags"
	"github.com/julienschmidt/httprouter"
	"github.com/openzipkin/zipkin-go-opentracing"
	"github.com/openzipkin/zipkin-go-opentracing/_thrift/gen-go/zipkincore"

	. "github.com/flachnetz/go-admin"
)

var log = logrus.WithField("prefix", "main")

func Main(spanConverter datadog.SpanConverterFunc) {
	var opts struct {
		ZipkinReporterUrl string `long:"zipkin-url" description:"Url for zipkin reporting."`
		DatadogReporting  bool   `long:"datadog-reporting" description:"Enable datadog trace reporting with the default url."`
		ListenAddr        string `long:"listen-address" default:":9411" description:"Address to listen for zipkin connections."`
		Verbose           bool   `long:"verbose" description:"Enable verbose debug logging."`
	}

	if _, err := flags.Parse(&opts); err != nil {
		os.Exit(1)
	}

	if opts.Verbose {
		logrus.SetLevel(logrus.DebugLevel)
	}

	var channels []chan<- *zipkincore.Span

	if opts.DatadogReporting {
		// report spans to datadog
		zipkinSpans := make(chan *zipkincore.Span, 512)
		datadogSpans := datadog.ConvertZipkinSpans(zipkinSpans, spanConverter)
		go datadog.ReportSpans(datadogSpans)
		channels = append(channels, zipkinSpans)
	}

	if opts.ZipkinReporterUrl != "" {
		log.Info("Enable zipkin reporting at url ", opts.ZipkinReporterUrl)

		collector, err := zipkintracer.NewHTTPCollector(opts.ZipkinReporterUrl,
			zipkintracer.HTTPBatchSize(5000),
			zipkintracer.HTTPLogger(funcLogger(log.Warn)))

		if err != nil {
			log.Fatal("Could not create zipkin http collector: ", err)
			return
		}

		zipkinSpans := make(chan *zipkincore.Span, 512)
		go zipkin.ReportSpans(collector, zipkinSpans)
		channels = append(channels, zipkinSpans)
	}

	// multiplex channels
	zipkinSpans := make(chan *zipkincore.Span, 16)
	go forwardSpansToChannels(zipkinSpans, channels)

	admin := NewAdminHandler("/admin", "dd-zipkin-proxy",
		WithDefaults(),
		WithForceGC(),
		WithPProfHandlers(),
		WithHeapDump())

	// listen for zipkin messages
	router := httprouter.New()
	registerAdminHandler(router, admin)
	router.POST("/api/v1/spans", handleSpans(zipkinSpans))
	log.Fatal(http.ListenAndServe(opts.ListenAddr, handlers.LoggingHandler(log.Logger.Writer(), router)))
}

func registerAdminHandler(router *httprouter.Router, handler http.Handler) {
	router.Handler("GET", "/", http.RedirectHandler("/admin", http.StatusTemporaryRedirect))

	methods := []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"}
	for _, method := range methods {
		router.Handler(method, "/admin", handler)
		router.Handler(method, "/admin/*path", handler)
	}
}

func forwardSpansToChannels(source <-chan *zipkincore.Span, targets []chan<- *zipkincore.Span) {
	dropped := 0
	lastLogMessageTime := time.Now()

	for span := range source {
		for _, target := range targets {
			// send span, do not block if target is full
			select {
			case target <- span:
			default:
				dropped += 1
			}
		}

		if dropped > 0 && time.Now().Sub(lastLogMessageTime) > 1*time.Second {
			log.Warnf("Dropped %d spans because a target buffer was full.", dropped)
			dropped = 0
			lastLogMessageTime = time.Now()
		}
	}
}

func handleSpans(spans chan<- *zipkincore.Span) httprouter.Handle {
	return func(w http.ResponseWriter, req *http.Request, params httprouter.Params) {
		var bodyReader io.Reader = req.Body
		if req.Header.Get("Content-Encoding") == "gzip" {
			var err error
			bodyReader, err = gzip.NewReader(bodyReader)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
		}

		body, err := ioutil.ReadAll(bodyReader)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		transport := thrift.NewStreamTransportR(bytes.NewReader(body))
		protocol := thrift.NewTBinaryProtocolTransport(transport)

		_, size, err := protocol.ReadListBegin()
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		for idx := 0; idx < size; idx++ {
			var span zipkincore.Span

			if err := span.Read(protocol); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			if span.Name == "watch-config-key-values" {
				continue
			}

			spans <- &span
		}

		if err := protocol.ReadListEnd(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	}
}

type funcLogger func(keyvals ...interface{})

func (fn funcLogger) Log(keyvals ...interface{}) error {
	fn(keyvals...)
	return nil
}
