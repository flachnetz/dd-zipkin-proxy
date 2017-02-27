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

	"encoding/json"
	"github.com/flachnetz/dd-zipkin-proxy/jsoncodec"
	. "github.com/flachnetz/go-admin"
	"sync"
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

	// a channel to store the last spans that were received
	var buffer *SpansBuffer
	{
		zipkinSpans := make(chan *zipkincore.Span, 512)
		buffer = NewSpansBuffer(2048)
		go buffer.ReadFrom(zipkinSpans)
		channels = append(channels, zipkinSpans)
	}

	// multiplex channels
	zipkinSpans := make(chan *zipkincore.Span, 16)
	go forwardSpansToChannels(zipkinSpans, channels)

	// do error correction for spans
	originalZipkinSpans := make(chan *zipkincore.Span, 1024)
	go ErrorCorrectSpans(originalZipkinSpans, zipkinSpans)

	admin := NewAdminHandler("/admin", "dd-zipkin-proxy",
		WithDefaults(),
		WithForceGC(),
		WithPProfHandlers(),
		WithHeapDump(),

		Describe("A buffer of the previous traces (in openzipkin-format) in the order they were received.",
			WithGenericValue("/spans", buffer.ToSlice)))

	// listen for zipkin messages
	router := httprouter.New()
	registerAdminHandler(router, admin)
	router.POST("/api/v1/spans", handleSpans(originalZipkinSpans))
	log.Fatal(http.ListenAndServe(opts.ListenAddr, handlers.LoggingHandler(log.Logger.Writer(), router)))
}

func registerAdminHandler(router *httprouter.Router, handler http.Handler) {
	router.Handler("GET", "/", http.RedirectHandler("/admin", http.StatusTemporaryRedirect))

	methods := []string{"GET", "POST", "PUT", "DELETE", "OPTIONS", "HEAD"}
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
			//select {
			//case target <- span:
			//default:
			//	dropped += 1
			//}

			target <- span
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

		// parse with correct mime type
		if req.Header.Get("Content-Type") == "application/json" {
			err = parseSpansWithJSON(spans, body)
		} else {
			err = parseSpansWithThrift(spans, body)
		}

		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
		} else {
			w.WriteHeader(http.StatusNoContent)
		}
	}
}

func parseSpansWithJSON(spansChannel chan<- *zipkincore.Span, body []byte) error {
	parsedSpans := []jsoncodec.Span{}
	if err := json.Unmarshal(body, &parsedSpans); err != nil {
		return err
	}

	// now convert to zipkin spans
	for idx := range parsedSpans {
		spansChannel <- parsedSpans[idx].ToZipkincoreSpan()
	}

	return nil
}

func parseSpansWithThrift(spansChannel chan<- *zipkincore.Span, body []byte) error {
	transport := thrift.NewStreamTransportR(bytes.NewReader(body))
	protocol := thrift.NewTBinaryProtocolTransport(transport)

	_, size, err := protocol.ReadListBegin()
	if err != nil {
		return err
	}

	for idx := 0; idx < size; idx++ {
		var span zipkincore.Span
		if err := span.Read(protocol); err != nil {
			return err
		}

		spansChannel <- &span
	}

	return protocol.ReadListEnd()
}

type funcLogger func(keyvals ...interface{})

func (fn funcLogger) Log(keyvals ...interface{}) error {
	fn(keyvals...)
	return nil
}

type SpansBuffer struct {
	lock     sync.Mutex
	position uint
	spans    []*zipkincore.Span
}

func NewSpansBuffer(capacity uint) *SpansBuffer {
	spans := make([]*zipkincore.Span, capacity)
	return &SpansBuffer{spans: spans}
}

func (buffer *SpansBuffer) ReadFrom(spans <-chan *zipkincore.Span) {
	for span := range spans {
		buffer.lock.Lock()
		buffer.spans[buffer.position] = span
		buffer.position = (buffer.position + 1) % uint(len(buffer.spans))
		buffer.lock.Unlock()
	}
}

func (buffer *SpansBuffer) ToSlice() []*zipkincore.Span {
	buffer.lock.Lock()
	defer buffer.lock.Unlock()

	var result []*zipkincore.Span
	for _, span := range buffer.spans {
		if span != nil {
			result = append(result, span)
		}
	}

	return result
}
