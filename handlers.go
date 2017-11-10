package zipkinproxy

import (
	"compress/gzip"
	"encoding/json"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/flachnetz/dd-zipkin-proxy/cache"
	"github.com/flachnetz/dd-zipkin-proxy/jsoncodec"
	"github.com/julienschmidt/httprouter"
	"github.com/openzipkin/zipkin-go-opentracing/thrift/gen-go/zipkincore"
	"github.com/pkg/errors"
	"github.com/rcrowley/go-metrics"
	"io"
	"net/http"
)

func handleSpans(spans chan<- *zipkincore.Span) httprouter.Handle {
	return func(w http.ResponseWriter, req *http.Request, params httprouter.Params) {
		var bodyReader io.Reader = req.Body

		// decode body on the fly if it comes compressed
		if req.Header.Get("Content-Encoding") == "gzip" {
			var err error
			bodyReader, err = gzip.NewReader(bodyReader)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
		}

		// parse with correct mime type
		var err error
		if req.Header.Get("Content-Type") == "application/json" {
			metrics.GetOrRegisterTimer("spans.receive[type:json]", Metrics).Time(func() {
				err = parseSpansWithJSON(spans, bodyReader)
			})
		} else {
			metrics.GetOrRegisterTimer("spans.receive[type:thrift]", Metrics).Time(func() {
				err = parseSpansWithThrift(spans, bodyReader)
			})
		}

		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)

			log.WithField("prefix", "parser").Warnf("Request failed with error: %s", err)

		} else {
			w.WriteHeader(http.StatusNoContent)
		}
	}
}

func parseSpansWithJSON(spansChannel chan<- *zipkincore.Span, body io.Reader) error {
	parsedSpans := []jsoncodec.Span{}
	if err := json.NewDecoder(body).Decode(&parsedSpans); err != nil {
		return errors.WithMessage(err, "Could not parse list of spans from json")
	}

	// now convert to zipkin spans
	for idx := range parsedSpans {
		spansChannel <- parsedSpans[idx].ToZipkincoreSpan()
	}

	spanCount := int64(len(parsedSpans))
	metrics.GetOrRegisterMeter("spans.parsed[type:json]", Metrics).Mark(spanCount)

	return nil
}

func parseSpansWithThrift(spansChannel chan<- *zipkincore.Span, body io.Reader) error {
	protocol := cache.CachingProtocol{
		TBinaryProtocol: thrift.NewTBinaryProtocolTransport(thrift.NewStreamTransportR(body))}

	_, size, err := protocol.ReadListBegin()
	if err != nil {
		return errors.WithMessage(err, "Expect begin of list")
	}

	if size <= 0 || size > 32*1024 {
		return errors.Errorf("Too many spans, handler will not try to read %d spans", size)
	}

	// allocate all spans at once in one big block of memory.
	spans := make([]zipkincore.Span, size)

	for idx := 0; idx < size; idx++ {
		if err := spans[idx].Read(protocol); err != nil {
			return errors.WithMessage(err, "Could not read thrift encoded span")
		}

		spansChannel <- &spans[idx]
	}

	metrics.GetOrRegisterMeter("spans.parsed[type:thrift]", Metrics).Mark(int64(size))

	return errors.WithMessage(protocol.ReadListEnd(), "Could not read end of list")
}
