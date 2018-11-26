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

func handleSpans(spans chan<- *zipkincore.Span, version int) httprouter.Handle {
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
				err = parseSpansWithJSON(spans, bodyReader, version)
			})
		} else {
			metrics.GetOrRegisterTimer("spans.receive[type:thrift]", Metrics).Time(func() {
				if version == 1 {
					err = parseSpansWithThrift(spans, bodyReader)
				} else {
					err = errors.New("only supports thrift v1")
				}
			})
		}

		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)

			log.WithField("prefix", "parser").Warnf("Request failed with error: %s", err)

		} else {
			w.WriteHeader(http.StatusAccepted)
		}
	}
}

func parseSpansWithJSON(spansChannel chan<- *zipkincore.Span, body io.Reader, version int) error {
	var parsedSpans []*zipkincore.Span

	switch version {
	case 1:
		var parsedSpansV1 []jsoncodec.SpanV1
		if err := json.NewDecoder(body).Decode(&parsedSpansV1); err != nil {
			return errors.WithMessage(err, "Could not parse list of spans from json")
		}

		for _, span := range parsedSpansV1 {
			parsedSpans = append(parsedSpans, span.ToZipkincoreSpan())
		}

	case 2:
		var parsedSpansV2 []jsoncodec.SpanV2
		if err := json.NewDecoder(body).Decode(&parsedSpansV2); err != nil {
			return errors.WithMessage(err, "Could not parse list of spans from json")
		}

		for _, span := range parsedSpansV2 {
			parsedSpans = append(parsedSpans, span.ToZipkincoreSpan())
		}

	default:
		return errors.New("invalid version code")
	}

	// now convert to zipkin spans
	for _, span := range parsedSpans {
		// log.Debugf("GOT SPAN: %s (id=%x, parent=%x)", span.Name, span.GetID(), span.GetParentID())
		spansChannel <- span
	}

	spanCount := int64(len(parsedSpans))
	metrics.GetOrRegisterMeter("spans.parsed[type:json]", Metrics).Mark(spanCount)

	return nil
}

func parseSpansWithThrift(spansChannel chan<- *zipkincore.Span, body io.Reader) error {
	pb := thrift.NewTBinaryProtocolTransport(thrift.NewStreamTransportR(body))
	protocol := cache.NewProtocol(pb)

	_, size, err := protocol.ReadListBegin()
	if err != nil {
		return errors.WithMessage(err, "Expect begin of list")
	}

	if size <= 0 || size > 32*1024 {
		return errors.Errorf("Too many spans, handler will not try to read %d spans", size)
	}

	for idx := 0; idx < size; idx++ {
		var span zipkincore.Span
		if err := span.Read(protocol); err != nil {
			return errors.WithMessage(err, "Could not read thrift encoded span")
		}

		// log.Debugf("GOT SPAN: %s (id=%x, parent=%x)", span.Name, span.GetID(), span.GetParentID())
		spansChannel <- &span
	}

	metrics.GetOrRegisterMeter("spans.parsed[type:thrift]", Metrics).Mark(int64(size))

	return errors.WithMessage(protocol.ReadListEnd(), "Could not read end of list")
}
