package zipkinproxy

import (
	"compress/gzip"
	"github.com/flachnetz/dd-zipkin-proxy/codec"
	"github.com/flachnetz/dd-zipkin-proxy/proxy"
	"github.com/julienschmidt/httprouter"
	"github.com/pkg/errors"
	"github.com/rcrowley/go-metrics"
	"io"
	"net/http"
	"strings"
)

func handleSpans(spans chan<- proxy.Span, version int) httprouter.Handle {
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
		if strings.Contains(req.Header.Get("Content-Type"), "application/json") {
			metrics.GetOrRegisterTimer("spans.receive[type:json]", nil).Time(func() {
				err = parseSpansWithJSON(spans, bodyReader, version)
			})
		} else {
			metrics.GetOrRegisterTimer("spans.receive[type:thrift]", nil).Time(func() {
				err = parseSpansWithThrift(spans, bodyReader, version)
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

func parseSpansWithThrift(spansChannel chan<- proxy.Span, body io.Reader, version int) error {
	if version != 1 {
		return errors.New("can only parse thrift v1")
	}

	parsedSpans, err := codec.ParseThriftV1(body)
	if err != nil {
		return errors.WithMessage(err, "parse body as thrift")
	}

	for _, span := range parsedSpans {
		spansChannel <- span
	}

	size := int64(len(parsedSpans))
	metrics.GetOrRegisterMeter("spans.parsed[type:thrift]", nil).Mark(size)

	return nil
}

func parseSpansWithJSON(spansChannel chan<- proxy.Span, body io.Reader, version int) error {
	var parsedSpans []proxy.Span

	var err error
	switch version {
	case 1:
		parsedSpans, err = codec.ParseJsonV1(body)

	case 2:
		parsedSpans, err = codec.ParseJsonV2(body)

	default:
		return errors.New("invalid version code")
	}

	if err != nil {
		return errors.WithMessage(err, "parsing spans from json")
	}

	for _, span := range parsedSpans {
		spansChannel <- span
	}

	spanCount := int64(len(parsedSpans))
	metrics.GetOrRegisterMeter("spans.parsed[type:json]", nil).Mark(spanCount)

	return nil
}
