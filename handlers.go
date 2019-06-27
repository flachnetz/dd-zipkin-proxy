package zipkinproxy

import (
	"compress/gzip"
	"github.com/flachnetz/dd-zipkin-proxy/codec"
	"github.com/flachnetz/dd-zipkin-proxy/proxy"
	"github.com/julienschmidt/httprouter"
	"github.com/pkg/errors"
	"github.com/rcrowley/go-metrics"
	"io"
	"math/rand"
	"net/http"
	"strings"
)

func handleSpans(r *httprouter.Router, spans chan<- proxy.Span) {
	r.POST("/api/v1/spans", func(writer http.ResponseWriter, req *http.Request, params httprouter.Params) {
		var err error
		if strings.Contains(req.Header.Get("Content-Type"), "application/json") {
			metrics.GetOrRegisterTimer("spans.receive[type:json-v1]", nil).Time(func() {
				err = parseSpansWithJSON(spans, req.Body, codec.ParseJsonV1)
			})
		} else {
			metrics.GetOrRegisterTimer("spans.receive[type:thrift]", nil).Time(func() {
				err = parseSpansWithThrift(spans, req.Body)
			})
		}

		if rand.Float64() < 0.01 {
			// dont do keep the connection alive in 1% of the cases
			writer.Header().Set("Connection", "close")
		}

		if err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)
		} else {
			writer.WriteHeader(http.StatusAccepted)
		}
	})

	r.POST("/api/v2/spans", func(writer http.ResponseWriter, req *http.Request, params httprouter.Params) {
		var err error
		metrics.GetOrRegisterTimer("spans.receive[type:json-v2]", nil).Time(func() {
			err = parseSpansWithJSON(spans, req.Body, codec.ParseJsonV2)
		})

		if err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)
		} else {
			writer.WriteHeader(http.StatusAccepted)
		}
	})
}

func handleGzipRequestBody(handle http.Handler) http.HandlerFunc {
	return func(writer http.ResponseWriter, req *http.Request) {
		// decode body on the fly if it comes compressed
		if req.Header.Get("Content-Encoding") == "gzip" {
			body, err := gzip.NewReader(req.Body)
			if err != nil {
				http.Error(writer, err.Error(), http.StatusBadRequest)
				return
			}

			req.Body = body
			req.Header.Del("Content-Encoding")
			req.Header.Del("Content-Length")
		}

		handle.ServeHTTP(writer, req)
	}
}

func parseSpansWithThrift(spansChannel chan<- proxy.Span, body io.Reader) error {
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

func parseSpansWithJSON(spansChannel chan<- proxy.Span, body io.Reader, parser func(io.Reader) ([]proxy.Span, error)) error {
	parsedSpans, err := parser(body)
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
