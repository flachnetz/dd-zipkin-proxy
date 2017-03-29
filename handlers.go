package zipkinproxy

import (
	"compress/gzip"
	"encoding/json"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/flachnetz/dd-zipkin-proxy/jsoncodec"
	"github.com/julienschmidt/httprouter"
	"github.com/openzipkin/zipkin-go-opentracing/_thrift/gen-go/zipkincore"
	"github.com/pkg/errors"
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
			err = parseSpansWithJSON(spans, bodyReader)
		} else {
			err = parseSpansWithThrift(spans, bodyReader)
		}

		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
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

	return nil
}

func parseSpansWithThrift(spansChannel chan<- *zipkincore.Span, body io.Reader) error {
	protocol := thrift.NewTBinaryProtocolTransport(thrift.NewStreamTransportR(body))

	_, size, err := protocol.ReadListBegin()
	if err != nil {
		return errors.WithMessage(err, "Expect begin of list")
	}

	// allocate all spans at once in one big block of memory.
	spans := make([]zipkincore.Span, size)

	for idx := 0; idx < size; idx++ {
		if err := spans[idx].Read(protocol); err != nil {
			return errors.WithMessage(err, "Could not read thrift encoded span")
		}

		spansChannel <- &spans[idx]
	}

	return errors.WithMessage(protocol.ReadListEnd(), "Could not read end of list")
}
