package codec

import (
	"encoding/json"
	"github.com/flachnetz/dd-zipkin-proxy/proxy"
	"github.com/pkg/errors"
	"io"
	"strings"
	"time"
)

type jaegerTag struct {
	Key   string      `json:"key"`
	Value interface{} `json:"value"`
}

type jaegerSpan struct {
	TraceId Id `json:"traceID"`
	SpanId  Id `json:"spanID"`

	OperationName string `json:"operationName"`

	References []struct {
		RefType string `json:"refType"`
		SpanId  Id     `json:"spanID"`
	} `json:"references"`

	Timestamp uint64 `json:"startTime"`
	Duration  uint64 `json:"duration"`

	ProcessId string      `json:"processID"`
	Tags      []jaegerTag `json:"tags"`
}

type jaegerObject struct {
	Data []struct {
		Spans     []jaegerSpan             `json:"spans"`
		Processes map[string]jaegerProcess `json:"processes"`
	} `json:"data"`
}

type jaegerProcess struct {
	ServiceName string `json:"serviceName"`
}

func ParseJaeger(input io.Reader) ([]proxy.Span, error) {
	// decode spans into the span slice.
	var decoded jaegerObject
	if err := json.NewDecoder(input).Decode(&decoded); err != nil {
		return nil, errors.WithMessage(err, "parse spans for jaeger")
	}

	// now convert them to span objects
	var parsedSpans []proxy.Span

	for _, dataObject := range decoded.Data {
		for _, span := range dataObject.Spans {
			parsedSpans = append(parsedSpans, span.ToSpan(dataObject.Processes))
		}
	}

	return parsedSpans, nil
}

func (span *jaegerSpan) ToSpan(procs map[string]jaegerProcess) proxy.Span {
	// if no CHILD_OF reference exists then this is a root span.
	parentId := span.SpanId

	for _, ref := range span.References {
		if ref.RefType == "CHILD_OF" {
			parentId = ref.SpanId
		}
	}

	proxySpan := proxy.NewSpan(span.OperationName, span.TraceId, span.SpanId, parentId)

	proxySpan.Service = procs[span.ProcessId].ServiceName

	var spanKind string
	for _, tag := range span.Tags {
		value, ok := tag.Value.(string)
		if !ok {
			continue
		}

		key := tag.Key
		if key == "component" {
			key = "lc"
		}

		if key == "internal.span.format" {
			continue
		}

		if key == "span.kind" {
			spanKind = value
			continue
		}

		proxySpan.AddTag(key, value)
	}

	proxySpan.AddTag(tagProtocolVersion, tagJaeger)

	proxySpan.Timestamp = proxy.Microseconds(int64(span.Timestamp))
	proxySpan.Duration = time.Duration(span.Duration) * time.Microsecond

	if proxySpan.Duration == 0 {
		proxySpan.Duration = 1 * time.Millisecond
	}

	if strings.EqualFold(spanKind, "client") {
		proxySpan.Timings.CS = proxySpan.Timestamp
		proxySpan.Timings.CR = proxySpan.Timestamp.Add(proxySpan.Duration)
	}

	if strings.EqualFold(spanKind, "server") {
		proxySpan.Timings.SR = proxySpan.Timestamp
		proxySpan.Timings.SS = proxySpan.Timestamp.Add(proxySpan.Duration)
	}

	return proxySpan
}
