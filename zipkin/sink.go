package zipkin

import (
	"github.com/flachnetz/dd-zipkin-proxy/proxy"
	"github.com/openzipkin-contrib/zipkin-go-opentracing"
	"github.com/openzipkin-contrib/zipkin-go-opentracing/types"
)

func Sink(tracesCh <-chan proxy.Trace) {
	collector, _ := zipkintracer.NewHTTPCollector("http://localhost:9412/api/v1/spans")
	recorder := zipkintracer.NewRecorder(collector, false, "test:1234", "zipkin-proxy")

	for trace := range tracesCh {
		for _, span := range trace {
			raw := zipkintracer.RawSpan{
				Context: zipkintracer.SpanContext{
					TraceID: types.TraceID{
						High: 0,
						Low:  span.Trace.Uint64(),
					},
					SpanID:       span.Id.Uint64(),
					Sampled:      true,
					ParentSpanID: span.Parent.Uint64OrNil(),
					Owner:        true,
				},
				Operation: span.Name,
				Start:     span.Timestamp.ToTime(),
				Duration:  span.Duration,
				Tags:      nil,
				Logs:      nil,
			}

			recorder.RecordSpan(raw)
		}
	}
}
