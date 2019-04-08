package datadog

import (
	"github.com/openzipkin-contrib/zipkin-go-opentracing/thrift/gen-go/zipkincore"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
	"time"
)

func Initialize(addr string) {
	// sets the global instance
	tracer.Start(tracer.WithAgentAddr(addr))
}

func sinkSpan(span *zipkincore.Span) {
	startTime := time.Unix(0, span.GetTimestamp()*int64(time.Microsecond))
	finishTime := startTime.Add(time.Duration(span.GetDuration()) * time.Microsecond)

	tags := map[string]interface{}{}
	for _, an := range span.BinaryAnnotations {
		value := string(an.Value)

		switch an.Key {
		case "dd.name":
			tags[ext.SpanName] = value

		case "dd.service":
			tags[ext.ServiceName] = value

		case "dd.resource":
			tags[ext.ResourceName] = value

		default:
			tags[an.Key] = string(an.Value)
		}
	}

	ddSpan := tracer.StartSpan(span.Name, func(cfg *ddtrace.StartSpanConfig) {
		cfg.StartTime = startTime
		cfg.SpanID = uint64(span.ID)
		cfg.Tags = tags

		if span.ParentID != nil {
			cfg.Parent = spanContext{
				startTime: startTime,
				traceId:   uint64(span.TraceID),
				spanId:    uint64(*span.ParentID),
			}
		}
	})

	ddSpan.Finish(tracer.FinishTime(finishTime))
}

type spanContext struct {
	startTime time.Time
	traceId   uint64
	spanId    uint64
}

func (c spanContext) SpanID() uint64 {
	return c.spanId
}

func (c spanContext) TraceID() uint64 {
	return c.traceId
}

func (c spanContext) ForeachBaggageItem(handler func(k, v string) bool) {
	return
}

// Reads all zipkin spans from the given channel, converts them to datadog spans using the given converter
// and write them into another channel.
func Sink(zipkinSpans <-chan *zipkincore.Span) {
	for span := range zipkinSpans {
		sinkSpan(span)
	}
}
